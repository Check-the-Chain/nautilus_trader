// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Gap-free bootstrap and live control for selected Uniswap v4 quote-state mirrors.

use std::{
    collections::{BTreeMap, BTreeSet},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::{Address, B256, keccak256};
use nautilus_model::defi::{SharedDex, rpc::RpcLog};
use thiserror::Error;

use super::{UniswapV4EventPosition, UniswapV4Mirror, UniswapV4MirrorConfig, UniswapV4MirrorEvent};
use crate::{
    contracts::uniswap_v4_state_view::{UniswapV4StateViewContract, UniswapV4StateViewError},
    exchanges::parsing::uniswap_v4::{
        modify_liquidity::{MODIFY_LIQUIDITY_EVENT_SIGNATURE, parse_modify_liquidity_event_rpc},
        protocol_fee_updated::{
            PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE, parse_protocol_fee_updated_event_rpc,
        },
        swap::{SWAP_EVENT_SIGNATURE, parse_swap_event_rpc},
    },
    rpc::{helpers as rpc_helpers, http::BlockchainHttpRpcClient},
};

/// Availability of a controller or one selected pool mirror.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniswapV4MirrorStatus {
    Unavailable,
    Bootstrapping,
    Live,
}

/// Deterministic address and topic alternatives for one unified WSS subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV4MirrorLogFilter {
    pub address: Address,
    pub topic0: Vec<B256>,
    pub topic1: Vec<B256>,
}

impl UniswapV4MirrorLogFilter {
    /// Returns the two topic-position OR lists accepted by the typed HTTP log API.
    #[must_use]
    pub fn topic_alternatives(&self) -> Vec<Vec<B256>> {
        vec![self.topic0.clone(), self.topic1.clone()]
    }
}

/// Result of routing one live log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniswapV4MirrorLogOutcome {
    Applied,
    IgnoredUnknownPool,
    IgnoredOverlap,
}

/// A block identity observed by the WebSocket node after the PoolManager subscription is active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UniswapV4BootstrapHead {
    pub number: u64,
    pub hash: String,
}

/// Result of accepting one WSS head into the mirror feed's liveness guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniswapV4HeadOutcome {
    First,
    Advanced,
    Duplicate,
}

/// Fail-closed WSS head continuity and liveness errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum UniswapV4HeadGuardError {
    #[error("WSS head timeout must be greater than zero")]
    InvalidTimeout,
    #[error("WSS head guard has not begun a confirmation cycle")]
    NotStarted,
    #[error("WSS head stream requires a fresh confirmation and bootstrap cycle")]
    RecoveryRequired,
    #[error("no advancing WSS head was observed within {timeout:?}")]
    TimedOut { timeout: Duration },
    #[error("WSS head regressed from block {previous} to {actual}")]
    Regression { previous: u64, actual: u64 },
    #[error("WSS head gap: expected block {expected}, received {actual}")]
    Gap { expected: u64, actual: u64 },
    #[error("WSS head {block} changed hash from {previous_hash} to {actual_hash}")]
    SameHeightReorg {
        block: u64,
        previous_hash: B256,
        actual_hash: B256,
    },
    #[error("WSS head {block} parent {actual_parent} does not match prior hash {expected_parent}")]
    ParentHashMismatch {
        block: u64,
        expected_parent: B256,
        actual_parent: B256,
    },
}

/// Monotonic liveness and continuity guard for the WSS head stream that drives a mirror.
#[derive(Debug, Clone)]
pub struct UniswapV4HeadGuard {
    timeout: Duration,
    last_head: Option<(u64, B256)>,
    last_progress_at: Option<Instant>,
    recovery_required: bool,
}

impl UniswapV4HeadGuard {
    /// Creates an inactive guard with a non-zero maximum interval between advancing heads.
    ///
    /// # Errors
    ///
    /// Returns [`UniswapV4HeadGuardError::InvalidTimeout`] for a zero timeout.
    pub fn new(timeout: Duration) -> Result<Self, UniswapV4HeadGuardError> {
        if timeout.is_zero() {
            return Err(UniswapV4HeadGuardError::InvalidTimeout);
        }
        Ok(Self {
            timeout,
            last_head: None,
            last_progress_at: None,
            recovery_required: false,
        })
    }

    /// Begins a new cycle after all required WSS subscriptions are freshly confirmed.
    ///
    /// This is the only operation that clears a prior failure. Mirrors must remain unavailable
    /// until the first accepted head anchors a complete bootstrap and catch-up.
    pub fn begin_confirmation_cycle(&mut self, now: Instant) {
        self.last_head = None;
        self.last_progress_at = Some(now);
        self.recovery_required = false;
    }

    /// Forces recovery, for example when the transport disconnects or reconnects.
    pub fn require_recovery(&mut self) {
        self.recovery_required = true;
    }

    #[must_use]
    pub const fn recovery_required(&self) -> bool {
        self.recovery_required
    }

    #[must_use]
    pub const fn last_head(&self) -> Option<(u64, B256)> {
        self.last_head
    }

    /// Returns the current monotonic liveness deadline while recovery is not required.
    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        if self.recovery_required {
            return None;
        }
        self.last_progress_at?.checked_add(self.timeout)
    }

    /// Fails the guard when no advancing head has arrived within the configured interval.
    ///
    /// # Errors
    ///
    /// Returns an error before a confirmation cycle starts, after recovery becomes required, or
    /// when the configured interval has elapsed without an advancing head.
    pub fn check_liveness(&mut self, now: Instant) -> Result<(), UniswapV4HeadGuardError> {
        if self.recovery_required {
            return Err(UniswapV4HeadGuardError::RecoveryRequired);
        }
        let Some(last_progress_at) = self.last_progress_at else {
            return Err(UniswapV4HeadGuardError::NotStarted);
        };
        if now.saturating_duration_since(last_progress_at) >= self.timeout {
            self.recovery_required = true;
            return Err(UniswapV4HeadGuardError::TimedOut {
                timeout: self.timeout,
            });
        }
        Ok(())
    }

    /// Validates and records one WSS head.
    ///
    /// Exact duplicates are tolerated but do not refresh liveness. Every other non-contiguous or
    /// conflicting head requires a fresh confirmation and bootstrap cycle.
    ///
    /// # Errors
    ///
    /// Returns a liveness error or a continuity error for a regression, gap, same-height reorg, or
    /// parent-hash mismatch. Every such error except `NotStarted` requires explicit recovery.
    pub fn observe_head(
        &mut self,
        number: u64,
        hash: B256,
        parent_hash: B256,
        now: Instant,
    ) -> Result<UniswapV4HeadOutcome, UniswapV4HeadGuardError> {
        self.check_liveness(now)?;

        let Some((previous_number, previous_hash)) = self.last_head else {
            self.last_head = Some((number, hash));
            self.last_progress_at = Some(now);
            return Ok(UniswapV4HeadOutcome::First);
        };

        if number < previous_number {
            return self.fail(UniswapV4HeadGuardError::Regression {
                previous: previous_number,
                actual: number,
            });
        }
        if number == previous_number {
            if hash == previous_hash {
                return Ok(UniswapV4HeadOutcome::Duplicate);
            }
            return self.fail(UniswapV4HeadGuardError::SameHeightReorg {
                block: number,
                previous_hash,
                actual_hash: hash,
            });
        }

        let expected = previous_number.saturating_add(1);
        if number != expected {
            return self.fail(UniswapV4HeadGuardError::Gap {
                expected,
                actual: number,
            });
        }
        if parent_hash != previous_hash {
            return self.fail(UniswapV4HeadGuardError::ParentHashMismatch {
                block: number,
                expected_parent: previous_hash,
                actual_parent: parent_hash,
            });
        }

        self.last_head = Some((number, hash));
        self.last_progress_at = Some(now);
        Ok(UniswapV4HeadOutcome::Advanced)
    }

    fn fail<T>(&mut self, error: UniswapV4HeadGuardError) -> Result<T, UniswapV4HeadGuardError> {
        self.recovery_required = true;
        Err(error)
    }
}

/// Fail-closed controller errors.
#[derive(Debug, Error)]
pub enum UniswapV4MirrorControllerError {
    #[error("at least one Uniswap v4 pool must be registered")]
    NoPools,
    #[error("pool registration is closed after bootstrap starts")]
    RegistrationClosed,
    #[error("pool {pool_id} is already registered")]
    DuplicatePool { pool_id: B256 },
    #[error("bootstrap is already in progress")]
    BootstrapInProgress,
    #[error("failed to query {operation}: {reason}")]
    Rpc {
        operation: &'static str,
        reason: String,
    },
    #[error("current head {head} precedes finalized bootstrap block {finalized}")]
    HeadBeforeFinalized { finalized: u64, head: u64 },
    #[error("HTTP block hash {actual} does not match WSS block hash {expected} at block {block}")]
    HeadHashMismatch {
        block: u64,
        expected: String,
        actual: String,
    },
    #[error("StateView is bound to PoolManager {actual}, expected {expected}")]
    StateViewPoolManagerMismatch { expected: Address, actual: Address },
    #[error("failed to fetch StateView state for pool {pool_id}: {source}")]
    StateView {
        pool_id: B256,
        #[source]
        source: UniswapV4StateViewError,
    },
    #[error("failed to bootstrap mirror for pool {pool_id}: {reason}")]
    MirrorBootstrap { pool_id: B256, reason: String },
    #[error("malformed filtered log: {reason}")]
    MalformedLog { reason: String },
    #[error("pool {pool_id} requires recovery: {reason}")]
    RecoveryRequired { pool_id: B256, reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AppliedLogIdentity {
    position: UniswapV4EventPosition,
    block_hash: Option<String>,
    transaction_hash: Option<String>,
    address: String,
    topics: Vec<String>,
    data: String,
}

impl AppliedLogIdentity {
    fn new(log: &RpcLog, position: UniswapV4EventPosition) -> Self {
        Self {
            position,
            block_hash: log.block_hash.clone(),
            transaction_hash: log.transaction_hash.clone(),
            address: log.address.clone(),
            topics: log.topics.clone(),
            data: log.data.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct MirrorGeneration {
    mirrors: BTreeMap<B256, UniswapV4Mirror>,
    snapshot_blocks: BTreeMap<B256, u64>,
    catchup_logs: BTreeMap<B256, BTreeSet<AppliedLogIdentity>>,
    last_logs: BTreeMap<B256, AppliedLogIdentity>,
}

/// Standalone bootstrap and live controller for static-fee, zero-hook Uniswap v4 mirrors.
#[derive(Debug)]
pub struct UniswapV4MirrorController {
    dex: SharedDex,
    pool_manager: Address,
    state_view_address: Address,
    http_client: Arc<BlockchainHttpRpcClient>,
    state_view: UniswapV4StateViewContract,
    configs: BTreeMap<B256, UniswapV4MirrorConfig>,
    pool_statuses: BTreeMap<B256, UniswapV4MirrorStatus>,
    generation: Option<MirrorGeneration>,
    status: UniswapV4MirrorStatus,
    catchup_head: Option<u64>,
    bootstrap_started: bool,
}

impl UniswapV4MirrorController {
    /// Creates an empty controller. Pools must be registered before bootstrap begins.
    #[must_use]
    pub fn new(
        dex: SharedDex,
        pool_manager: Address,
        state_view_address: Address,
        http_client: Arc<BlockchainHttpRpcClient>,
        multicall_calls_per_rpc_request: u32,
    ) -> Self {
        let state_view = UniswapV4StateViewContract::new(
            Arc::clone(&http_client),
            multicall_calls_per_rpc_request,
        );
        Self {
            dex,
            pool_manager,
            state_view_address,
            http_client,
            state_view,
            configs: BTreeMap::new(),
            pool_statuses: BTreeMap::new(),
            generation: None,
            status: UniswapV4MirrorStatus::Unavailable,
            catchup_head: None,
            bootstrap_started: false,
        }
    }

    /// Registers one validated static-fee, zero-hook pool configuration.
    ///
    /// # Errors
    ///
    /// Returns an error after bootstrap has started or when the Pool ID is already selected.
    pub fn register_pool(
        &mut self,
        config: UniswapV4MirrorConfig,
    ) -> Result<(), UniswapV4MirrorControllerError> {
        if self.bootstrap_started {
            return Err(UniswapV4MirrorControllerError::RegistrationClosed);
        }
        let pool_id = config.pool_id();
        if self.configs.contains_key(&pool_id) {
            return Err(UniswapV4MirrorControllerError::DuplicatePool { pool_id });
        }
        self.configs.insert(pool_id, config);
        self.pool_statuses
            .insert(pool_id, UniswapV4MirrorStatus::Unavailable);
        Ok(())
    }

    #[must_use]
    pub const fn status(&self) -> UniswapV4MirrorStatus {
        self.status
    }

    #[must_use]
    pub const fn catchup_head(&self) -> Option<u64> {
        self.catchup_head
    }

    #[must_use]
    pub fn pool_status(&self, pool_id: B256) -> Option<UniswapV4MirrorStatus> {
        self.pool_statuses.get(&pool_id).copied()
    }

    /// Returns a mirror only while that individual pool is live.
    #[must_use]
    pub fn mirror(&self, pool_id: B256) -> Option<&UniswapV4Mirror> {
        if self.pool_status(pool_id) != Some(UniswapV4MirrorStatus::Live) {
            return None;
        }
        self.generation.as_ref()?.mirrors.get(&pool_id)
    }

    /// Revokes all mirror availability until a complete bootstrap succeeds.
    pub fn mark_all_unavailable(&mut self) {
        self.status = UniswapV4MirrorStatus::Unavailable;
        self.pool_statuses
            .values_mut()
            .for_each(|status| *status = UniswapV4MirrorStatus::Unavailable);
    }

    /// Returns selected Pool IDs in bytewise ascending order.
    #[must_use]
    pub fn selected_pool_ids(&self) -> Vec<B256> {
        self.configs.keys().copied().collect()
    }

    /// Returns the quote-relevant event hashes in stable semantic order: Swap,
    /// ModifyLiquidity and ProtocolFeeUpdated.
    #[must_use]
    pub fn operational_signature_topic_hashes() -> [B256; 3] {
        [
            keccak256(SWAP_EVENT_SIGNATURE),
            keccak256(MODIFY_LIQUIDITY_EVENT_SIGNATURE),
            keccak256(PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE),
        ]
    }

    /// Returns the exact address and topic alternatives for the unified live subscription.
    ///
    /// # Errors
    ///
    /// Returns an error when no pools have been registered.
    pub fn unified_subscription_filter(
        &self,
    ) -> Result<UniswapV4MirrorLogFilter, UniswapV4MirrorControllerError> {
        let topic1 = self.selected_pool_ids();
        if topic1.is_empty() {
            return Err(UniswapV4MirrorControllerError::NoPools);
        }
        Ok(UniswapV4MirrorLogFilter {
            address: self.pool_manager,
            topic0: Self::operational_signature_topic_hashes().to_vec(),
            topic1,
        })
    }

    /// Bootstraps a replacement generation after the caller confirms the WSS subscription.
    ///
    /// Reads every pool at finalized block `B`, captures head `H`, performs one selected-pool HTTP
    /// log query over `B + 1..=H`, sorts and applies those logs, then publishes all mirrors in one
    /// assignment. The prior generation remains untouched on every failure.
    ///
    /// # Errors
    ///
    /// Returns an error if no pools are selected, bootstrap is already running, any StateView or
    /// RPC operation fails, or any selected catch-up log cannot be validated and applied.
    pub async fn bootstrap_after_subscription_confirmation(
        &mut self,
        wss_head: &UniswapV4BootstrapHead,
    ) -> Result<(), UniswapV4MirrorControllerError> {
        if self.configs.is_empty() {
            return Err(UniswapV4MirrorControllerError::NoPools);
        }
        if self.status == UniswapV4MirrorStatus::Bootstrapping {
            return Err(UniswapV4MirrorControllerError::BootstrapInProgress);
        }

        self.bootstrap_started = true;
        self.status = UniswapV4MirrorStatus::Bootstrapping;
        self.pool_statuses
            .values_mut()
            .for_each(|status| *status = UniswapV4MirrorStatus::Bootstrapping);

        let result = self.build_replacement_generation(wss_head).await;
        self.finish_bootstrap(result)
    }

    /// Routes and atomically applies one WSS log to its selected live mirror.
    ///
    /// Unknown Pool IDs are ignored. A selected removed log, malformed event, unknown operational
    /// signature, emitter mismatch or state/order violation makes that pool unavailable.
    ///
    /// # Errors
    ///
    /// Returns a recovery-required error for every selected-pool validation, parse or state error,
    /// or a malformed-log error when topic1 cannot identify a Pool ID.
    pub fn apply_live_log(
        &mut self,
        log: &RpcLog,
    ) -> Result<UniswapV4MirrorLogOutcome, UniswapV4MirrorControllerError> {
        let pool_id = match parse_pool_id(log) {
            Ok(pool_id) => pool_id,
            Err(reason) => {
                self.mark_all_unavailable();
                return Err(UniswapV4MirrorControllerError::MalformedLog { reason });
            }
        };
        if !self.configs.contains_key(&pool_id) {
            return Ok(UniswapV4MirrorLogOutcome::IgnoredUnknownPool);
        }
        if self.pool_status(pool_id) != Some(UniswapV4MirrorStatus::Live) {
            return Err(UniswapV4MirrorControllerError::RecoveryRequired {
                pool_id,
                reason: "mirror is not live".to_string(),
            });
        }

        let result = self
            .generation
            .as_mut()
            .ok_or_else(|| UniswapV4MirrorControllerError::RecoveryRequired {
                pool_id,
                reason: "published mirror generation is missing".to_string(),
            })
            .and_then(|generation| {
                apply_selected_log(
                    &self.dex,
                    self.pool_manager,
                    generation,
                    pool_id,
                    log,
                    false,
                )
                .map_err(|reason| {
                    UniswapV4MirrorControllerError::RecoveryRequired { pool_id, reason }
                })
            });
        if result.is_err() {
            self.pool_statuses
                .insert(pool_id, UniswapV4MirrorStatus::Unavailable);
            self.status = UniswapV4MirrorStatus::Unavailable;
        }
        result
    }

    async fn build_replacement_generation(
        &self,
        wss_head: &UniswapV4BootstrapHead,
    ) -> Result<(MirrorGeneration, u64), UniswapV4MirrorControllerError> {
        let finalized = self.http_client.finalized_block().await.map_err(|error| {
            UniswapV4MirrorControllerError::Rpc {
                operation: "finalized block",
                reason: error.to_string(),
            }
        })?;
        let state_view_pool_manager = self
            .state_view
            .pool_manager(&self.state_view_address, finalized)
            .await
            .map_err(|error| UniswapV4MirrorControllerError::Rpc {
                operation: "StateView PoolManager binding",
                reason: error.to_string(),
            })?;
        if state_view_pool_manager != self.pool_manager {
            return Err(
                UniswapV4MirrorControllerError::StateViewPoolManagerMismatch {
                    expected: self.pool_manager,
                    actual: state_view_pool_manager,
                },
            );
        }
        if wss_head.number < finalized {
            return Err(UniswapV4MirrorControllerError::HeadBeforeFinalized {
                finalized,
                head: wss_head.number,
            });
        }
        self.verify_http_head(wss_head).await?;

        let mut generation = MirrorGeneration::default();
        for (&pool_id, &config) in &self.configs {
            let snapshot = self
                .state_view
                .fetch_pool_state(
                    &self.state_view_address,
                    pool_id,
                    config.tick_spacing(),
                    finalized,
                )
                .await
                .map_err(|source| UniswapV4MirrorControllerError::StateView { pool_id, source })?;
            let mirror = UniswapV4Mirror::bootstrap(config, &snapshot).map_err(|error| {
                UniswapV4MirrorControllerError::MirrorBootstrap {
                    pool_id,
                    reason: error.to_string(),
                }
            })?;
            generation.mirrors.insert(pool_id, mirror);
            generation.snapshot_blocks.insert(pool_id, finalized);
        }

        let head = wss_head.number;
        if head > finalized {
            let filter = self.unified_subscription_filter()?;
            let logs = self
                .http_client
                .get_logs_with_topic_alternatives(
                    Some(&filter.address),
                    &filter.topic_alternatives(),
                    finalized + 1,
                    head,
                )
                .await
                .map_err(|error| UniswapV4MirrorControllerError::Rpc {
                    operation: "selected-pool catch-up logs",
                    reason: error.to_string(),
                })?;
            apply_catchup_logs(&self.dex, self.pool_manager, &mut generation, logs)?;
        }
        self.verify_http_head(wss_head).await?;
        Ok((generation, head))
    }

    async fn verify_http_head(
        &self,
        wss_head: &UniswapV4BootstrapHead,
    ) -> Result<(), UniswapV4MirrorControllerError> {
        let actual = self
            .http_client
            .block_hash(wss_head.number)
            .await
            .map_err(|error| UniswapV4MirrorControllerError::Rpc {
                operation: "HTTP/WSS head comparison",
                reason: error.to_string(),
            })?;
        if !actual.eq_ignore_ascii_case(&wss_head.hash) {
            return Err(UniswapV4MirrorControllerError::HeadHashMismatch {
                block: wss_head.number,
                expected: wss_head.hash.clone(),
                actual,
            });
        }
        Ok(())
    }

    fn finish_bootstrap(
        &mut self,
        result: Result<(MirrorGeneration, u64), UniswapV4MirrorControllerError>,
    ) -> Result<(), UniswapV4MirrorControllerError> {
        match result {
            Ok((generation, head)) => {
                self.generation = Some(generation);
                self.catchup_head = Some(head);
                self.status = UniswapV4MirrorStatus::Live;
                self.pool_statuses
                    .values_mut()
                    .for_each(|status| *status = UniswapV4MirrorStatus::Live);
                Ok(())
            }
            Err(error) => {
                self.status = UniswapV4MirrorStatus::Unavailable;
                self.pool_statuses
                    .values_mut()
                    .for_each(|status| *status = UniswapV4MirrorStatus::Unavailable);
                Err(error)
            }
        }
    }
}

fn apply_catchup_logs(
    dex: &SharedDex,
    pool_manager: Address,
    generation: &mut MirrorGeneration,
    logs: Vec<RpcLog>,
) -> Result<(), UniswapV4MirrorControllerError> {
    let mut selected_logs = Vec::new();
    for log in logs {
        let pool_id = parse_pool_id(&log)
            .map_err(|reason| UniswapV4MirrorControllerError::MalformedLog { reason })?;
        if !generation.mirrors.contains_key(&pool_id) {
            continue;
        }
        let position = parse_position(&log).map_err(|reason| {
            UniswapV4MirrorControllerError::RecoveryRequired { pool_id, reason }
        })?;
        selected_logs.push((position, pool_id, log));
    }
    selected_logs.sort_by_key(|(position, _, _)| *position);

    for (_, pool_id, log) in selected_logs {
        apply_selected_log(dex, pool_manager, generation, pool_id, &log, true).map_err(
            |reason| UniswapV4MirrorControllerError::RecoveryRequired { pool_id, reason },
        )?;
    }
    Ok(())
}

fn apply_selected_log(
    dex: &SharedDex,
    pool_manager: Address,
    generation: &mut MirrorGeneration,
    pool_id: B256,
    log: &RpcLog,
    record_catchup: bool,
) -> Result<UniswapV4MirrorLogOutcome, String> {
    if log.removed {
        return Err("received removed=true".to_string());
    }
    let emitter = Address::from_str(&log.address)
        .map_err(|error| format!("invalid emitter address '{}': {error}", log.address))?;
    if emitter != pool_manager {
        return Err(format!(
            "emitter {emitter} does not match PoolManager {pool_manager}"
        ));
    }
    let signature = parse_topic(log, 0, "event signature")?;
    let signatures = UniswapV4MirrorController::operational_signature_topic_hashes();
    if !signatures.contains(&signature) {
        return Err(format!("unknown filtered event signature {signature}"));
    }
    let position = parse_position(log)?;
    let identity = AppliedLogIdentity::new(log, position);
    let mirror = generation
        .mirrors
        .get(&pool_id)
        .ok_or_else(|| "selected mirror is missing".to_string())?;

    let overlap_outcome = if position <= mirror.watermark() {
        let snapshot_block = generation
            .snapshot_blocks
            .get(&pool_id)
            .copied()
            .ok_or_else(|| "snapshot watermark is missing".to_string())?;
        let is_snapshot_overlap = position.block_number <= snapshot_block;
        let is_catchup_overlap = generation
            .catchup_logs
            .get(&pool_id)
            .is_some_and(|logs| logs.contains(&identity));
        let is_last_duplicate = generation.last_logs.get(&pool_id) == Some(&identity);
        if is_snapshot_overlap || is_catchup_overlap || is_last_duplicate {
            Some(UniswapV4MirrorLogOutcome::IgnoredOverlap)
        } else {
            return Err(format!(
                "log position {position:?} is not after watermark {:?} and is not an exact overlap",
                mirror.watermark()
            ));
        }
    } else {
        None
    };

    if signature == signatures[0] {
        let event = parse_swap_event_rpc(dex.clone(), log).map_err(|error| error.to_string())?;
        if overlap_outcome.is_none() {
            generation
                .mirrors
                .get_mut(&pool_id)
                .ok_or_else(|| "selected mirror is missing".to_string())?
                .apply(UniswapV4MirrorEvent::Swap(&event))
                .map_err(|error| error.to_string())?;
        }
    } else if signature == signatures[1] {
        let event = parse_modify_liquidity_event_rpc(dex.clone(), log)
            .map_err(|error| error.to_string())?;
        if overlap_outcome.is_none() {
            generation
                .mirrors
                .get_mut(&pool_id)
                .ok_or_else(|| "selected mirror is missing".to_string())?
                .apply(UniswapV4MirrorEvent::ModifyLiquidity(&event))
                .map_err(|error| error.to_string())?;
        }
    } else if signature == signatures[2] {
        let event = parse_protocol_fee_updated_event_rpc(dex.clone(), log)
            .map_err(|error| error.to_string())?;
        if overlap_outcome.is_none() {
            generation
                .mirrors
                .get_mut(&pool_id)
                .ok_or_else(|| "selected mirror is missing".to_string())?
                .apply(UniswapV4MirrorEvent::ProtocolFeeUpdated(&event))
                .map_err(|error| error.to_string())?;
        }
    }

    if let Some(outcome) = overlap_outcome {
        return Ok(outcome);
    }

    if record_catchup {
        generation
            .catchup_logs
            .entry(pool_id)
            .or_default()
            .insert(identity.clone());
    }
    generation.last_logs.insert(pool_id, identity);
    Ok(UniswapV4MirrorLogOutcome::Applied)
}

fn parse_pool_id(log: &RpcLog) -> Result<B256, String> {
    parse_topic(log, 1, "Pool ID")
}

fn parse_topic(log: &RpcLog, index: usize, field: &str) -> Result<B256, String> {
    let topic = log
        .topics
        .get(index)
        .ok_or_else(|| format!("missing {field} at topic{index}"))?;
    B256::from_str(topic).map_err(|error| format!("invalid {field} '{topic}': {error}"))
}

fn parse_position(log: &RpcLog) -> Result<UniswapV4EventPosition, String> {
    let block_number = rpc_helpers::extract_block_number(log).map_err(|error| error.to_string())?;
    let transaction_index =
        rpc_helpers::extract_transaction_index(log).map_err(|error| error.to_string())?;
    let log_index = rpc_helpers::extract_log_index(log).map_err(|error| error.to_string())?;
    Ok(UniswapV4EventPosition::new(
        block_number,
        transaction_index,
        log_index,
    ))
}

#[cfg(test)]
mod tests {
    use alloy::{
        primitives::{Address, B256, U160, address, aliases::U24},
        sol_types::SolValue,
    };
    use nautilus_core::hex;

    use super::*;
    use crate::{
        contracts::uniswap_v4_state_view::{
            UniswapV4PoolState, UniswapV4Slot0State, UniswapV4TickLiquidityState,
        },
        exchanges::robinhood,
        rpc::{
            BlockchainRpcClient,
            chains::robinhood::RobinhoodRpcClient,
            types::{BlockchainMessage, RpcEventType},
        },
    };

    const BOOTSTRAP_BLOCK: u64 = 10;
    const STATIC_LP_FEE: u32 = 3_000;

    fn config(pool_id: B256) -> UniswapV4MirrorConfig {
        UniswapV4MirrorConfig::new_unchecked_for_test(pool_id, 60, STATIC_LP_FEE)
    }

    fn snapshot(pool_id: B256, liquidity: u128) -> UniswapV4PoolState {
        UniswapV4PoolState {
            pool_id,
            tick_spacing: 60,
            block_number: BOOTSTRAP_BLOCK,
            slot0: UniswapV4Slot0State {
                sqrt_price_x96: U160::from(1_u8) << 96,
                tick: 0,
                protocol_fee: 0,
                lp_fee: STATIC_LP_FEE,
            },
            liquidity,
            ticks: vec![
                UniswapV4TickLiquidityState {
                    tick: -120,
                    liquidity_gross: liquidity,
                    liquidity_net: liquidity as i128,
                },
                UniswapV4TickLiquidityState {
                    tick: 120,
                    liquidity_gross: liquidity,
                    liquidity_net: -(liquidity as i128),
                },
            ],
        }
    }

    fn controller(pool_ids: &[B256]) -> UniswapV4MirrorController {
        let client = Arc::new(BlockchainHttpRpcClient::new(
            "http://unused.invalid".to_string(),
            None,
            None,
        ));
        let mut controller = UniswapV4MirrorController::new(
            robinhood::UNISWAP_V4.dex.clone(),
            address!("8366a39CC670B4001A1121B8F6A443A643e40951"),
            address!("F3334192D15450CdD385c8B70e03f9A6bD9E673b"),
            client,
            100,
        );
        for &pool_id in pool_ids {
            controller.register_pool(config(pool_id)).unwrap();
        }
        controller
    }

    fn generation(pool_ids: &[B256], liquidity: u128) -> MirrorGeneration {
        let mut generation = MirrorGeneration::default();
        for &pool_id in pool_ids {
            generation.mirrors.insert(
                pool_id,
                UniswapV4Mirror::bootstrap(config(pool_id), &snapshot(pool_id, liquidity)).unwrap(),
            );
            generation.snapshot_blocks.insert(pool_id, BOOTSTRAP_BLOCK);
        }
        generation
    }

    fn protocol_log(
        pool_manager: Address,
        pool_id: B256,
        block: u64,
        transaction: u32,
        log_index: u32,
        protocol_fee: u32,
    ) -> RpcLog {
        RpcLog {
            removed: false,
            log_index: Some(format!("0x{log_index:x}")),
            transaction_index: Some(format!("0x{transaction:x}")),
            transaction_hash: Some(format!("0x{block:064x}")),
            block_hash: Some(format!("0x{:064x}", block + 1_000)),
            block_number: Some(format!("0x{block:x}")),
            address: pool_manager.to_string(),
            data: hex::encode_prefixed(U24::from(protocol_fee).abi_encode()),
            topics: vec![
                keccak256(PROTOCOL_FEE_UPDATED_EVENT_SIGNATURE).to_string(),
                pool_id.to_string(),
            ],
        }
    }

    fn publish(
        controller: &mut UniswapV4MirrorController,
        generation: MirrorGeneration,
        head: u64,
    ) {
        controller.bootstrap_started = true;
        controller.status = UniswapV4MirrorStatus::Bootstrapping;
        controller.finish_bootstrap(Ok((generation, head))).unwrap();
    }

    #[test]
    fn head_guard_requires_nonzero_timeout_and_explicit_cycle() {
        assert_eq!(
            UniswapV4HeadGuard::new(Duration::ZERO).unwrap_err(),
            UniswapV4HeadGuardError::InvalidTimeout
        );

        let mut guard = UniswapV4HeadGuard::new(Duration::from_secs(5)).unwrap();
        assert_eq!(
            guard.check_liveness(Instant::now()).unwrap_err(),
            UniswapV4HeadGuardError::NotStarted
        );
    }

    #[test]
    fn head_guard_duplicates_do_not_extend_liveness_or_revive_timeout() {
        let start = Instant::now();
        let timeout = Duration::from_secs(5);
        let first_hash = B256::repeat_byte(1);
        let mut guard = UniswapV4HeadGuard::new(timeout).unwrap();
        guard.begin_confirmation_cycle(start);

        assert_eq!(
            guard
                .observe_head(10, first_hash, B256::ZERO, start + Duration::from_secs(1))
                .unwrap(),
            UniswapV4HeadOutcome::First
        );
        assert_eq!(
            guard
                .observe_head(10, first_hash, B256::ZERO, start + Duration::from_secs(4))
                .unwrap(),
            UniswapV4HeadOutcome::Duplicate
        );

        assert_eq!(
            guard
                .check_liveness(start + Duration::from_secs(6))
                .unwrap_err(),
            UniswapV4HeadGuardError::TimedOut { timeout }
        );
        assert_eq!(
            guard
                .observe_head(
                    11,
                    B256::repeat_byte(2),
                    first_hash,
                    start + Duration::from_secs(7)
                )
                .unwrap_err(),
            UniswapV4HeadGuardError::RecoveryRequired
        );

        guard.begin_confirmation_cycle(start + Duration::from_secs(8));
        assert!(!guard.recovery_required());
        assert_eq!(guard.last_head(), None);
    }

    #[test]
    fn advancing_head_refreshes_liveness() {
        let start = Instant::now();
        let first_hash = B256::repeat_byte(1);
        let second_hash = B256::repeat_byte(2);
        let mut guard = UniswapV4HeadGuard::new(Duration::from_secs(5)).unwrap();
        guard.begin_confirmation_cycle(start);
        guard
            .observe_head(10, first_hash, B256::ZERO, start + Duration::from_secs(1))
            .unwrap();

        assert_eq!(
            guard
                .observe_head(11, second_hash, first_hash, start + Duration::from_secs(5))
                .unwrap(),
            UniswapV4HeadOutcome::Advanced
        );
        guard
            .check_liveness(start + Duration::from_secs(9))
            .unwrap();
        assert_eq!(guard.last_head(), Some((11, second_hash)));
    }

    #[test]
    fn conflicting_or_noncontiguous_head_requires_recovery() {
        let start = Instant::now();
        let previous_hash = B256::repeat_byte(1);
        let mut base = UniswapV4HeadGuard::new(Duration::from_secs(5)).unwrap();
        base.begin_confirmation_cycle(start);
        base.observe_head(
            10,
            previous_hash,
            B256::ZERO,
            start + Duration::from_secs(1),
        )
        .unwrap();

        let mut regression = base.clone();
        assert_eq!(
            regression
                .observe_head(
                    9,
                    B256::repeat_byte(2),
                    B256::ZERO,
                    start + Duration::from_secs(2)
                )
                .unwrap_err(),
            UniswapV4HeadGuardError::Regression {
                previous: 10,
                actual: 9,
            }
        );
        assert!(regression.recovery_required());

        let mut gap = base.clone();
        assert_eq!(
            gap.observe_head(
                12,
                B256::repeat_byte(2),
                previous_hash,
                start + Duration::from_secs(2)
            )
            .unwrap_err(),
            UniswapV4HeadGuardError::Gap {
                expected: 11,
                actual: 12,
            }
        );

        let mut same_height_reorg = base.clone();
        assert!(matches!(
            same_height_reorg.observe_head(
                10,
                B256::repeat_byte(2),
                B256::ZERO,
                start + Duration::from_secs(2)
            ),
            Err(UniswapV4HeadGuardError::SameHeightReorg { block: 10, .. })
        ));

        let mut parent_mismatch = base;
        assert!(matches!(
            parent_mismatch.observe_head(
                11,
                B256::repeat_byte(2),
                B256::repeat_byte(3),
                start + Duration::from_secs(2)
            ),
            Err(UniswapV4HeadGuardError::ParentHashMismatch { block: 11, .. })
        ));
    }

    #[test]
    fn selected_ids_and_unified_filter_are_deterministic() {
        let low = B256::repeat_byte(1);
        let high = B256::repeat_byte(2);
        let controller = controller(&[high, low]);

        assert_eq!(controller.selected_pool_ids(), vec![low, high]);
        let filter = controller.unified_subscription_filter().unwrap();
        assert_eq!(filter.address, controller.pool_manager);
        assert_eq!(
            filter.topic0,
            UniswapV4MirrorController::operational_signature_topic_hashes()
        );
        assert_eq!(filter.topic1, vec![low, high]);
        assert_eq!(filter.topic_alternatives().len(), 2);
    }

    #[test]
    fn catchup_logs_are_sorted_and_routed_by_pool_id() {
        let first = B256::repeat_byte(1);
        let second = B256::repeat_byte(2);
        let controller = controller(&[first, second]);
        let mut generation = generation(&[first, second], 1_000);
        let logs = vec![
            protocol_log(controller.pool_manager, first, 12, 0, 0, 300),
            protocol_log(controller.pool_manager, second, 11, 1, 0, 200),
            protocol_log(controller.pool_manager, first, 11, 0, 1, 100),
        ];

        apply_catchup_logs(
            &controller.dex,
            controller.pool_manager,
            &mut generation,
            logs,
        )
        .unwrap();

        assert_eq!(generation.mirrors[&first].protocol_fee(), 300);
        assert_eq!(generation.mirrors[&second].protocol_fee(), 200);
        assert_eq!(
            generation.mirrors[&first].watermark(),
            UniswapV4EventPosition::new(12, 0, 0)
        );
        assert_eq!(
            generation.mirrors[&second].watermark(),
            UniswapV4EventPosition::new(11, 1, 0)
        );
    }

    #[test]
    fn exact_live_overlap_is_ignored_but_other_regression_fails_closed() {
        let pool_id = B256::repeat_byte(1);
        let mut controller = controller(&[pool_id]);
        let log = protocol_log(controller.pool_manager, pool_id, 11, 0, 0, 100);
        let mut generation = generation(&[pool_id], 1_000);
        apply_catchup_logs(
            &controller.dex,
            controller.pool_manager,
            &mut generation,
            vec![log.clone()],
        )
        .unwrap();
        publish(&mut controller, generation, 11);

        assert_eq!(
            controller.apply_live_log(&log).unwrap(),
            UniswapV4MirrorLogOutcome::IgnoredOverlap
        );

        let regressing = protocol_log(controller.pool_manager, pool_id, 11, 0, 0, 200);
        assert!(matches!(
            controller.apply_live_log(&regressing),
            Err(UniswapV4MirrorControllerError::RecoveryRequired { .. })
        ));
        assert_eq!(
            controller.pool_status(pool_id),
            Some(UniswapV4MirrorStatus::Unavailable)
        );
    }

    #[test]
    fn removed_and_parse_errors_make_only_the_affected_pool_unavailable() {
        let removed_pool = B256::repeat_byte(1);
        let parse_pool = B256::repeat_byte(2);
        let mut controller = controller(&[removed_pool, parse_pool]);
        publish(
            &mut controller,
            generation(&[removed_pool, parse_pool], 1_000),
            BOOTSTRAP_BLOCK,
        );

        let mut removed = protocol_log(
            controller.pool_manager,
            removed_pool,
            BOOTSTRAP_BLOCK + 1,
            0,
            0,
            100,
        );
        removed.removed = true;
        assert!(matches!(
            controller.apply_live_log(&removed),
            Err(UniswapV4MirrorControllerError::RecoveryRequired { .. })
        ));
        assert_eq!(
            controller.pool_status(removed_pool),
            Some(UniswapV4MirrorStatus::Unavailable)
        );
        assert_eq!(
            controller.pool_status(parse_pool),
            Some(UniswapV4MirrorStatus::Live)
        );

        let mut malformed = protocol_log(
            controller.pool_manager,
            parse_pool,
            BOOTSTRAP_BLOCK + 1,
            0,
            1,
            100,
        );
        malformed.data = "0x01".to_string();
        assert!(matches!(
            controller.apply_live_log(&malformed),
            Err(UniswapV4MirrorControllerError::RecoveryRequired { .. })
        ));
        assert_eq!(
            controller.pool_status(parse_pool),
            Some(UniswapV4MirrorStatus::Unavailable)
        );
    }

    #[test]
    fn malformed_pool_id_revokes_all_mirrors() {
        let first = B256::repeat_byte(1);
        let second = B256::repeat_byte(2);
        let mut controller = controller(&[first, second]);
        publish(
            &mut controller,
            generation(&[first, second], 1_000),
            BOOTSTRAP_BLOCK,
        );

        let mut malformed = protocol_log(
            controller.pool_manager,
            first,
            BOOTSTRAP_BLOCK + 1,
            0,
            0,
            100,
        );
        malformed.topics[1] = "0x01".to_string();

        assert!(matches!(
            controller.apply_live_log(&malformed),
            Err(UniswapV4MirrorControllerError::MalformedLog { .. })
        ));
        assert_eq!(controller.status(), UniswapV4MirrorStatus::Unavailable);
        assert!(controller.mirror(first).is_none());
        assert!(controller.mirror(second).is_none());
    }

    #[test]
    fn unknown_pools_are_ignored_but_unknown_selected_signatures_fail_closed() {
        let selected_pool = B256::repeat_byte(1);
        let unknown_pool = B256::repeat_byte(2);
        let mut controller = controller(&[selected_pool]);
        publish(
            &mut controller,
            generation(&[selected_pool], 1_000),
            BOOTSTRAP_BLOCK,
        );

        let mut log = protocol_log(
            controller.pool_manager,
            unknown_pool,
            BOOTSTRAP_BLOCK + 1,
            0,
            0,
            100,
        );
        log.topics[0] = B256::repeat_byte(0xff).to_string();
        assert_eq!(
            controller.apply_live_log(&log).unwrap(),
            UniswapV4MirrorLogOutcome::IgnoredUnknownPool
        );

        log.topics[1] = selected_pool.to_string();
        assert!(matches!(
            controller.apply_live_log(&log),
            Err(UniswapV4MirrorControllerError::RecoveryRequired { .. })
        ));
        assert_eq!(
            controller.pool_status(selected_pool),
            Some(UniswapV4MirrorStatus::Unavailable)
        );
    }

    #[test]
    fn failed_bootstrap_does_not_replace_the_published_generation() {
        let pool_id = B256::repeat_byte(1);
        let mut controller = controller(&[pool_id]);
        publish(
            &mut controller,
            generation(&[pool_id], 1_000),
            BOOTSTRAP_BLOCK,
        );
        let original = controller.generation.clone();

        let error =
            controller.finish_bootstrap(Err(UniswapV4MirrorControllerError::HeadBeforeFinalized {
                finalized: 20,
                head: 19,
            }));
        assert!(error.is_err());
        assert_eq!(
            controller.generation.as_ref().unwrap().mirrors,
            original.as_ref().unwrap().mirrors
        );

        controller
            .finish_bootstrap(Ok((generation(&[pool_id], 2_000), 21)))
            .unwrap();
        assert_eq!(controller.mirror(pool_id).unwrap().liquidity(), 2_000);
        assert_eq!(controller.catchup_head(), Some(21));
    }

    #[tokio::test]
    #[ignore = "requires live Robinhood Chain RPC access"]
    async fn live_robinhood_nvda_bootstrap_validates_with_nonempty_ticks() {
        let pool_manager = address!("8366a39CC670B4001A1121B8F6A443A643e40951");
        let state_view_address = address!("F3334192D15450CdD385c8B70e03f9A6bD9E673b");
        let pool_id =
            B256::from_str("0x3bb34a44f1b2b5f32c034c38a53065a521a47b199700fa9bd19d60985ff24bf1")
                .unwrap();
        let client = Arc::new(BlockchainHttpRpcClient::new(
            "https://rpc.mainnet.chain.robinhood.com".to_string(),
            None,
            None,
        ));
        let mut controller = UniswapV4MirrorController::new(
            robinhood::UNISWAP_V4.dex.clone(),
            pool_manager,
            state_view_address,
            Arc::clone(&client),
            100,
        );
        controller
            .register_pool(
                UniswapV4MirrorConfig::new(
                    pool_id,
                    address!("5fc5360D0400a0Fd4f2af552ADD042D716F1d168"),
                    address!("d0601CE157Db5BDc3162BbaC2a2C8aF5320D9EEC"),
                    60,
                    3_000,
                    Address::ZERO,
                )
                .unwrap(),
            )
            .unwrap();

        let head = client.current_block().await.unwrap();
        let bootstrap_head = UniswapV4BootstrapHead {
            number: head,
            hash: client.block_hash(head).await.unwrap(),
        };
        controller
            .bootstrap_after_subscription_confirmation(&bootstrap_head)
            .await
            .unwrap();
        let mirror = controller.mirror(pool_id).unwrap().clone();
        let validation_block = mirror.watermark().block_number;
        let snapshot = UniswapV4StateViewContract::new(client, 100)
            .fetch_pool_state(&state_view_address, pool_id, 60, validation_block)
            .await
            .unwrap();

        mirror.validate_against_snapshot(&snapshot).unwrap();
        assert!(!mirror.ticks().is_empty());
    }

    #[tokio::test]
    #[ignore = "requires ALCHEMY_ROBINHOOD_WSS_URL and live Robinhood Chain access"]
    async fn live_alchemy_wss_bootstraps_nvda_mirror_from_confirmed_subscription() {
        let wss_url = std::env::var("ALCHEMY_ROBINHOOD_WSS_URL")
            .expect("ALCHEMY_ROBINHOOD_WSS_URL must be set");
        let http_url = wss_url.replacen("wss://", "https://", 1);
        let pool_manager = address!("8366a39CC670B4001A1121B8F6A443A643e40951");
        let state_view_address = address!("F3334192D15450CdD385c8B70e03f9A6bD9E673b");
        let pool_id =
            B256::from_str("0x3bb34a44f1b2b5f32c034c38a53065a521a47b199700fa9bd19d60985ff24bf1")
                .unwrap();
        let http_client = Arc::new(BlockchainHttpRpcClient::new(http_url, None, None));
        let mut controller = UniswapV4MirrorController::new(
            robinhood::UNISWAP_V4.dex.clone(),
            pool_manager,
            state_view_address,
            http_client,
            100,
        );
        controller
            .register_pool(
                UniswapV4MirrorConfig::new(
                    pool_id,
                    address!("5fc5360D0400a0Fd4f2af552ADD042D716F1d168"),
                    address!("d0601CE157Db5BDc3162BbaC2a2C8aF5320D9EEC"),
                    60,
                    3_000,
                    Address::ZERO,
                )
                .unwrap(),
            )
            .unwrap();

        let mut rpc = RobinhoodRpcClient::new(wss_url, None);
        rpc.connect().await.unwrap();
        rpc.subscribe_blocks().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if matches!(
                    rpc.next_rpc_message().await.unwrap(),
                    BlockchainMessage::SubscriptionConfirmed(RpcEventType::NewBlock)
                ) {
                    break;
                }
            }
        })
        .await
        .expect("newHeads subscription confirmation");

        let filter = controller.unified_subscription_filter().unwrap();
        let signatures = filter
            .topic0
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let pool_ids = filter
            .topic1
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        rpc.subscribe_pool_manager_events(
            nautilus_model::defi::DexType::UniswapV4,
            filter.address,
            &signatures,
            &pool_ids,
        )
        .await
        .unwrap();

        let (head, buffered_logs) =
            tokio::time::timeout(std::time::Duration::from_secs(30), async {
                let mut confirmed = false;
                let mut buffered_logs = Vec::new();
                loop {
                    match rpc.next_rpc_message().await.unwrap() {
                        BlockchainMessage::SubscriptionConfirmed(RpcEventType::PoolManager(
                            nautilus_model::defi::DexType::UniswapV4,
                        )) => confirmed = true,
                        BlockchainMessage::PoolManagerEvent(
                            nautilus_model::defi::DexType::UniswapV4,
                            log,
                        ) => buffered_logs.push(log),
                        BlockchainMessage::Block(block) if confirmed => {
                            break (block, buffered_logs);
                        }
                        BlockchainMessage::Reconnected => {
                            panic!("unexpected reconnect during live test")
                        }
                        _ => {}
                    }
                }
            })
            .await
            .expect("PoolManager confirmation and post-confirmation head");

        controller
            .bootstrap_after_subscription_confirmation(&UniswapV4BootstrapHead {
                number: head.number,
                hash: head.hash,
            })
            .await
            .unwrap();
        for log in buffered_logs {
            controller.apply_live_log(&log).unwrap();
        }

        let mirror = controller
            .mirror(pool_id)
            .expect("NVDA mirror should be live");
        assert!(mirror.sqrt_price_x96() > U160::ZERO);
        assert!(!mirror.ticks().is_empty());
    }
}

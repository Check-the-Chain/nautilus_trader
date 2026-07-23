// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Runtime ownership of one selected Uniswap v4 mirror feed.

use std::{str::FromStr, time::Instant};

use alloy::primitives::B256;
use nautilus_model::defi::{DexType, data::Block, rpc::RpcLog};
use thiserror::Error;

use super::{
    UniswapV4BootstrapHead, UniswapV4HeadGuard, UniswapV4HeadGuardError, UniswapV4MirrorController,
    UniswapV4MirrorControllerError, UniswapV4MirrorLogFilter,
};
use crate::rpc::types::RpcEventType;

const MAX_BUFFERED_POOL_MANAGER_LOGS: usize = 10_000;

/// Current control phase for the unified mirror feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UniswapV4MirrorRuntimePhase {
    AwaitingConfirmations,
    AwaitingHead,
    Live,
    RecoveryRequired,
}

/// Fail-closed runtime orchestration errors.
#[derive(Debug, Error)]
pub enum UniswapV4MirrorRuntimeError {
    #[error("invalid WSS block {field} '{value}': {reason}")]
    InvalidBlockHash {
        field: &'static str,
        value: String,
        reason: String,
    },
    #[error("received a PoolManager log before its subscription was confirmed")]
    LogBeforeConfirmation,
    #[error("PoolManager bootstrap buffer reached {limit} logs")]
    BufferOverflow { limit: usize },
    #[error(transparent)]
    Head(#[from] UniswapV4HeadGuardError),
    #[error(transparent)]
    Controller(#[from] UniswapV4MirrorControllerError),
}

/// Owns subscription confirmation, bootstrap, continuity, buffering, and recovery state.
#[derive(Debug)]
pub struct UniswapV4MirrorRuntime {
    controller: UniswapV4MirrorController,
    head_guard: UniswapV4HeadGuard,
    phase: UniswapV4MirrorRuntimePhase,
    blocks_confirmed: bool,
    pool_manager_confirmed: bool,
    buffered_logs: Vec<RpcLog>,
}

impl UniswapV4MirrorRuntime {
    #[must_use]
    pub fn new(controller: UniswapV4MirrorController, head_guard: UniswapV4HeadGuard) -> Self {
        Self {
            controller,
            head_guard,
            phase: UniswapV4MirrorRuntimePhase::AwaitingConfirmations,
            blocks_confirmed: false,
            pool_manager_confirmed: false,
            buffered_logs: Vec::new(),
        }
    }

    #[must_use]
    pub const fn phase(&self) -> UniswapV4MirrorRuntimePhase {
        self.phase
    }

    #[must_use]
    pub const fn controller(&self) -> &UniswapV4MirrorController {
        &self.controller
    }

    /// Returns the deterministic unified PoolManager subscription filter.
    ///
    /// # Errors
    ///
    /// Returns an error when no selected pool has been registered.
    pub fn subscription_filter(
        &self,
    ) -> Result<UniswapV4MirrorLogFilter, UniswapV4MirrorControllerError> {
        self.controller.unified_subscription_filter()
    }

    #[must_use]
    pub fn head_deadline(&self) -> Option<Instant> {
        self.head_guard.deadline()
    }

    /// Records one RPC subscription confirmation.
    pub fn handle_subscription_confirmation(&mut self, event: RpcEventType, now: Instant) {
        if self.phase == UniswapV4MirrorRuntimePhase::RecoveryRequired {
            return;
        }
        match event {
            RpcEventType::NewBlock => self.blocks_confirmed = true,
            RpcEventType::PoolManager(DexType::UniswapV4) => {
                self.pool_manager_confirmed = true;
            }
            _ => return,
        }
        if self.blocks_confirmed
            && self.pool_manager_confirmed
            && self.phase == UniswapV4MirrorRuntimePhase::AwaitingConfirmations
        {
            self.head_guard.begin_confirmation_cycle(now);
            self.phase = UniswapV4MirrorRuntimePhase::AwaitingHead;
        }
    }

    /// Applies or buffers one raw PoolManager log.
    ///
    /// # Errors
    ///
    /// Returns an error if the log precedes confirmation, the bootstrap buffer is full, or a live
    /// selected-pool log cannot be validated and applied.
    pub fn handle_pool_manager_log(
        &mut self,
        log: RpcLog,
    ) -> Result<(), UniswapV4MirrorRuntimeError> {
        if self.phase == UniswapV4MirrorRuntimePhase::RecoveryRequired {
            return Err(UniswapV4MirrorRuntimeError::Head(
                UniswapV4HeadGuardError::RecoveryRequired,
            ));
        }
        if !self.pool_manager_confirmed {
            return self.fail(UniswapV4MirrorRuntimeError::LogBeforeConfirmation);
        }
        if self.phase == UniswapV4MirrorRuntimePhase::Live {
            return self
                .controller
                .apply_live_log(&log)
                .map(|_| ())
                .map_err(UniswapV4MirrorRuntimeError::from)
                .or_else(|error| self.fail(error));
        }
        if self.buffered_logs.len() >= MAX_BUFFERED_POOL_MANAGER_LOGS {
            return self.fail(UniswapV4MirrorRuntimeError::BufferOverflow {
                limit: MAX_BUFFERED_POOL_MANAGER_LOGS,
            });
        }
        self.buffered_logs.push(log);
        Ok(())
    }

    /// Validates a WSS head and bootstraps on the first post-confirmation head.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed block hashes, head liveness/continuity failures, bootstrap
    /// failures, or buffered selected-pool logs that cannot be applied.
    pub async fn handle_block(
        &mut self,
        block: &Block,
        now: Instant,
    ) -> Result<(), UniswapV4MirrorRuntimeError> {
        if matches!(
            self.phase,
            UniswapV4MirrorRuntimePhase::AwaitingConfirmations
                | UniswapV4MirrorRuntimePhase::RecoveryRequired
        ) {
            return Ok(());
        }

        let hash = match parse_block_hash("hash", &block.hash) {
            Ok(hash) => hash,
            Err(error) => return self.fail(error),
        };
        let parent_hash = match parse_block_hash("parentHash", &block.parent_hash) {
            Ok(hash) => hash,
            Err(error) => return self.fail(error),
        };
        let outcome = self
            .head_guard
            .observe_head(block.number, hash, parent_hash, now)
            .map_err(UniswapV4MirrorRuntimeError::from)
            .or_else(|error| self.fail(error))?;

        if self.phase == UniswapV4MirrorRuntimePhase::Live {
            return Ok(());
        }
        debug_assert_eq!(outcome, super::UniswapV4HeadOutcome::First);

        let bootstrap_head = UniswapV4BootstrapHead {
            number: block.number,
            hash: block.hash.clone(),
        };
        if let Err(error) = self
            .controller
            .bootstrap_after_subscription_confirmation(&bootstrap_head)
            .await
        {
            return self.fail(error.into());
        }
        if let Err(error) = self.head_guard.check_liveness(Instant::now()) {
            return self.fail(error.into());
        }

        for log in std::mem::take(&mut self.buffered_logs) {
            if let Err(error) = self.controller.apply_live_log(&log) {
                return self.fail(error.into());
            }
        }
        self.phase = UniswapV4MirrorRuntimePhase::Live;
        Ok(())
    }

    /// Checks the head deadline and revokes all mirrors on expiry.
    ///
    /// # Errors
    ///
    /// Returns the latched head guard error when liveness is unavailable.
    pub fn check_head_liveness(&mut self, now: Instant) -> Result<(), UniswapV4MirrorRuntimeError> {
        self.head_guard
            .check_liveness(now)
            .map_err(UniswapV4MirrorRuntimeError::from)
            .or_else(|error| self.fail(error))
    }

    /// Revokes mirrors and waits for fresh post-reconnection confirmations.
    pub fn handle_reconnected(&mut self) {
        self.controller.mark_all_unavailable();
        self.head_guard.require_recovery();
        self.phase = UniswapV4MirrorRuntimePhase::AwaitingConfirmations;
        self.blocks_confirmed = false;
        self.pool_manager_confirmed = false;
        self.buffered_logs.clear();
    }

    /// Revokes mirrors after an RPC stream error while awaiting an actual reconnection cycle.
    pub fn handle_transport_failure(&mut self) {
        self.controller.mark_all_unavailable();
        self.head_guard.require_recovery();
        self.phase = UniswapV4MirrorRuntimePhase::RecoveryRequired;
        self.buffered_logs.clear();
    }

    fn fail<T>(
        &mut self,
        error: UniswapV4MirrorRuntimeError,
    ) -> Result<T, UniswapV4MirrorRuntimeError> {
        self.controller.mark_all_unavailable();
        self.head_guard.require_recovery();
        self.phase = UniswapV4MirrorRuntimePhase::RecoveryRequired;
        self.buffered_logs.clear();
        Err(error)
    }
}

fn parse_block_hash(field: &'static str, value: &str) -> Result<B256, UniswapV4MirrorRuntimeError> {
    B256::from_str(value).map_err(|error| UniswapV4MirrorRuntimeError::InvalidBlockHash {
        field,
        value: value.to_string(),
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use alloy::primitives::{B256, address};
    use nautilus_core::UnixNanos;
    use ustr::Ustr;

    use super::*;
    use crate::{
        exchanges::robinhood, rpc::http::BlockchainHttpRpcClient, services::UniswapV4MirrorConfig,
    };

    fn runtime() -> UniswapV4MirrorRuntime {
        let mut controller = UniswapV4MirrorController::new(
            robinhood::UNISWAP_V4.dex.clone(),
            address!("8366a39CC670B4001A1121B8F6A443A643e40951"),
            address!("F3334192D15450CdD385c8B70e03f9A6bD9E673b"),
            Arc::new(BlockchainHttpRpcClient::new(
                "http://unused.invalid".to_string(),
                None,
                None,
            )),
            100,
        );
        controller
            .register_pool(UniswapV4MirrorConfig::new_unchecked_for_test(
                B256::repeat_byte(1),
                60,
                3_000,
            ))
            .unwrap();
        UniswapV4MirrorRuntime::new(
            controller,
            UniswapV4HeadGuard::new(Duration::from_secs(5)).unwrap(),
        )
    }

    fn block(hash: &str, parent_hash: &str, number: u64) -> Block {
        Block::new(
            hash.to_string(),
            parent_hash.to_string(),
            number,
            Ustr::from("0x0000000000000000000000000000000000000000"),
            0,
            0,
            UnixNanos::default(),
            None,
        )
    }

    #[test]
    fn both_confirmations_are_required_before_head_deadline_starts() {
        let start = Instant::now();
        let mut runtime = runtime();
        runtime
            .handle_subscription_confirmation(RpcEventType::PoolManager(DexType::UniswapV4), start);
        assert_eq!(
            runtime.phase(),
            UniswapV4MirrorRuntimePhase::AwaitingConfirmations
        );
        assert_eq!(runtime.head_deadline(), None);

        runtime.handle_subscription_confirmation(RpcEventType::NewBlock, start);
        assert_eq!(runtime.phase(), UniswapV4MirrorRuntimePhase::AwaitingHead);
        assert_eq!(
            runtime.head_deadline(),
            Some(start + Duration::from_secs(5))
        );
    }

    #[test]
    fn timeout_requires_reconnect_and_fresh_confirmations() {
        let start = Instant::now();
        let mut runtime = runtime();
        runtime.handle_subscription_confirmation(RpcEventType::NewBlock, start);
        runtime
            .handle_subscription_confirmation(RpcEventType::PoolManager(DexType::UniswapV4), start);

        assert!(
            runtime
                .check_head_liveness(start + Duration::from_secs(5))
                .is_err()
        );
        assert_eq!(
            runtime.phase(),
            UniswapV4MirrorRuntimePhase::RecoveryRequired
        );
        runtime.handle_subscription_confirmation(RpcEventType::NewBlock, start);
        assert_eq!(
            runtime.phase(),
            UniswapV4MirrorRuntimePhase::RecoveryRequired
        );

        runtime.handle_reconnected();
        assert_eq!(
            runtime.phase(),
            UniswapV4MirrorRuntimePhase::AwaitingConfirmations
        );
        runtime.handle_subscription_confirmation(RpcEventType::NewBlock, start);
        runtime
            .handle_subscription_confirmation(RpcEventType::PoolManager(DexType::UniswapV4), start);
        assert_eq!(runtime.phase(), UniswapV4MirrorRuntimePhase::AwaitingHead);
    }

    #[test]
    fn pool_manager_log_before_confirmation_fails_closed() {
        let mut runtime = runtime();
        let log = RpcLog {
            removed: false,
            log_index: None,
            transaction_index: None,
            transaction_hash: None,
            block_hash: None,
            block_number: None,
            address: String::new(),
            data: String::new(),
            topics: Vec::new(),
        };

        assert!(matches!(
            runtime.handle_pool_manager_log(log),
            Err(UniswapV4MirrorRuntimeError::LogBeforeConfirmation)
        ));
        assert_eq!(
            runtime.phase(),
            UniswapV4MirrorRuntimePhase::RecoveryRequired
        );
    }

    #[tokio::test]
    async fn malformed_post_confirmation_head_fails_closed() {
        let start = Instant::now();
        let mut runtime = runtime();
        runtime.handle_subscription_confirmation(RpcEventType::NewBlock, start);
        runtime
            .handle_subscription_confirmation(RpcEventType::PoolManager(DexType::UniswapV4), start);

        assert!(matches!(
            runtime
                .handle_block(&block("not-a-hash", &B256::ZERO.to_string(), 10), start)
                .await,
            Err(UniswapV4MirrorRuntimeError::InvalidBlockHash { field: "hash", .. })
        ));
        assert_eq!(
            runtime.phase(),
            UniswapV4MirrorRuntimePhase::RecoveryRequired
        );
    }
}

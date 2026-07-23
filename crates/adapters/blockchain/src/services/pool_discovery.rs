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

use std::{
    cmp::{max, min},
    collections::{HashSet, VecDeque},
    time::Duration,
};

use alloy::primitives::Address;
use futures_util::StreamExt;
use nautilus_core::string::formatting::Separable;
use nautilus_model::defi::{
    Block, SharedDex,
    amm::Pool,
    chain::SharedChain,
    reporting::{BlockchainSyncReportItems, BlockchainSyncReporter},
    token::Token,
};
use tokio_util::sync::CancellationToken;

use crate::{
    cache::BlockchainCache,
    config::BlockchainDataClientConfig,
    contracts::erc20::{Erc20Contract, TokenInfoError},
    events::pool_created::PoolCreatedEvent,
    exchanges::extended::DexExtended,
    hypersync::{
        client::{HyperSyncClient, PoolEventStreamItem},
        helpers::extract_block_number,
    },
    rpc::{helpers as rpc_helpers, http::BlockchainHttpRpcClient},
};

const BLOCKS_PROCESS_IN_SYNC_REPORT: u64 = 50_000;
const POOL_DB_BATCH_SIZE: usize = 2000;
const POOL_EVENT_BLOCK_DB_BATCH_SIZE: usize = 20_000;
const RPC_LOG_INITIAL_BLOCK_CHUNK_SIZE: u64 = 5_000_000;
const RPC_MAX_ATTEMPTS: u32 = 7;
const RPC_MAX_BACKOFF_SECS: u64 = 30;

#[derive(Debug, Clone, Copy)]
enum PoolDiscoveryProvider {
    HyperSync,
    Rpc,
}

#[derive(Debug)]
struct PoolDiscoveryBuffers {
    token_rpc_batch_size: usize,
    token_rpc: HashSet<Address>,
    token_db: Vec<Token>,
    pool_events: Vec<PoolCreatedEvent>,
    blocks: Vec<Block>,
}

impl PoolDiscoveryBuffers {
    fn new(multicall_calls_per_rpc_request: u32) -> Self {
        Self {
            token_rpc_batch_size: max((multicall_calls_per_rpc_request / 3) as usize, 1),
            token_rpc: HashSet::new(),
            token_db: Vec::new(),
            pool_events: Vec::new(),
            blocks: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct PoolDiscoveryStats {
    discovered: usize,
    skipped_exists: usize,
    skipped_invalid_tokens: usize,
    saved: usize,
}

/// Sanitizes a string by removing null bytes and other invalid characters for PostgreSQL UTF-8.
///
/// This function strips null bytes (0x00) and other problematic control characters that are
/// invalid in PostgreSQL's UTF-8 text fields. Common with malformed on-chain token metadata.
/// Preserves printable characters and common whitespace (space, tab, newline).
fn sanitize_string(s: &str) -> String {
    s.chars()
        .filter(|c| {
            // Keep printable characters and common whitespace, but filter null bytes
            // and other problematic control characters
            *c != '\0' && (*c >= ' ' || *c == '\t' || *c == '\n' || *c == '\r')
        })
        .collect()
}

/// Service responsible for discovering DEX liquidity pools from blockchain events.
///
/// This service handles the synchronization of pool creation events from various DEXes,
/// managing token metadata fetching, buffering strategies, and database persistence.
#[derive(Debug)]
pub struct PoolDiscoveryService<'a> {
    /// The blockchain network being synced
    chain: SharedChain,
    /// Cache for tokens and pools
    cache: &'a mut BlockchainCache,
    /// ERC20 contract interface for token metadata
    erc20_contract: &'a Erc20Contract,
    /// Optional HyperSync client for event streaming
    hypersync_client: Option<&'a HyperSyncClient>,
    /// HTTP RPC client for RPC-native discovery
    http_rpc_client: &'a BlockchainHttpRpcClient,
    /// Cancellation token for graceful shutdown
    cancellation_token: CancellationToken,
    /// Configuration for sync operations
    config: BlockchainDataClientConfig,
}

impl<'a> PoolDiscoveryService<'a> {
    /// Creates a new [`PoolDiscoveryService`] instance.
    #[must_use]
    pub const fn new(
        chain: SharedChain,
        cache: &'a mut BlockchainCache,
        erc20_contract: &'a Erc20Contract,
        hypersync_client: Option<&'a HyperSyncClient>,
        http_rpc_client: &'a BlockchainHttpRpcClient,
        cancellation_token: CancellationToken,
        config: BlockchainDataClientConfig,
    ) -> Self {
        Self {
            chain,
            cache,
            erc20_contract,
            hypersync_client,
            http_rpc_client,
            cancellation_token,
            config,
        }
    }

    /// Synchronizes pools for a specific DEX within a given block range.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - HyperSync streaming fails
    /// - Token RPC calls fail
    /// - Database operations fail
    /// - Sync is cancelled
    pub async fn sync_pools(
        &mut self,
        dex: &DexExtended,
        from_block: u64,
        to_block: Option<u64>,
        reset: bool,
    ) -> anyhow::Result<()> {
        let requested_from_block = max(from_block, dex.factory_creation_block);
        let (last_synced_block, effective_from_block) = if reset {
            (None, requested_from_block)
        } else {
            let last_synced_block = self.cache.get_dex_last_synced_block(&dex.dex.name).await?;
            let effective_from_block = last_synced_block
                .map_or(requested_from_block, |last_synced| {
                    max(requested_from_block, last_synced.saturating_add(1))
                });
            (last_synced_block, effective_from_block)
        };

        let provider = self.discovery_provider(dex)?;
        let to_block = match provider {
            PoolDiscoveryProvider::HyperSync => match to_block {
                Some(block) => block,
                None => {
                    self.hypersync_client
                        .ok_or_else(|| {
                            anyhow::anyhow!("HyperSync discovery client is unavailable")
                        })?
                        .current_block()
                        .await
                }
            },
            PoolDiscoveryProvider::Rpc => {
                let finalized_block = self.http_rpc_client.finalized_block().await?;
                if to_block.is_some_and(|requested| requested > finalized_block) {
                    log::warn!("Clamping RPC pool discovery to finalized block {finalized_block}");
                }
                min(to_block.unwrap_or(finalized_block), finalized_block)
            }
        };

        if effective_from_block > to_block {
            log::info!(
                "DEX {} already synced to block {} (current: {}), skipping sync",
                dex.dex.name,
                last_synced_block.unwrap_or(0).separate_with_commas(),
                to_block.separate_with_commas()
            );
            return Ok(());
        }

        let total_blocks = to_block.saturating_sub(effective_from_block) + 1;
        log::debug!(
            "Syncing DEX exchange pools from {} to {} (total: {} blocks){}",
            effective_from_block.separate_with_commas(),
            to_block.separate_with_commas(),
            total_blocks.separate_with_commas(),
            last_synced_block.map_or_else(String::new, |last_synced| format!(
                " - resuming from last synced block {}",
                last_synced.separate_with_commas()
            )),
        );
        log::debug!(
            "Syncing {} pool creation events from factory contract {} on chain {} via {provider:?}",
            dex.dex.name,
            dex.factory,
            self.chain.name
        );

        if let Err(e) = self.cache.toggle_performance_settings(true).await {
            log::warn!("Failed to enable performance settings: {e}");
        }

        let mut metrics = BlockchainSyncReporter::new(
            BlockchainSyncReportItems::PoolCreatedEvents,
            effective_from_block,
            total_blocks,
            BLOCKS_PROCESS_IN_SYNC_REPORT,
        );
        let mut buffers = PoolDiscoveryBuffers::new(self.config.multicall_calls_per_rpc_request);
        let mut stats = PoolDiscoveryStats::default();
        let mut last_block_saved = effective_from_block;
        let cancellation_token = self.cancellation_token.clone();

        let sync_result = tokio::select! {
            () = cancellation_token.cancelled() => {
                log::debug!("Exchange pool sync cancelled");
                Err(anyhow::anyhow!("Sync cancelled"))
            }
            result = async {
                match provider {
                    PoolDiscoveryProvider::HyperSync => {
                        let hypersync_client = self
                            .hypersync_client
                            .ok_or_else(|| anyhow::anyhow!("HyperSync discovery client is unavailable"))?;
                        let pools_stream = hypersync_client
                            .request_contract_events_stream(
                                effective_from_block,
                                Some(to_block),
                                &dex.factory,
                                vec![dex.pool_created_event.as_ref()],
                            )
                            .await;
                        tokio::pin!(pools_stream);

                        while let Some(item) = pools_stream.next().await {
                            let log = match item {
                                PoolEventStreamItem::Block(block) => {
                                    self.cache.cache_block_timestamp(block.number, block.timestamp);
                                    buffers.blocks.push(block);
                                    if buffers.blocks.len() >= POOL_EVENT_BLOCK_DB_BATCH_SIZE {
                                        self.flush_pool_event_blocks(&mut buffers.blocks).await?;
                                    }
                                    continue;
                                }
                                PoolEventStreamItem::Log(log) => log,
                            };
                            let block_number = extract_block_number(&log)?;
                            let pool = dex.parse_pool_created_event_hypersync(log)?;
                            self.process_pool_created_event(pool, dex, &mut buffers, &mut stats)
                                .await?;
                            Self::update_discovery_progress(
                                block_number,
                                to_block,
                                &mut last_block_saved,
                                &mut metrics,
                            );
                        }
                    }
                    PoolDiscoveryProvider::Rpc => {
                        let mut ranges = VecDeque::new();
                        let mut chunk_start = effective_from_block;
                        while chunk_start <= to_block {
                            let chunk_end = min(
                                chunk_start
                                    .saturating_add(RPC_LOG_INITIAL_BLOCK_CHUNK_SIZE - 1),
                                to_block,
                            );
                            ranges.push_back((chunk_start, chunk_end));
                            if chunk_end == u64::MAX {
                                break;
                            }
                            chunk_start = chunk_end + 1;
                        }

                        'ranges: while let Some((range_start, range_end)) = ranges.pop_front() {
                            let mut attempt = 1;
                            let logs = loop {
                                match self
                                    .http_rpc_client
                                    .get_logs(
                                        Some(&dex.factory),
                                        Some(vec![Some(dex.pool_created_event.to_string())]),
                                        range_start,
                                        range_end,
                                    )
                                    .await
                                {
                                    Ok(logs) => break logs,
                                    Err(error)
                                        if range_start < range_end
                                            && Self::is_log_query_limit_error(&error) =>
                                    {
                                        let midpoint = range_start + (range_end - range_start) / 2;
                                        ranges.push_front((midpoint + 1, range_end));
                                        ranges.push_front((range_start, midpoint));
                                        continue 'ranges;
                                    }
                                    Err(error)
                                        if attempt < RPC_MAX_ATTEMPTS
                                            && Self::is_retryable_rpc_error(&error) =>
                                    {
                                        let delay = Self::rpc_retry_delay(attempt);
                                        log::warn!(
                                            "Retryable pool discovery RPC failure for blocks {range_start}-{range_end}: {error}; retrying in {}s (attempt {}/{RPC_MAX_ATTEMPTS})",
                                            delay.as_secs(),
                                            attempt + 1,
                                        );
                                        tokio::time::sleep(delay).await;
                                        attempt += 1;
                                    }
                                    Err(error) => return Err(error),
                                }
                            };
                            let mut positioned_logs = logs
                                .into_iter()
                                .filter(|log| !log.removed)
                                .map(|log| {
                                    let position = (
                                        rpc_helpers::extract_block_number(&log)?,
                                        rpc_helpers::extract_transaction_index(&log)?,
                                        rpc_helpers::extract_log_index(&log)?,
                                    );
                                    Ok((position, log))
                                })
                                .collect::<anyhow::Result<Vec<_>>>()?;
                            positioned_logs.sort_by_key(|(position, _)| *position);

                            for ((block_number, _, _), log) in positioned_logs {
                                self.cache_rpc_pool_event_block(
                                    block_number,
                                    &mut buffers.blocks,
                                )
                                .await?;
                                let pool = dex.parse_pool_created_event_rpc(&log)?;
                                self.process_pool_created_event(pool, dex, &mut buffers, &mut stats)
                                    .await?;
                                Self::update_discovery_progress(
                                    block_number,
                                    to_block,
                                    &mut last_block_saved,
                                    &mut metrics,
                                );
                            }

                        }
                    }
                }

                stats.saved += self.flush_discovery_buffers(&mut buffers, &dex.dex).await?;
                self.flush_pool_event_blocks(&mut buffers.blocks).await?;
                metrics.log_final_stats();
                self.cache
                    .update_dex_last_synced_block(&dex.dex.name, to_block)
                    .await?;

                log::debug!(
                    "Successfully synced DEX {} pools up to block {} | Summary: discovered={}, saved={}, skipped_exists={}, skipped_invalid_tokens={}",
                    dex.dex.name,
                    to_block.separate_with_commas(),
                    stats.discovered,
                    stats.saved,
                    stats.skipped_exists,
                    stats.skipped_invalid_tokens
                );
                Ok(())
            } => result
        };

        let restore_result = self.cache.toggle_performance_settings(false).await;
        if let Err(e) = restore_result {
            log::warn!("Failed to restore default settings: {e}");
        }
        sync_result
    }

    fn discovery_provider(&self, dex: &DexExtended) -> anyhow::Result<PoolDiscoveryProvider> {
        if self.hypersync_client.is_some() && dex.supports_pool_discovery_hypersync() {
            Ok(PoolDiscoveryProvider::HyperSync)
        } else if dex.supports_pool_discovery_rpc() {
            Ok(PoolDiscoveryProvider::Rpc)
        } else {
            anyhow::bail!(
                "Pool discovery is unsupported for DEX {} on chain {}",
                dex.name,
                self.chain.name
            )
        }
    }

    fn update_discovery_progress(
        block_number: u64,
        to_block: u64,
        last_block_saved: &mut u64,
        metrics: &mut BlockchainSyncReporter,
    ) {
        metrics.update(block_number.saturating_sub(*last_block_saved) as usize);
        *last_block_saved = block_number;
        if metrics.should_log_progress(block_number, to_block) {
            metrics.log_progress(block_number);
        }
    }

    fn is_log_query_limit_error(error: &anyhow::Error) -> bool {
        let message = error.to_string().to_ascii_lowercase();
        [
            "exceeds limit",
            "limit exceeded",
            "too many results",
            "block range",
            "response size",
            "payload too large",
        ]
        .iter()
        .any(|needle| message.contains(needle))
    }

    fn is_retryable_rpc_error(error: &anyhow::Error) -> bool {
        let message = error.to_string().to_ascii_lowercase();
        [
            "429",
            "too many requests",
            "rate limit",
            "timed out",
            "timeout",
            "connection reset",
            "connection refused",
            "temporarily unavailable",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
            "500",
            "internal error",
        ]
        .iter()
        .any(|needle| message.contains(needle))
    }

    fn is_retryable_token_metadata_error(error: &TokenInfoError) -> bool {
        matches!(
            error,
            TokenInfoError::RpcError(_) | TokenInfoError::UnexpectedResultCount { .. }
        )
    }

    fn rpc_retry_delay(attempt: u32) -> Duration {
        let seconds = 1_u64
            .checked_shl(attempt.saturating_sub(1))
            .unwrap_or(u64::MAX)
            .min(RPC_MAX_BACKOFF_SECS);
        Duration::from_secs(seconds)
    }

    async fn process_pool_created_event(
        &mut self,
        pool: PoolCreatedEvent,
        dex: &DexExtended,
        buffers: &mut PoolDiscoveryBuffers,
        stats: &mut PoolDiscoveryStats,
    ) -> anyhow::Result<()> {
        stats.discovered += 1;
        if self.cache.get_pool(&pool.pool_identifier).is_some() {
            stats.skipped_exists += 1;
            return Ok(());
        }

        self.cache_native_token(pool.token0, &mut buffers.token_db);
        self.cache_native_token(pool.token1, &mut buffers.token_db);

        let token0_invalid = self.cache.get_token(&pool.token0).is_none()
            && self.cache.is_invalid_token(&pool.token0);
        let token1_invalid = self.cache.get_token(&pool.token1).is_none()
            && self.cache.is_invalid_token(&pool.token1);
        if token0_invalid || token1_invalid {
            stats.skipped_invalid_tokens += 1;
            return Ok(());
        }

        if self.cache.get_token(&pool.token0).is_none() {
            buffers.token_rpc.insert(pool.token0);
        }
        if self.cache.get_token(&pool.token1).is_none() {
            buffers.token_rpc.insert(pool.token1);
        }
        buffers.pool_events.push(pool);

        if buffers.token_rpc.len() >= buffers.token_rpc_batch_size {
            let fetched_tokens = self
                .fetch_and_cache_tokens_in_memory(&mut buffers.token_rpc)
                .await?;
            buffers.token_db.extend(fetched_tokens);
        }
        if buffers.pool_events.len() >= POOL_DB_BATCH_SIZE {
            stats.saved += self.flush_discovery_buffers(buffers, &dex.dex).await?;
        }
        Ok(())
    }

    fn cache_native_token(&mut self, address: Address, token_db_buffer: &mut Vec<Token>) {
        if address != Address::ZERO || self.cache.get_token(&address).is_some() {
            return;
        }

        let currency = self.chain.native_currency();
        let token = Token::new(
            self.chain.clone(),
            Address::ZERO,
            currency.name.to_string(),
            currency.code.to_string(),
            currency.precision,
        );
        self.cache.insert_token_in_memory(token.clone());
        token_db_buffer.push(token);
    }

    async fn flush_discovery_buffers(
        &mut self,
        buffers: &mut PoolDiscoveryBuffers,
        dex: &SharedDex,
    ) -> anyhow::Result<usize> {
        // Pool creation timestamps must be durable before any pool row that references them.
        self.flush_pool_event_blocks(&mut buffers.blocks).await?;
        if !buffers.token_rpc.is_empty() {
            let fetched_tokens = self
                .fetch_and_cache_tokens_in_memory(&mut buffers.token_rpc)
                .await?;
            buffers.token_db.extend(fetched_tokens);
        }
        if !buffers.token_db.is_empty() {
            self.cache
                .add_tokens_batch(std::mem::take(&mut buffers.token_db))
                .await?;
        }
        if buffers.pool_events.is_empty() {
            return Ok(0);
        }

        let pools = self
            .construct_pools_batch(&mut buffers.pool_events, dex)
            .await?;
        let saved = pools.len();
        self.cache.add_pools_batch(pools).await?;
        Ok(saved)
    }

    async fn flush_pool_event_blocks(&mut self, blocks: &mut Vec<Block>) -> anyhow::Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        self.cache
            .add_pool_event_blocks_batch(std::mem::take(blocks))
            .await
    }

    async fn cache_rpc_pool_event_block(
        &mut self,
        block_number: u64,
        blocks: &mut Vec<Block>,
    ) -> anyhow::Result<()> {
        if self.cache.get_block_timestamp(block_number).is_some() {
            return Ok(());
        }

        let mut attempt = 1;
        let mut block = loop {
            match self.http_rpc_client.block(block_number).await {
                Ok(block) => break block,
                Err(error)
                    if attempt < RPC_MAX_ATTEMPTS && Self::is_retryable_rpc_error(&error) =>
                {
                    let delay = Self::rpc_retry_delay(attempt);
                    log::warn!(
                        "Retryable block header RPC failure for block {block_number}: {error}; retrying in {}s (attempt {}/{RPC_MAX_ATTEMPTS})",
                        delay.as_secs(),
                        attempt + 1,
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
                Err(error) => return Err(error),
            }
        };
        block.chain = Some(self.chain.name);
        self.cache
            .cache_block_timestamp(block.number, block.timestamp);
        blocks.push(block);
        if blocks.len() >= POOL_EVENT_BLOCK_DB_BATCH_SIZE {
            self.flush_pool_event_blocks(blocks).await?;
        }
        Ok(())
    }

    /// Fetches token metadata via RPC and updates in-memory cache immediately.
    ///
    /// This method fetches token information using multicall, updates the in-memory cache right away
    /// (so pool construction can proceed), and returns valid tokens for later batch DB writes.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC multicall fails or database operations fail.
    async fn fetch_and_cache_tokens_in_memory(
        &mut self,
        token_buffer: &mut HashSet<Address>,
    ) -> anyhow::Result<Vec<Token>> {
        let batch_addresses: Vec<Address> = token_buffer.drain().collect();
        let mut attempt = 1;
        let token_infos = loop {
            match self
                .erc20_contract
                .batch_fetch_token_info(&batch_addresses)
                .await
            {
                Ok(token_infos) => {
                    let retryable_failure = token_infos.iter().find_map(
                        |(token_address, token_info)| match token_info {
                            Err(error) if Self::is_retryable_token_metadata_error(error) => {
                                Some((*token_address, error.to_string()))
                            }
                            _ => None,
                        },
                    );
                    let Some((token_address, error)) = retryable_failure else {
                        break token_infos;
                    };
                    if attempt >= RPC_MAX_ATTEMPTS {
                        anyhow::bail!(
                            "Failed to fetch token metadata for {token_address} after {attempt} attempts: {error}"
                        );
                    }
                    let delay = Self::rpc_retry_delay(attempt);
                    log::warn!(
                        "Retryable token metadata failure for {token_address}: {error}; retrying batch in {}s (attempt {}/{RPC_MAX_ATTEMPTS})",
                        delay.as_secs(),
                        attempt + 1,
                    );
                    tokio::time::sleep(delay).await;
                }
                Err(error) => {
                    if attempt >= RPC_MAX_ATTEMPTS {
                        return Err(error.into());
                    }
                    let delay = Self::rpc_retry_delay(attempt);
                    log::warn!(
                        "Retryable token metadata batch failure: {error}; retrying in {}s (attempt {}/{RPC_MAX_ATTEMPTS})",
                        delay.as_secs(),
                        attempt + 1,
                    );
                    tokio::time::sleep(delay).await;
                }
            }
            attempt += 1;
        };

        let mut valid_tokens = Vec::new();

        for (token_address, token_info) in token_infos {
            match token_info {
                Ok(token_info) => {
                    // Sanitize token metadata to remove null bytes and invalid UTF-8 characters
                    let sanitized_name = sanitize_string(&token_info.name);
                    let sanitized_symbol = sanitize_string(&token_info.symbol);

                    let token = Token::new(
                        self.chain.clone(),
                        token_address,
                        sanitized_name,
                        sanitized_symbol,
                        token_info.decimals,
                    );

                    // Update in-memory cache IMMEDIATELY (so construct_pool can read it)
                    self.cache.insert_token_in_memory(token.clone());

                    // Collect for LATER DB write
                    valid_tokens.push(token);
                }
                Err(token_info_error) => {
                    self.cache.insert_invalid_token_in_memory(token_address);
                    if let Some(database) = &self.cache.database {
                        let sanitized_error = sanitize_string(&token_info_error.to_string());
                        database
                            .add_invalid_token(
                                self.chain.chain_id,
                                &token_address,
                                &sanitized_error,
                            )
                            .await?;
                    }
                }
            }
        }

        Ok(valid_tokens)
    }

    /// Constructs multiple pools from pool creation events.
    ///
    /// Assumes all required tokens are already in the in-memory cache.
    ///
    /// # Errors
    ///
    /// Logs errors for pools that cannot be constructed (missing tokens),
    /// but does not fail the entire batch.
    async fn construct_pools_batch(
        &self,
        pool_events: &mut Vec<PoolCreatedEvent>,
        dex: &SharedDex,
    ) -> anyhow::Result<Vec<Pool>> {
        let mut pools = Vec::with_capacity(pool_events.len());

        for pool_event in pool_events.drain(..) {
            // Both tokens should be in cache now
            let token0 = match self.cache.get_token(&pool_event.token0) {
                Some(token) => token.clone(),
                None => {
                    if !self.cache.is_invalid_token(&pool_event.token0) {
                        log::warn!(
                            "Skipping pool {}: Token0 {} not in cache and not marked as invalid",
                            pool_event.pool_address,
                            pool_event.token0
                        );
                    }
                    continue;
                }
            };

            let token1 = match self.cache.get_token(&pool_event.token1) {
                Some(token) => token.clone(),
                None => {
                    if !self.cache.is_invalid_token(&pool_event.token1) {
                        log::warn!(
                            "Skipping pool {}: Token1 {} not in cache and not marked as invalid",
                            pool_event.pool_address,
                            pool_event.token1
                        );
                    }
                    continue;
                }
            };

            let ts_init = self
                .cache
                .get_block_timestamp(pool_event.block_number)
                .copied()
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "Missing creation timestamp for pool {} at block {}",
                        pool_event.pool_address,
                        pool_event.block_number,
                    )
                })?;

            let mut pool = Pool::new(
                self.chain.clone(),
                dex.clone(),
                pool_event.pool_address,
                pool_event.pool_identifier,
                pool_event.block_number,
                token0,
                token1,
                pool_event.fee,
                pool_event.tick_spacing,
                ts_init,
            );

            if let Some(amm_type) = pool_event.amm_type {
                pool.set_amm_type(amm_type);
            }

            // Set hooks if available (UniswapV4)
            if let Some(hooks) = pool_event.hooks {
                pool.set_hooks(hooks);
            }

            // Initialize pool with sqrt_price_x96 and tick if available (UniswapV4)
            if let (Some(sqrt_price_x96), Some(tick)) = (pool_event.sqrt_price_x96, pool_event.tick)
            {
                pool.initialize(sqrt_price_x96, tick);
            }

            pools.push(pool);
        }

        Ok(pools)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use alloy::primitives::Address;
    use rstest::rstest;

    use super::PoolDiscoveryService;
    use crate::{
        contracts::erc20::{Erc20Field, TokenInfoError},
        rpc::error::BlockchainRpcClientError,
    };

    #[rstest]
    #[case("RPC error -32000: logs matched by query exceeds limit of 10000")]
    #[case("query returned too many results")]
    #[case("requested block range is too wide")]
    #[case("response size exceeded")]
    #[case("query limit exceeded")]
    #[case("payload too large")]
    fn recognizes_log_query_limit_errors(#[case] message: &str) {
        assert!(PoolDiscoveryService::is_log_query_limit_error(
            &anyhow::anyhow!("{message}")
        ));
    }

    #[rstest]
    fn does_not_treat_transport_failure_as_log_query_limit() {
        assert!(!PoolDiscoveryService::is_log_query_limit_error(
            &anyhow::anyhow!("connection reset by peer")
        ));
    }

    #[rstest]
    #[case("RPC error 429: Too Many Requests")]
    #[case("connection reset by peer")]
    #[case("504 Gateway Timeout")]
    #[case("500 Internal Server Error")]
    #[case("RPC error -32603: internal error")]
    fn recognizes_retryable_rpc_errors(#[case] message: &str) {
        assert!(PoolDiscoveryService::is_retryable_rpc_error(
            &anyhow::anyhow!("{message}")
        ));
    }

    #[rstest]
    fn token_rpc_failure_is_retryable() {
        let error = TokenInfoError::RpcError(BlockchainRpcClientError::ClientError(
            "connection reset".to_string(),
        ));

        assert!(PoolDiscoveryService::is_retryable_token_metadata_error(
            &error
        ));
    }

    #[rstest]
    fn empty_token_field_is_deterministically_invalid() {
        let error = TokenInfoError::EmptyTokenField {
            field: Erc20Field::Symbol,
            address: Address::ZERO,
        };

        assert!(!PoolDiscoveryService::is_retryable_token_metadata_error(
            &error
        ));
    }

    #[rstest]
    #[case(1, 1)]
    #[case(2, 2)]
    #[case(6, 30)]
    #[case(20, 30)]
    fn rpc_retry_delay_is_exponential_and_bounded(
        #[case] attempt: u32,
        #[case] expected_seconds: u64,
    ) {
        assert_eq!(
            PoolDiscoveryService::rpc_retry_delay(attempt),
            Duration::from_secs(expected_seconds)
        );
    }
}

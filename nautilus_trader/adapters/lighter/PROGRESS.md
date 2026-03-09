# Lighter Adapter Progress

This file tracks the implementation status of the Nautilus Lighter adapter.

## Goals

- Build a fully featured Lighter adapter in Nautilus idioms.
- Keep the protocol implementation Rust-first, with thin Python wrappers.
- Use official Lighter docs as the primary source of truth.
- Follow existing adapter conventions from Hyperliquid, Binance, Bybit, and OKX.

## Scope

- Public market data
  - Instruments
  - Order book snapshots and streaming updates
  - Quotes / ticker
  - Trades
  - Bars / candles
  - Mark / index / funding data where supported
- Private execution
  - Submit
  - Modify
  - Cancel
  - Cancel all
  - Batch operations where Lighter supports them
  - Order status reports
  - Fill reports
  - Position reports
  - Account state updates
- Venue-specific admin helpers
  - Auth token creation
  - Nonce handling
  - Leverage and margin updates
  - Transfer / withdraw helpers
  - Account / funding / history helpers

## Current Status

- [x] Created adapter folders and progress tracker
- [x] Scaffold Rust crate layout
- [x] Scaffold Python package layout
- [x] Implement Rust config and constants
- [x] Implement Rust signing and nonce management
- [x] Implement Rust HTTP client and models
- [x] Implement Rust WebSocket client and parsers
- [x] Implement Rust instrument provider helpers
- [x] Implement Rust data client
- [x] Implement Rust execution client
- [x] Implement Rust factories
- [x] Implement PyO3 bindings
- [x] Implement Rust-side factory / registry integration
- [x] Implement Python config/provider/factory wrappers
- [x] Implement Python data client wrapper
- [x] Implement Python execution client wrapper
- [x] Wire into workspace and `nautilus-pyo3`
- [x] Validate Python adapter package with `ruff` and `compileall`
- [x] Validate Rust crates with `cargo check`
- [x] Add Python integration tests
- [x] Add Rust transport integration tests
- [x] Add broader adapter test coverage
- [x] Align contingent-order handling with stable adapter semantics
- [x] Expose account-management helper surface through PyO3/Python adapter layers
- [x] Expose token, pool, L1, and notification helper surface through PyO3/Python layers
- [x] Expose referral, liquidation, transfer-fee, and withdrawal-delay helper surface through PyO3/Python adapter layers
- [x] Expose public-info, bridge, fast-withdraw, and lease helper surface through PyO3/Python adapter layers
- [x] Expose root/system, export, and tx-history helper surface through PyO3/Python adapter layers
- [x] Add examples
- [x] Add adapter README
- [x] Add stable-style sandbox smoke coverage for live public/private REST and WebSocket flows

## Notes

- Private Lighter flow depends on signed transactions, API-key indices, nonces, and expiring auth tokens.
- The clean implementation path is closer to Hyperliquid and dYdX than to a pure CEX adapter, but code style should still match the rest of Nautilus.
- The current adapter is Rust-first for transport/signing and Python-first for Nautilus normalization and orchestration.
- The Python data client now maintains local book state so Lighter order book deltas are applied correctly after the initial subscribed snapshot.
- The Python execution client now checks signed transaction responses and exposes venue-specific helper methods for limits, PnL, position funding, transfer / withdraw, and account history queries.
- The adapter now has stable-style Python integration coverage for providers, config/factories, parsing, data-client behavior, and execution-client behavior.
- The Python integration suite now mirrors the mature adapter test shape more closely, including failure paths, cancel-all fallback handling, modify flows, and subscribe/unsubscribe coverage.
- The Python execution client now uses Lighter batch transaction support for order-list submission and batch cancel flows, with chunking aligned to the documented `sendTxBatch` limit.
- Unsupported contingent order lists are now denied explicitly rather than flattened into plain Lighter batches, matching the safer behavior used by the mature adapters when venue semantics do not map cleanly.
- Cached Nautilus contingent metadata is now reapplied to both reconciliation and WebSocket-derived order status reports so linked/order-list relationships survive report generation consistently.
- The venue-helper surface now exposes account metadata, sub-account discovery, account tier changes, public-key rotation, and sub-account creation through the same PyO3/Python layers as the other Lighter admin helpers.
- The Python execution integration suite now covers the venue-helper layer as well as the core order/report paths, including auth-gated helper calls and error handling for admin transactions.
- The venue-helper surface now also covers API token management, public-pool metadata and pool transactions, L1 metadata lookup, L1 transaction lookup, and notification acknowledgement using the officially documented REST endpoints.
- The venue-helper surface now also covers referral-code lifecycle helpers, user-referral queries, liquidation history, transfer-fee lookup, and withdrawal-delay lookup using the officially documented REST endpoints.
- The venue-helper surface now also covers announcements, exchange metrics, execute stats, intent-address creation, deposit helper endpoints, fast-withdraw helper endpoints, and fee-credit lease helpers.
- The venue-helper surface now also covers root status, system config, layer1-basic-info, zkLighter contract info, transaction-history lookup, and export helper endpoints.
- The Rust HTTP integration suite now exercises the additional helper endpoints so token/public-pool/L1 request wiring is checked at the transport layer rather than only through Python mocks.
- The Rust HTTP integration suite now also exercises the new public-info, bridge, fast-withdraw, and lease routes, while the Python execution integration suite covers the same helper surface from the Nautilus-facing layer.
- The Python execution integration suite now also covers the root/system and tx/export helpers so the public/no-auth helper split and auth-gated export path are both exercised.
- The Python execution integration suite now exercises the added referral and account-info helpers, including success-path forwarding and stable-style failure-path handling for referral mutations.
- The Rust crate now has mock-server integration coverage for HTTP and WebSocket transport behavior, including auth/query wiring, metadata loading, subscribe/unsubscribe, and ping/pong handling.
- The Rust crate now has stable-style native `data_client.rs` and `exec_client.rs` suites covering lifecycle, subscription wiring, report generation, individual execution commands, batch submit/cancel flows, and filtered cancel-all behavior.
- The native execution client now keeps a tracked-order map so private WebSocket reports can synthesize order/fill events for orders submitted through the client, while still emitting reconciliation reports.
- The repo docs now include Lighter integration and API-reference pages, plus README/index entries so the adapter is discoverable alongside the other maintained integrations.
- The adapter now follows the same sandbox-test pattern used by other mature adapters, with opt-in live smoke scripts for public HTTP, private HTTP, public WebSocket, and private WebSocket coverage.
- Live public validation now covers the current mainnet quirks as well: single-token perp symbols in metadata, colon-delimited WebSocket channel names in payloads, and the read-only `?readonly=true` public WebSocket variant documented by Lighter.
- The remaining work is incremental adapter hardening: broader authenticated live smoke runs against real Lighter accounts and venue-specific edge-case expansion as the docs or exchange behavior change.

# Lighter Adapter

The Lighter adapter is split across two layers:

- `crates/adapters/lighter`
  The Rust transport, signing, nonce, REST, and WebSocket layer exposed through PyO3.
- `nautilus_trader/adapters/lighter`
  The Nautilus-facing Python adapter which normalizes Lighter payloads into instruments, market data, account state, orders, fills, and positions.

## Supported Surface

- Public market data
  - Instrument discovery
  - Order book snapshot requests
  - Order book streaming
  - Quotes / ticker
  - Trades
  - Historical candles
  - Mark / index / funding updates for perpetuals
- Private execution
  - Submit
  - Modify
  - Cancel
  - Cancel-all via Nautilus command fanout
  - Order status reports
  - Fill reports
  - Position status reports
  - Account balance / margin updates
- Venue-specific helpers
  - Auth token creation
  - Account metadata and sub-account queries
  - L1 metadata and L1 transaction lookup
  - Account tier changes
  - Liquidation, transfer-fee, and withdrawal-delay queries
  - Referral code and referral-usage helpers
  - Announcements, exchange metrics, and execute stats
  - Intent-address, deposit, fast-bridge, and fast-withdraw helpers
  - Lease option and lease history helpers
  - System config, root status, layer1-basic-info, tx-history, and export helpers
  - API token list / create / revoke helpers
  - Public pool metadata plus public-pool transaction helpers
  - Leverage updates
  - Margin updates
  - API key rotation and sub-account creation
  - Notification acknowledgement
  - Transfer / withdraw helpers

## Symbol Convention

The provider keeps the venue raw symbol in `raw_symbol` and appends a market-type suffix to the Nautilus symbol:

- Perpetual: `BTC-USDC-PERP.LIGHTER`
- Spot: `ETH-USDC-SPOT.LIGHTER`

## Configuration

Use:

- `LighterDataClientConfig`
- `LighterExecClientConfig`
- `LighterLiveDataClientFactory`
- `LighterLiveExecClientFactory`

Important execution fields:

- `account_index`
  Lighter account/subaccount index.
- `api_key_index`
  The API key index used for auth token generation and signed transactions.
- `private_key`
  The API private key for that API key index.
- `signer_lib_path`
  Optional override for the signer shared library path.
- `testnet`
  Switches the default REST / WebSocket endpoints.

You can also pass `api_private_keys` if you want to provide more than one key to the Rust signer client.

## Examples

See:

- `/Users/ungus/Documents/willy/nautilus_trader/examples/live/lighter/lighter_data_tester.py`
- `/Users/ungus/Documents/willy/nautilus_trader/examples/live/lighter/lighter_exec_tester.py`
- `/Users/ungus/Documents/willy/nautilus_trader/tests/integration_tests/adapters/lighter/sandbox/sandbox_http_public.py`
- `/Users/ungus/Documents/willy/nautilus_trader/tests/integration_tests/adapters/lighter/sandbox/sandbox_http_private.py`
- `/Users/ungus/Documents/willy/nautilus_trader/tests/integration_tests/adapters/lighter/sandbox/sandbox_ws_public.py`
- `/Users/ungus/Documents/willy/nautilus_trader/tests/integration_tests/adapters/lighter/sandbox/sandbox_ws_private.py`

## Notes

- Private REST and WebSocket auth use a raw auth token string, not a `Bearer` header.
- Funding-rate subscriptions and requests are only valid for perpetual instruments.
- Perpetual metadata can arrive as single-token symbols such as `ASTER` or `EURUSD`; the adapter normalizes those into USDC-settled `CryptoPerpetual` instruments.
- The current implementation is Rust-first for transport/signing and Python-first for Nautilus normalization and orchestration, matching the existing Python `TradingNode` adapter pattern.
- Stable adapters in this repo typically keep live-network smoke coverage under `tests/integration_tests/adapters/<adapter>/sandbox`; the Lighter adapter now follows that pattern as well.
- If you need public WebSocket data from a restricted region, Lighter documents a read-only variant at `.../stream?readonly=true`; the adapter already lets you override `base_url_ws` if you need that endpoint shape.

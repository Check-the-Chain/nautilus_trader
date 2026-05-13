# Lighter Adapter

Rust-first NautilusTrader adapter for the [Lighter](https://lighter.xyz) exchange.

## Components

- `config`: adapter configuration and venue URL helpers
- `http`: low-level HTTP client bindings
- `websocket` / `ws`: low-level WebSocket transport and typed message handling
- `models`: REST and WebSocket payload models
- `data`: native Nautilus live data client
- `execution`: native Nautilus live execution client
- `factories`: Rust live-node factory integration
- `python`: PyO3 bindings used by the Python adapter layer
- `ffi` / `nonce`: transaction signing and nonce coordination

## Scope

- Instrument discovery for Lighter spot and perpetual markets
- Historical and live market data normalization
- Private execution, reconciliation, and account-state updates
- Venue-specific admin helpers such as leverage, margin, transfer, and withdraw flows

## Tests

Run the adapter test suite with:

```bash
cargo test -p nautilus-lighter --features python
```

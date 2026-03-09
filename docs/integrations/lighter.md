# Lighter

[Lighter](https://lighter.xyz) is a crypto exchange with REST and WebSocket APIs for public market data,
private account state, and signed transaction flows. This NautilusTrader integration follows the same
Rust-first pattern as the newer DEX adapters, with thin Python wrappers over the transport and signing layer.

## Overview

The Lighter adapter includes:

- `LighterHttpClient`: Low-level HTTP API connectivity.
- `LighterWebSocketClient`: Low-level WebSocket API connectivity.
- `LighterInstrumentProvider`: Instrument loading and normalization.
- `LighterDataClient`: Live data orchestration for books, quotes, trades, bars, and market stats.
- `LighterExecutionClient`: Private account, reconciliation, and execution gateway.
- `LighterLiveDataClientFactory`: Python live-node data factory.
- `LighterLiveExecClientFactory`: Python live-node execution factory.

The underlying Rust crate also exposes native Nautilus data/execution clients and factory registration for
the Rust-side live-node path.

:::note
Most users will configure Lighter through a `TradingNodeConfig` and the adapter factories rather than using
the low-level clients directly.
:::

## Venue setup

The private Lighter flow requires:

1. An account index.
2. A wallet private key for transaction signing.
3. An API key index when using delegated API keys.
4. Auth token refresh for private REST and WebSocket channels.

The adapter handles auth token creation and refresh automatically once the execution client is configured.

## Product support

Lighter market metadata exposes both `perp` and `spot` market types, and the adapter normalizes both into
Nautilus instruments.

| Product Type      | Data Feed | Trading | Notes                               |
|-------------------|-----------|---------|-------------------------------------|
| Perpetual Futures | ✓         | ✓       | Mark/index/funding data supported.  |
| Spot              | ✓         | ✓       | Normalized as `CurrencyPair`.       |

## Data support

| Data type         | Live | Historical | Nautilus type      | Notes                                          |
|-------------------|------|------------|--------------------|------------------------------------------------|
| Instruments       | ✓    | -          | `Instrument`       | Loaded from Lighter market metadata.           |
| Order book deltas | ✓    | ✓ snapshot | `OrderBookDelta`   | L2 MBP only. Historical path is snapshot-only. |
| Quote ticks       | ✓    | -          | `QuoteTick`        | Derived from the ticker/book stream.           |
| Trade ticks       | ✓    | ✓          | `TradeTick`        | Historical recent-trades endpoint supported.   |
| Bars              | -    | ✓          | `Bar`              | Lighter currently provides historical candles. |
| Mark prices       | ✓    | -          | `MarkPriceUpdate`  | Via shared market-stats subscription.          |
| Index prices      | ✓    | -          | `IndexPriceUpdate` | Via shared market-stats subscription.          |
| Funding rates     | ✓    | ✓          | `FundingRateUpdate`| Perpetual markets only.                        |

## Private execution support

The execution client supports:

- order submission
- order modification
- order cancellation
- cancel-all with per-order fallback
- order status reconciliation
- fill report reconciliation
- position status reconciliation
- account-state refresh from private REST plus private WebSocket channels
- venue helper methods for account metadata, sub-account discovery, account tier changes,
  public-key rotation, API token management, L1 metadata / tx lookup, liquidation history,
  transfer-fee / withdrawal-delay lookup, referral management, public announcements / metrics,
  root/system info helpers, tx-history / export helpers, intent-address and deposit helpers,
  fast-withdraw / lease helpers, public-pool management, notification acknowledgement,
  leverage/margin updates, and sub-account creation

Private WebSocket subscriptions include the documented `account_all`, `account_all_orders`,
`account_all_positions`, `account_all_trades`, `account_all_assets`, and `user_stats` channels.

Current live Lighter perp metadata uses single-token symbols such as `ASTER` or `EURUSD`.
The Nautilus adapter maps those to USDC-settled `CryptoPerpetual` instruments and keeps the raw
venue symbol for request routing.

## Example configuration

```python
from nautilus_trader.adapters.lighter import LighterDataClientConfig
from nautilus_trader.adapters.lighter import LighterExecClientConfig
from nautilus_trader.adapters.lighter import LighterLiveDataClientFactory
from nautilus_trader.adapters.lighter import LighterLiveExecClientFactory
from nautilus_trader.live.config import InstrumentProviderConfig
from nautilus_trader.live.config import TradingNodeConfig


config = TradingNodeConfig(
    data_clients={
        "LIGHTER": LighterLiveDataClientFactory(
            LighterDataClientConfig(testnet=False),
        ),
    },
    exec_clients={
        "LIGHTER": LighterLiveExecClientFactory(
            LighterExecClientConfig(
                account_index=7,
                private_key="0x...",
                api_key_index=0,
                testnet=False,
            ),
        ),
    },
    instrument_providers={
        "LIGHTER": InstrumentProviderConfig(load_all=True),
    },
)
```

## Examples

See:

- `examples/live/lighter/lighter_data_tester.py`
- `examples/live/lighter/lighter_exec_tester.py`
- `tests/integration_tests/adapters/lighter/sandbox/sandbox_http_public.py`
- `tests/integration_tests/adapters/lighter/sandbox/sandbox_http_private.py`
- `tests/integration_tests/adapters/lighter/sandbox/sandbox_ws_public.py`
- `tests/integration_tests/adapters/lighter/sandbox/sandbox_ws_private.py`

The sandbox scripts mirror the pattern used by other mature adapters in this repository:
the main integration suite stays deterministic and mocked, while opt-in live smoke coverage
for real REST / WebSocket connectivity lives under the adapter `sandbox` folder.

For public WebSocket data in restricted regions, Lighter documents a read-only websocket URL
variant using `?readonly=true`. You can pass that directly via `base_url_ws` when needed.

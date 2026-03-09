# Alpaca

[Alpaca](https://alpaca.markets) is a brokerage API covering US equities, listed US equity options,
and crypto spot markets.
This NautilusTrader adapter is a Python-native integration over Alpaca's REST and WebSocket APIs,
focused on the standard live-trading workflow: instrument loading, live market data, historical bars,
single-order execution, and execution-state reconciliation.

## Overview

The Alpaca adapter includes the following core components:

- `AlpacaInstrumentProvider`: loads and normalizes supported stock, option, and crypto instruments.
- `AlpacaDataClient`: manages live quotes, trades, and bars plus historical data requests.
- `AlpacaExecutionClient`: handles account queries, order submission, modification, cancellation, and reconciliation.
- `AlpacaLiveDataClientFactory`: builds live data clients for a trading node.
- `AlpacaLiveExecClientFactory`: builds live execution clients for a trading node.

:::note
The current adapter targets Alpaca's standard workflow for US equities, listed equity options,
and crypto spot.
Some venue-specific features remain out of scope, but the core live-data, historical-data,
order-management, and reconciliation paths are implemented and covered by integration tests.
:::

## Installation

Install the adapter dependencies with the `alpaca` extra:

```bash
uv pip install "nautilus_trader[alpaca]"
```

From source:

```bash
uv sync --extra alpaca
```

## Examples

You can find a minimal live data example [here](https://github.com/nautechsystems/nautilus_trader/tree/develop/examples/live/alpaca/).

## Credentials and environments

The adapter is expected to work with both Alpaca paper and live environments.
Typical credentials are:

- `ALPACA_API_KEY`
- `ALPACA_API_SECRET`

Paper-vs-live selection is controlled through the client config (`paper=True` or `paper=False`).

For market data, the exact symbols and feed permissions available to you depend on the
Alpaca account tier and subscriptions attached to your account.

## Product support

| Product Type | Data Feed | Trading | Notes |
|--------------|-----------|---------|-------|
| US equities  | ✓         | ✓       | Equities are modeled as whole-share instruments; notional orders are supported. |
| US equity options | ✓    | ✓       | Single-leg `OptionContract` instruments; Alpaca order constraints still apply. |
| Crypto spot  | ✓         | ✓       | Normalized as `CurrencyPair` instruments. |

## Current capability

### Market data

| Data type    | Streaming | Historical | Notes |
|--------------|-----------|------------|-------|
| Quote ticks  | ✓         | ✓          | Top-of-book quotes from streaming and REST APIs. |
| Trade ticks  | ✓         | ✓          | Trade streams plus historical trade requests. |
| Bars         | ✓         | ✓          | External time bars only; live bars are not available for options. |
| Instruments  | ✓         | ✓          | Provider-backed equity, option, and crypto instruments. |

### Execution

| Feature                    | Support | Notes |
|---------------------------|---------|-------|
| Account query            | ✓       | Base account state plus open positions. |
| Order submit             | ✓       | Single orders plus supported equities advanced order lists. |
| Order modify             | ✓       | Standard single-order updates. |
| Cancel order             | ✓       | By client or venue order ID. |
| Cancel all               | ✓       | Venue-wide or filtered via Nautilus cache. |
| Reconciliation           | ✓       | Order, fill, and position report generation. |
| Bracket/OCO/OTO lists    | ✓       | Equities only; supports Alpaca `bracket`, `oto`, and `oco` shapes. |
| Options trading          | ✓       | Single-leg options only; `MARKET`/`LIMIT`, `DAY`, whole-contract quantity. |
| Full fractional equities | -       | Deferred pending a Nautilus equity-model mapping decision. |

## Supported order types

The adapter currently covers Alpaca's standard single-order workflow:

| Order Type               | Support | Notes |
|--------------------------|---------|-------|
| `MARKET`                 | ✓       | |
| `LIMIT`                  | ✓       | |
| `STOP_MARKET`            | ✓       | Mapped to Alpaca stop orders. |
| `STOP_LIMIT`             | ✓       | |
| `TRAILING_STOP_MARKET`   | ✓       | Mapped to Alpaca trailing stop orders. |
| `MARKET_IF_TOUCHED`      | -       | Deferred. |
| `LIMIT_IF_TOUCHED`       | -       | Deferred. |
| `TRAILING_STOP_LIMIT`    | -       | Deferred. |

Options use the stricter Alpaca subset: `MARKET` and `LIMIT` only, `DAY` only, no notional
quantity, and no advanced order lists or extended-hours flow.

### Supported time-in-force values

| Nautilus `TimeInForce` | Alpaca |
|------------------------|--------|
| `DAY`                  | `day`  |
| `GTC`                  | `gtc`  |
| `IOC`                  | `ioc`  |
| `FOK`                  | `fok`  |
| `AT_THE_OPEN`          | `opg`  |
| `AT_THE_CLOSE`         | `cls`  |

## Symbology

The adapter is intended to keep Nautilus symbols close to Alpaca's native symbols:

- Equities: `AAPL.ALPACA`, `MSFT.ALPACA`
- Options: `AAPL260320C00150000.ALPACA`
- Crypto: `BTC/USD.ALPACA`, `ETH/USD.ALPACA`

For crypto, Alpaca uses slightly different symbol conventions across some REST and streaming
surfaces. The adapter normalizes these internally and stores the venue-specific forms in
instrument metadata.

## Instrument loading

The provider supports the standard instrument-provider flow through
`AlpacaInstrumentProviderConfig`.
Typical configurations are:

Load specific symbols:

```python
from nautilus_trader.adapters.alpaca import AlpacaInstrumentProviderConfig
from nautilus_trader.model.identifiers import InstrumentId

instrument_provider = AlpacaInstrumentProviderConfig(
    load_ids=frozenset(
        [
            InstrumentId.from_str("AAPL.ALPACA"),
            InstrumentId.from_str("BTC/USD.ALPACA"),
        ],
    ),
)
```

Load all supported Alpaca instruments:

```python
AlpacaInstrumentProviderConfig(load_all=True)
```

Load listed options for specific underlyings:

```python
AlpacaInstrumentProviderConfig(
    asset_classes=frozenset({"option"}),
    option_underlyings=frozenset({"AAPL", "MSFT"}),
)
```

## Trading node setup

```python
from nautilus_trader.adapters.alpaca import ALPACA
from nautilus_trader.adapters.alpaca import AlpacaDataClientConfig
from nautilus_trader.adapters.alpaca import AlpacaExecClientConfig
from nautilus_trader.adapters.alpaca import AlpacaInstrumentProviderConfig
from nautilus_trader.adapters.alpaca import AlpacaLiveDataClientFactory
from nautilus_trader.adapters.alpaca import AlpacaLiveExecClientFactory
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import TraderId

config = TradingNodeConfig(
    trader_id=TraderId("TESTER-001"),
    logging=LoggingConfig(log_level="INFO", use_pyo3=True),
    data_clients={
        ALPACA: AlpacaDataClientConfig(
            api_key="YOUR_API_KEY",
            api_secret="YOUR_API_SECRET",
            paper=True,
            instrument_provider=AlpacaInstrumentProviderConfig(
                load_ids=frozenset(
                    [
                        "AAPL.ALPACA",
                        "BTC/USD.ALPACA",
                    ],
                ),
            ),
        ),
    },
    exec_clients={
        ALPACA: AlpacaExecClientConfig(
            api_key="YOUR_API_KEY",
            api_secret="YOUR_API_SECRET",
            paper=True,
            instrument_provider=AlpacaInstrumentProviderConfig(
                load_ids=frozenset(["AAPL.ALPACA"]),
            ),
        ),
    },
)

node = TradingNode(config=config)
node.add_data_client_factory(ALPACA, AlpacaLiveDataClientFactory)
node.add_exec_client_factory(ALPACA, AlpacaLiveExecClientFactory)
```

## Example script

The repository includes a small market-data tester under `examples/live/alpaca/` which is
useful for validating:

- instrument loading
- live quote and trade subscriptions
- historical bar requests
- paper-vs-live environment configuration

## Current limitations

- Advanced order lists currently cover Alpaca equities `bracket`, `oto`, and `oco`.
- Advanced order lists are denied for unsupported contingent shapes, non-equities, extended-hours flow, or non-`reduce_only` exit legs.
- Options currently cover single-leg equity-option instruments plus quote/trade/bar history and live quote/trade streams.
- Alpaca live option bars are not exposed by this adapter because Alpaca does not stream them.
- Equities are intentionally modeled without promising full fractional-share behavior because the Nautilus `Equity` model remains whole-share oriented.

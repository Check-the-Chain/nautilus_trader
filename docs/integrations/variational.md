# Variational

The Variational adapter provides read-only access to Variational Omni market data.

The public Variational API currently exposes `GET /metadata/stats`, which includes
platform statistics and per-listing stats for perpetual markets. The production
Omni app also uses a public `/prices` websocket for live price updates.
Variational's trading API is not publicly available yet, so this adapter
intentionally does not implement execution.

## Venue

The default venue is `VARIATIONAL`.

Perpetual instruments use the format:

```python
InstrumentId.from_str("BTC-USDC-PERP.VARIATIONAL")
```

The upstream listing ticker is preserved as the instrument raw symbol.

## Supported Data

The adapter can load instruments and stream/poll:

- `MarkPriceUpdate` from the `/prices` websocket.
- `IndexPriceUpdate` from the websocket `underlying_price` field.
- `QuoteTick` from the configured stats quote tier.
- `FundingRateUpdate` from `funding_rate` and `funding_interval_s`.

The public interfaces do not expose order books, trades, or bars. Bid/ask quotes
and funding remain REST-polled because they are not present on the price
websocket.

## Configuration

```python
from nautilus_trader.adapters.variational import VariationalDataClientConfig
from nautilus_trader.adapters.variational import VariationalQuoteTier

config = VariationalDataClientConfig(
    poll_interval_secs=30,
    quote_tier=VariationalQuoteTier.BASE,
    default_size_precision=8,
    ws_price_funding_interval_secs=3600,
)
```

`default_size_precision` is required because the public stats endpoint does not
publish quantity increments. Quote sizes are unknown for the `BASE` tier and are
published as zero size; notional quote tiers estimate size from `notional / price`.

The websocket subscription payload requires a one-hour `funding_interval_s` in
the instrument object (`P-BTC-USDC-3600` channels), which is separate from the
eight-hour funding interval reported by the public stats endpoint.

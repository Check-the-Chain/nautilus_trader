# NautilusTrader Variational Adapter

Read-only Variational Omni adapter.

The public Variational API currently exposes market metadata, quotes, mark prices,
open interest, and funding rates through `GET /metadata/stats`. The adapter uses
Variational Omni's `/prices` websocket for live mark and index price updates.
The trading API is not public yet, so this crate intentionally does not implement
execution.

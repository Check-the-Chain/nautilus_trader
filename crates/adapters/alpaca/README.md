# nautilus-alpaca

[![build](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/nautechsystems/nautilus_trader/actions/workflows/build.yml)
[![Documentation](https://img.shields.io/docsrs/nautilus-alpaca)](https://docs.rs/nautilus-alpaca/latest/nautilus-alpaca/)
[![crates.io version](https://img.shields.io/crates/v/nautilus-alpaca.svg)](https://crates.io/crates/nautilus-alpaca)
![license](https://img.shields.io/github/license/nautechsystems/nautilus_trader?color=blue)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?logo=discord&logoColor=white)](https://discord.gg/NautilusTrader)

[NautilusTrader](https://nautilustrader.io) adapter for [Alpaca Markets](https://alpaca.markets/).

The `nautilus-alpaca` crate provides client bindings (HTTP & WebSocket), data models,
and helper utilities that wrap the official **Alpaca Trading and Market Data APIs**,
with a focus on US spot equities.

The official Alpaca API reference can be found at <https://docs.alpaca.markets/>.

## NautilusTrader

[NautilusTrader](https://nautilustrader.io) is an open-source, production-grade, Rust-native
engine for multi-asset, multi-venue trading systems.

The system spans research, deterministic simulation, and live execution within a single
event-driven architecture, providing research-to-live semantic parity.

## Features

- HTTP REST client for the Trading API (account, assets, orders, positions).
- HTTP REST client for historical market data (bars, trades, quotes).
- WebSocket client for real-time equities market data (trades, quotes, bars) over the IEX or SIP feeds.
- WebSocket client for account trade updates (order lifecycle events).
- Instrument provider building Nautilus `Equity` instruments from Alpaca assets.
- Paper trading support via the paper API environment.

## API environments

| Component          | Live                                     | Paper                                    |
|--------------------|------------------------------------------|------------------------------------------|
| Trading API        | `https://api.alpaca.markets`             | `https://paper-api.alpaca.markets`       |
| Market Data API    | `https://data.alpaca.markets`            | (same)                                   |
| Market Data stream | `wss://stream.data.alpaca.markets/v2/*`  | (same, `test` feed available)            |
| Trade updates      | `wss://api.alpaca.markets/stream`        | `wss://paper-api.alpaca.markets/stream`  |

## License

The source code for NautilusTrader is available on GitHub under the [GNU Lesser General Public License v3.0](https://www.gnu.org/licenses/lgpl-3.0.en.html).
Contributions to the project are welcome and require the completion of a standard [Contributor License Agreement (CLA)](https://github.com/nautechsystems/nautilus_trader/blob/develop/CLA.md).

---

NautilusTrader™ is developed and maintained by Nautech Systems, a technology
company specializing in the development of high-performance trading systems.
For more information, visit <https://nautilustrader.io>.

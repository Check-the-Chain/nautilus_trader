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

//! WebSocket clients for Alpaca market data and trade-updates streams.
//!
//! Two streams are covered:
//!
//! - The equities market data stream (`wss://stream.data.alpaca.markets/v2/{feed}`)
//!   carrying trades, quotes, and bars as `"T"`-tagged arrays, framed as
//!   MessagePack (default) or JSON per [`WsFormat`].
//! - The account trade-updates stream (`wss://{paper-}api.alpaca.markets/stream`)
//!   carrying order lifecycle events in `{"stream","data"}` envelopes.
//!
//! Both clients follow the feed-handler pattern: the handler task owns the
//! underlying transport, parses each frame in a single typed pass, and emits
//! finished [`NautilusWsMessage`] values over an unbounded channel.

pub mod client;
pub mod error;
pub mod handler;
pub mod messages;
pub mod parse;

pub use client::{AlpacaTradeUpdatesWebSocketClient, AlpacaWebSocketClient};
pub use error::AlpacaWsError;
pub use messages::{
    AlpacaInstrumentInfo, AlpacaSubscriptionAck, AlpacaTradeUpdateMsg, AlpacaWsChannel,
    AlpacaWsOrder, NautilusWsMessage, WsFormat,
};

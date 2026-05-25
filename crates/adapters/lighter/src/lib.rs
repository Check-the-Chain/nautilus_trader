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

//! NautilusTrader adapter primitives for the Lighter exchange.

#![warn(rustc::all)]
#![deny(nonstandard_style)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::too_many_arguments)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod client;
pub mod common;
pub mod config;
pub mod constants;
pub mod data;
pub mod error;
pub mod execution;
pub mod factories;
pub mod ffi;
pub mod http;
pub mod models;
pub mod nonce;
pub mod normalize;
#[cfg(feature = "python")]
pub mod python;
pub mod rest;
pub mod types;
pub mod websocket;

pub use crate::{
    config::{
        Config, LIGHTER_MAINNET_HOST, LIGHTER_TESTNET_HOST, LighterDataClientConfig,
        LighterExecClientConfig, lighter_http_base_url, lighter_ws_base_url,
    },
    data::LighterDataClient,
    execution::LighterExecutionClient,
    factories::{
        LighterDataClientFactory, LighterExecFactoryConfig, LighterExecutionClientFactory,
    },
    http::client::LighterHttpClient,
    websocket::client::LighterWebSocketClient,
};

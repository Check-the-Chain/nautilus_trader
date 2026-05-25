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

//! NautilusTrader adapter primitives for Variational Omni.

#![warn(rustc::all)]
#![deny(nonstandard_style)]
#![allow(clippy::missing_errors_doc)]
#![allow(clippy::too_many_arguments)]
#![deny(rustdoc::broken_intra_doc_links)]

pub mod common;
pub mod config;
pub mod data;
pub mod error;
pub mod factories;
pub mod http;
pub mod models;
#[cfg(feature = "python")]
pub mod python;
pub mod websocket;

pub use crate::{
    config::{
        VARIATIONAL_OMNI_HTTP_BASE_URL, VARIATIONAL_OMNI_WS_BASE_URL,
        VARIATIONAL_WS_PRICE_FUNDING_INTERVAL_SECS, VariationalDataClientConfig,
        VariationalQuoteTier, variational_http_base_url, variational_ws_base_url,
    },
    data::VariationalDataClient,
    factories::VariationalDataClientFactory,
    http::client::VariationalHttpClient,
};

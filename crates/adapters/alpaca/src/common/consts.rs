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

//! Venue identifiers and tuning constants for the Alpaca adapter.

use std::{sync::LazyLock, time::Duration};

use nautilus_model::identifiers::Venue;
use ustr::Ustr;

/// Venue name string for Alpaca.
pub const ALPACA: &str = "ALPACA";

/// Alpaca venue identifier.
pub static ALPACA_VENUE: LazyLock<Venue> = LazyLock::new(|| Venue::new(Ustr::from(ALPACA)));

/// HTTP header carrying the Alpaca API key ID.
pub const ALPACA_API_KEY_HEADER: &str = "APCA-API-KEY-ID";

/// HTTP header carrying the Alpaca API secret key.
pub const ALPACA_API_SECRET_HEADER: &str = "APCA-API-SECRET-KEY";

/// Default WebSocket heartbeat interval.
///
/// Alpaca disconnects silent connections; we ping well below the venue timeout.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// Reconnect timeout for the WebSocket client.
pub const RECONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Base reconnect backoff for the WebSocket client.
pub const RECONNECT_DELAY_INITIAL: Duration = Duration::from_secs(1);

/// Maximum reconnect backoff for the WebSocket client.
pub const RECONNECT_DELAY_MAX: Duration = Duration::from_secs(30);

/// Reconnect backoff multiplier for the WebSocket client.
pub const RECONNECT_BACKOFF_FACTOR: f64 = 2.0;

/// Reconnect jitter for the WebSocket client.
pub const RECONNECT_JITTER: Duration = Duration::from_millis(250);

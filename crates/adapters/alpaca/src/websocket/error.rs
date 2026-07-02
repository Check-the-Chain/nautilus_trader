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

//! WebSocket error taxonomy.

use nautilus_network::error::SendError;
use thiserror::Error;

/// Errors emitted by the Alpaca WebSocket clients.
#[derive(Debug, Error)]
pub enum AlpacaWsError {
    /// Generic client error.
    #[error("client error: {0}")]
    Client(String),
    /// Send-side transport failure. Carries the structured [`SendError`]
    /// so retry classifiers can match on the variant rather than the
    /// formatted message.
    #[error("transport error: {0}")]
    Transport(#[from] SendError),
    /// Failed to parse a wire frame.
    #[error("parse error: {0}")]
    Parse(String),
    /// Authentication failure.
    #[error("authentication error: {0}")]
    Authentication(String),
    /// Venue error frame.
    ///
    /// Documented codes: 400 invalid syntax, 401 not authenticated,
    /// 402 auth failed, 403 already authenticated, 404 auth timeout,
    /// 405 symbol limit exceeded, 406 connection limit exceeded,
    /// 407 slow client, 409 insufficient subscription, 410 invalid
    /// subscribe action, 500 internal error.
    #[error("venue error {code:?}: {message}")]
    Venue { code: Option<u16>, message: String },
    /// Operation timed out.
    #[error("timeout: {0}")]
    Timeout(String),
}

impl From<String> for AlpacaWsError {
    fn from(value: String) -> Self {
        Self::Client(value)
    }
}

/// Returns `true` when the error is transient and a retry may succeed.
pub(crate) fn should_retry_alpaca_ws_error(error: &AlpacaWsError) -> bool {
    match error {
        // Closed and BrokenPipe are terminal on this client; only Timeout
        // (wait_for_active) can recover if the connection comes up.
        AlpacaWsError::Transport(send_error) => match send_error {
            SendError::Timeout => true,
            SendError::Closed | SendError::BrokenPipe(_) => false,
        },
        AlpacaWsError::Timeout(_) => true,
        AlpacaWsError::Client(_)
        | AlpacaWsError::Parse(_)
        | AlpacaWsError::Authentication(_)
        | AlpacaWsError::Venue { .. } => false,
    }
}

/// Builds the structured timeout error the retry manager feeds back into the
/// classifier above.
pub(crate) fn create_alpaca_ws_timeout_error(_msg: String) -> AlpacaWsError {
    // Structured variant so the classifier retries; the retry manager
    // already logs the textual timeout context.
    AlpacaWsError::Transport(SendError::Timeout)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_should_retry_classification() {
        assert!(should_retry_alpaca_ws_error(&AlpacaWsError::Transport(
            SendError::Timeout
        )));
        assert!(should_retry_alpaca_ws_error(&AlpacaWsError::Timeout(
            "t".to_string()
        )));
        assert!(!should_retry_alpaca_ws_error(&AlpacaWsError::Transport(
            SendError::Closed
        )));
        assert!(!should_retry_alpaca_ws_error(&AlpacaWsError::Venue {
            code: Some(406),
            message: "connection limit exceeded".to_string(),
        }));
    }

    #[rstest]
    fn test_from_string() {
        let error = AlpacaWsError::from("boom".to_string());
        assert!(matches!(error, AlpacaWsError::Client(msg) if msg == "boom"));
    }
}

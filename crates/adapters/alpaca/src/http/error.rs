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

//! HTTP error taxonomy for Alpaca REST responses.
//!
//! Alpaca error bodies carry a `{"code": 42210000, "message": "..."}` shape;
//! rate limiting surfaces as HTTP 429 (see
//! <https://docs.alpaca.markets/us/docs/about-market-data-api>).

use nautilus_network::http::HttpClientError;
use serde::Deserialize;
use thiserror::Error;

/// Result alias for Alpaca HTTP operations.
pub type AlpacaHttpResult<T> = Result<T, AlpacaHttpError>;

/// Represents the JSON structure of an error response returned by the Alpaca API.
#[derive(Clone, Debug, Deserialize)]
pub struct AlpacaErrorBody {
    /// Venue-specific error code (e.g. `42210000` for sub-penny increments).
    pub code: Option<i64>,
    /// A human-readable explanation of the error condition.
    pub message: String,
}

/// Errors emitted by the Alpaca HTTP client.
#[derive(Debug, Clone, Error)]
pub enum AlpacaHttpError {
    /// Credentials are missing for an authenticated request.
    #[error("missing credentials for authenticated request")]
    MissingCredentials,
    /// Network-level failure (transport, DNS, TLS).
    #[error("network error: {0}")]
    Network(String),
    /// HTTP-level failure with status code and body.
    #[error("HTTP {status}: {body}")]
    Http { status: u16, body: String },
    /// Rate limit exceeded (HTTP 429).
    #[error("rate limit exceeded: {0}")]
    RateLimit(String),
    /// Venue returned a structured error code.
    #[error("venue error {code}: {message}")]
    Venue { code: i64, message: String },
    /// Failed to parse a venue response.
    #[error("parse error: {0}")]
    Parse(String),
    /// Parameter validation error.
    #[error("validation error: {0}")]
    Validation(String),
}

impl From<HttpClientError> for AlpacaHttpError {
    fn from(error: HttpClientError) -> Self {
        Self::Network(error.to_string())
    }
}

impl From<serde_json::Error> for AlpacaHttpError {
    fn from(error: serde_json::Error) -> Self {
        Self::Parse(error.to_string())
    }
}

impl From<anyhow::Error> for AlpacaHttpError {
    fn from(error: anyhow::Error) -> Self {
        Self::Parse(error.to_string())
    }
}

/// Returns `true` if a request producing this error should be retried.
///
/// Retryable shapes are transport-layer failures, server-side 5xx, and rate
/// limits. Venue-semantic errors (4xx other than 429, `Venue`, `Parse`,
/// `Validation`, `MissingCredentials`) are surfaced unchanged.
#[must_use]
pub fn should_retry_alpaca_http_error(error: &AlpacaHttpError) -> bool {
    match error {
        AlpacaHttpError::Network(_) | AlpacaHttpError::RateLimit(_) => true,
        AlpacaHttpError::Http { status, .. } => *status >= 500,
        AlpacaHttpError::MissingCredentials
        | AlpacaHttpError::Venue { .. }
        | AlpacaHttpError::Parse(_)
        | AlpacaHttpError::Validation(_) => false,
    }
}

/// Constructs a transport-shaped error for retry-manager timeout / cancellation paths.
#[must_use]
pub fn create_alpaca_http_timeout_error(msg: String) -> AlpacaHttpError {
    AlpacaHttpError::Network(msg)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::network_retries(AlpacaHttpError::Network("dns failure".into()), true)]
    #[case::rate_limit_retries(AlpacaHttpError::RateLimit("429".into()), true)]
    #[case::server_5xx_retries(AlpacaHttpError::Http { status: 503, body: "busy".into() }, true)]
    #[case::server_500_retries(AlpacaHttpError::Http { status: 500, body: "boom".into() }, true)]
    #[case::client_400_does_not_retry(AlpacaHttpError::Http { status: 400, body: "bad".into() }, false)]
    #[case::client_422_does_not_retry(AlpacaHttpError::Http { status: 422, body: "uncancelable".into() }, false)]
    #[case::venue_does_not_retry(AlpacaHttpError::Venue { code: 42210000, message: "sub-penny".into() }, false)]
    #[case::parse_does_not_retry(AlpacaHttpError::Parse("bad json".into()), false)]
    #[case::missing_credentials_does_not_retry(AlpacaHttpError::MissingCredentials, false)]
    fn test_should_retry_alpaca_http_error(#[case] error: AlpacaHttpError, #[case] expected: bool) {
        assert_eq!(should_retry_alpaca_http_error(&error), expected);
    }

    #[rstest]
    fn test_error_body_deserialization() {
        let body: AlpacaErrorBody =
            serde_json::from_str(r#"{"code":42210000,"message":"invalid limit_price"}"#).unwrap();

        assert_eq!(body.code, Some(42_210_000));
        assert_eq!(body.message, "invalid limit_price");
    }

    #[rstest]
    fn test_error_body_without_code() {
        let body: AlpacaErrorBody = serde_json::from_str(r#"{"message":"forbidden"}"#).unwrap();

        assert_eq!(body.code, None);
        assert_eq!(body.message, "forbidden");
    }
}

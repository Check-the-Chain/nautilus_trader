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

//! Alpaca API credential storage and resolution.
//!
//! Alpaca authenticates HTTP requests with the `APCA-API-KEY-ID` and
//! `APCA-API-SECRET-KEY` headers, and WebSocket connections with an auth
//! message carrying the same key pair. No request signing is involved.

use std::{fmt::Debug, str};

use anyhow::Context;
use nautilus_core::{env::get_or_env_var_opt, string::secret::REDACTED};
use zeroize::ZeroizeOnDrop;

use crate::common::enums::AlpacaEnvironment;

const ALPACA_API_KEY_VAR: &str = "ALPACA_API_KEY";
const ALPACA_API_SECRET_VAR: &str = "ALPACA_API_SECRET";
const ALPACA_PAPER_API_KEY_VAR: &str = "ALPACA_PAPER_API_KEY";
const ALPACA_PAPER_API_SECRET_VAR: &str = "ALPACA_PAPER_API_SECRET";

/// Environment variable names for Alpaca credentials.
///
/// Returns `(api_key_var, api_secret_var)`. Paper trading uses a separate key
/// pair issued for the paper account.
#[must_use]
pub const fn credential_env_vars(environment: AlpacaEnvironment) -> (&'static str, &'static str) {
    match environment {
        AlpacaEnvironment::Live => (ALPACA_API_KEY_VAR, ALPACA_API_SECRET_VAR),
        AlpacaEnvironment::Paper => (ALPACA_PAPER_API_KEY_VAR, ALPACA_PAPER_API_SECRET_VAR),
    }
}

/// Alpaca API credentials for authenticated REST and WebSocket connections.
///
/// Secrets are automatically zeroized on drop for security.
#[derive(Clone, ZeroizeOnDrop)]
pub struct Credential {
    api_key: Box<str>,
    api_secret: Box<[u8]>,
}

impl Debug for Credential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(Credential))
            .field("api_key", &self.api_key)
            .field("api_secret", &REDACTED)
            .finish()
    }
}

impl Credential {
    /// Creates a new [`Credential`] instance.
    #[must_use]
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            api_key: api_key.into_boxed_str(),
            api_secret: api_secret.into_bytes().into_boxed_slice(),
        }
    }

    /// Resolves credentials from provided config values or environment variables.
    ///
    /// Config values take precedence, but blank or whitespace-only values fall
    /// back to the environment variables for the given `environment` (see
    /// [`credential_env_vars`]).
    ///
    /// # Errors
    ///
    /// Returns an error if only one of the key pair resolves.
    pub fn resolve(
        api_key: Option<String>,
        api_secret: Option<String>,
        environment: AlpacaEnvironment,
    ) -> anyhow::Result<Option<Self>> {
        let (key_var, secret_var) = credential_env_vars(environment);

        let key = get_or_env_var_opt(api_key.filter(|s| !s.trim().is_empty()), key_var)
            .filter(|s| !s.trim().is_empty());
        let secret = get_or_env_var_opt(api_secret.filter(|s| !s.trim().is_empty()), secret_var)
            .filter(|s| !s.trim().is_empty());

        match (key, secret) {
            (Some(key), Some(secret)) => Ok(Some(Self::new(key, secret))),
            (None, None) => Ok(None),
            _ => anyhow::bail!("incomplete Alpaca credentials: set {key_var} and {secret_var}"),
        }
    }

    /// Returns the API key ID.
    #[must_use]
    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    /// Returns the API secret key.
    ///
    /// # Errors
    ///
    /// Returns an error if the stored secret is not valid UTF-8.
    pub fn api_secret(&self) -> anyhow::Result<&str> {
        str::from_utf8(&self.api_secret).context("Alpaca API secret must be UTF-8")
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_credential_env_vars_live() {
        assert_eq!(
            credential_env_vars(AlpacaEnvironment::Live),
            ("ALPACA_API_KEY", "ALPACA_API_SECRET"),
        );
    }

    #[rstest]
    fn test_credential_env_vars_paper() {
        assert_eq!(
            credential_env_vars(AlpacaEnvironment::Paper),
            ("ALPACA_PAPER_API_KEY", "ALPACA_PAPER_API_SECRET"),
        );
    }

    #[rstest]
    fn test_resolve_with_config_values() {
        let credential = Credential::resolve(
            Some("key-id".to_string()),
            Some("secret-key".to_string()),
            AlpacaEnvironment::Paper,
        )
        .unwrap()
        .unwrap();

        assert_eq!(credential.api_key(), "key-id");
        assert_eq!(credential.api_secret().unwrap(), "secret-key");
    }

    #[rstest]
    fn test_debug_redacts_api_secret() {
        let credential = Credential::new("key-id".to_string(), "secret-key".to_string());

        let dbg_out = format!("{credential:?}");

        assert!(dbg_out.contains(REDACTED));
        assert!(!dbg_out.contains("secret-key"));
    }
}

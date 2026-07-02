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

//! Raw and domain HTTP clients for Alpaca REST endpoints.
//!
//! The raw client maps directly to venue endpoints and returns wire models;
//! the domain client converts selected responses into Nautilus types and
//! caches instruments. Trading endpoints target the (paper) trading host and
//! market data endpoints target the shared data host.

use std::{
    collections::HashMap,
    fmt::Debug,
    num::NonZeroU32,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
};

use nautilus_core::{
    AtomicMap, AtomicTime, UnixNanos, consts::NAUTILUS_USER_AGENT, time::get_atomic_clock_realtime,
};
use nautilus_model::{
    identifiers::InstrumentId,
    instruments::{Instrument, InstrumentAny},
};
use nautilus_network::{
    http::{HttpClient, HttpResponse, Method, USER_AGENT},
    ratelimiter::quota::Quota,
    retry::{RetryManager, create_http_retry_manager},
};
use serde::{Serialize, de::DeserializeOwned};
use ustr::Ustr;

use crate::{
    common::{
        consts::{ALPACA_API_KEY_HEADER, ALPACA_API_SECRET_HEADER},
        credential::Credential,
        enums::{AlpacaAssetClass, AlpacaAssetStatus, AlpacaEnvironment},
        urls::{alpaca_data_http_url, alpaca_trading_http_url},
    },
    http::{
        error::{
            AlpacaErrorBody, AlpacaHttpError, AlpacaHttpResult, create_alpaca_http_timeout_error,
            should_retry_alpaca_http_error,
        },
        models::{
            AlpacaAccount, AlpacaAccountActivity, AlpacaAsset, AlpacaBar, AlpacaBarsResponse,
            AlpacaCancelOrderStatus, AlpacaHistoricalQuote, AlpacaHistoricalTrade, AlpacaOrder,
            AlpacaPosition, AlpacaQuotesResponse, AlpacaTradesResponse,
        },
        parse::parse_equity_instrument,
        query::{
            GetAccountActivitiesParams, GetAssetsParams, GetAssetsParamsBuilder, GetOrdersParams,
            GetStockBarsParams, GetStockQuotesParams, GetStockTradesParams, PatchOrderParams,
            PostOrderParams,
        },
    },
};

const ENDPOINT_ACCOUNT: &str = "/v2/account";
const ENDPOINT_ACCOUNT_ACTIVITIES: &str = "/v2/account/activities";
const ENDPOINT_ASSETS: &str = "/v2/assets";
const ENDPOINT_ORDERS: &str = "/v2/orders";
const ENDPOINT_POSITIONS: &str = "/v2/positions";
const ENDPOINT_STOCK_BARS: &str = "/v2/stocks/bars";
const ENDPOINT_STOCK_QUOTES: &str = "/v2/stocks/quotes";
const ENDPOINT_STOCK_TRADES: &str = "/v2/stocks/trades";

/// Shared rate-limit bucket key for all Alpaca REST requests.
pub const ALPACA_REST_BUCKET: &str = "alpaca:rest";

/// Default REST quota: 200 requests/min (Trading API and Basic market data tier).
///
/// See <https://docs.alpaca.markets/us/docs/about-market-data-api>.
pub const ALPACA_REST_QUOTA_PER_MIN: u32 = 200;

static ALPACA_REST_QUOTA: LazyLock<Quota> = LazyLock::new(|| {
    Quota::per_minute(
        NonZeroU32::new(ALPACA_REST_QUOTA_PER_MIN).expect("quota constant is non-zero"),
    )
});

/// Maximum pages followed by paginated market data helpers.
const MAX_PAGINATION_PAGES: usize = 50;

/// Raw HTTP client for Alpaca REST API operations.
///
/// This client owns the transport, base URLs, default headers (including
/// credentials), retry manager, and rate limiter. Methods map directly to
/// venue endpoints and return wire models without converting to Nautilus
/// domain types.
#[derive(Clone)]
pub struct AlpacaRawHttpClient {
    base_url_trading: String,
    base_url_data: String,
    environment: AlpacaEnvironment,
    client: HttpClient,
    credential: Option<Credential>,
    retry_manager: RetryManager<AlpacaHttpError>,
}

impl Debug for AlpacaRawHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaRawHttpClient))
            .field("base_url_trading", &self.base_url_trading)
            .field("base_url_data", &self.base_url_data)
            .field("environment", &self.environment)
            .field("has_credential", &self.credential.is_some())
            .finish()
    }
}

impl AlpacaRawHttpClient {
    /// Creates a new [`AlpacaRawHttpClient`].
    ///
    /// Credentials, when provided, are attached to every request via the
    /// `APCA-API-KEY-ID` / `APCA-API-SECRET-KEY` headers.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying HTTP client cannot be created or the
    /// credential secret is not valid UTF-8.
    pub fn new(
        environment: AlpacaEnvironment,
        base_url_trading: Option<String>,
        base_url_data: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
        credential: Option<Credential>,
    ) -> AlpacaHttpResult<Self> {
        let base_url_trading = base_url_trading
            .unwrap_or_else(|| alpaca_trading_http_url(environment).to_string())
            .trim_end_matches('/')
            .to_string();
        let base_url_data = base_url_data
            .unwrap_or_else(|| alpaca_data_http_url().to_string())
            .trim_end_matches('/')
            .to_string();

        let mut headers =
            HashMap::from([(USER_AGENT.to_string(), NAUTILUS_USER_AGENT.to_string())]);
        if let Some(credential) = &credential {
            let secret = credential
                .api_secret()
                .map_err(|e| AlpacaHttpError::Validation(e.to_string()))?
                .to_string();
            headers.insert(
                ALPACA_API_KEY_HEADER.to_string(),
                credential.api_key().to_string(),
            );
            headers.insert(ALPACA_API_SECRET_HEADER.to_string(), secret);
        }

        Ok(Self {
            base_url_trading,
            base_url_data,
            environment,
            client: HttpClient::new(
                headers,
                vec![],
                vec![],
                Some(*ALPACA_REST_QUOTA),
                Some(timeout_secs),
                proxy_url,
            )?,
            credential,
            retry_manager: create_http_retry_manager(),
        })
    }

    /// Returns the configured Trading API base URL.
    #[must_use]
    pub fn base_url_trading(&self) -> &str {
        self.base_url_trading.as_str()
    }

    /// Returns the configured Market Data API base URL.
    #[must_use]
    pub fn base_url_data(&self) -> &str {
        self.base_url_data.as_str()
    }

    /// Returns the configured Alpaca environment.
    #[must_use]
    pub const fn environment(&self) -> AlpacaEnvironment {
        self.environment
    }

    /// Returns `true` when the client carries credentials.
    #[must_use]
    pub const fn has_credentials(&self) -> bool {
        self.credential.is_some()
    }

    /// Overrides the Trading API base URL. Intended for mock-server tests.
    pub fn set_base_url_trading(&mut self, base_url: &str) {
        self.base_url_trading = base_url.trim_end_matches('/').to_string();
    }

    /// Overrides the Market Data API base URL. Intended for mock-server tests.
    pub fn set_base_url_data(&mut self, base_url: &str) {
        self.base_url_data = base_url.trim_end_matches('/').to_string();
    }

    /// Overrides the retry manager. Intended for mock-server tests that need
    /// shorter backoff than [`create_http_retry_manager`] produces.
    pub fn set_retry_manager(&mut self, retry_manager: RetryManager<AlpacaHttpError>) {
        self.retry_manager = retry_manager;
    }

    fn require_credentials(&self) -> AlpacaHttpResult<()> {
        if self.credential.is_none() {
            return Err(AlpacaHttpError::MissingCredentials);
        }
        Ok(())
    }

    /// Calls `GET /v2/account`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_account(&self) -> AlpacaHttpResult<AlpacaAccount> {
        self.require_credentials()?;
        self.send_get(&self.base_url_trading, ENDPOINT_ACCOUNT, None::<&()>)
            .await
    }

    /// Calls `GET /v2/assets`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_assets(&self, params: &GetAssetsParams) -> AlpacaHttpResult<Vec<AlpacaAsset>> {
        self.require_credentials()?;
        self.send_get(&self.base_url_trading, ENDPOINT_ASSETS, Some(params))
            .await
    }

    /// Calls `GET /v2/assets/{symbol_or_asset_id}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_asset(&self, symbol: &str) -> AlpacaHttpResult<AlpacaAsset> {
        self.require_credentials()?;
        let endpoint = format!("{ENDPOINT_ASSETS}/{symbol}");
        self.send_get(&self.base_url_trading, &endpoint, None::<&()>)
            .await
    }

    /// Calls `POST /v2/orders`.
    ///
    /// Not retried: a transport-level retry could double-submit an order whose
    /// original request landed but whose acknowledgement was lost.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the venue rejects the order.
    pub async fn submit_order(&self, params: &PostOrderParams) -> AlpacaHttpResult<AlpacaOrder> {
        self.require_credentials()?;
        let url = format!("{}{ENDPOINT_ORDERS}", self.base_url_trading);
        let body = serde_json::to_vec(params)?;
        let response = self
            .client
            .request(
                Method::POST,
                url,
                None,
                Some(json_headers()),
                Some(body),
                None,
                Some(Self::rate_limit_keys(ENDPOINT_ORDERS)),
            )
            .await?;
        Self::parse_response(&response)
    }

    /// Calls `GET /v2/orders`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_orders(&self, params: &GetOrdersParams) -> AlpacaHttpResult<Vec<AlpacaOrder>> {
        self.require_credentials()?;
        self.send_get(&self.base_url_trading, ENDPOINT_ORDERS, Some(params))
            .await
    }

    /// Calls `GET /v2/orders/{order_id}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_order(&self, order_id: &str) -> AlpacaHttpResult<AlpacaOrder> {
        self.require_credentials()?;
        let endpoint = format!("{ENDPOINT_ORDERS}/{order_id}");
        self.send_get(&self.base_url_trading, &endpoint, None::<&()>)
            .await
    }

    /// Calls `DELETE /v2/orders/{order_id}` (HTTP 204 on acceptance).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the order is not cancelable
    /// (HTTP 422).
    pub async fn cancel_order(&self, order_id: &str) -> AlpacaHttpResult<()> {
        self.require_credentials()?;
        let url = format!("{}{ENDPOINT_ORDERS}/{order_id}", self.base_url_trading);
        let response = self
            .client
            .request(
                Method::DELETE,
                url,
                None,
                None,
                None,
                None,
                Some(Self::rate_limit_keys(ENDPOINT_ORDERS)),
            )
            .await?;
        Self::expect_success(&response)
    }

    /// Calls `DELETE /v2/orders` (cancel all; HTTP 207 multi-status).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn cancel_all_orders(&self) -> AlpacaHttpResult<Vec<AlpacaCancelOrderStatus>> {
        self.require_credentials()?;
        let url = format!("{}{ENDPOINT_ORDERS}", self.base_url_trading);
        let response = self
            .client
            .request(
                Method::DELETE,
                url,
                None,
                None,
                None,
                None,
                Some(Self::rate_limit_keys(ENDPOINT_ORDERS)),
            )
            .await?;
        Self::parse_response(&response)
    }

    /// Calls `PATCH /v2/orders/{order_id}` (returns the replacement order).
    ///
    /// Not retried for the same reason as [`Self::submit_order`].
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the venue rejects the replace.
    pub async fn patch_order(
        &self,
        order_id: &str,
        params: &PatchOrderParams,
    ) -> AlpacaHttpResult<AlpacaOrder> {
        self.require_credentials()?;
        let url = format!("{}{ENDPOINT_ORDERS}/{order_id}", self.base_url_trading);
        let body = serde_json::to_vec(params)?;
        let response = self
            .client
            .request(
                Method::PATCH,
                url,
                None,
                Some(json_headers()),
                Some(body),
                None,
                Some(Self::rate_limit_keys(ENDPOINT_ORDERS)),
            )
            .await?;
        Self::parse_response(&response)
    }

    /// Calls `GET /v2/positions`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_positions(&self) -> AlpacaHttpResult<Vec<AlpacaPosition>> {
        self.require_credentials()?;
        self.send_get(&self.base_url_trading, ENDPOINT_POSITIONS, None::<&()>)
            .await
    }

    /// Calls `GET /v2/account/activities`.
    ///
    /// Trade activities (`FILL`) carry executions; non-trade activities carry
    /// monetary postings, including the `FEE` entries where Alpaca books
    /// sell-side regulatory pass-throughs (SEC fee and FINRA TAF).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_account_activities(
        &self,
        params: &GetAccountActivitiesParams,
    ) -> AlpacaHttpResult<Vec<AlpacaAccountActivity>> {
        self.require_credentials()?;
        self.send_get(&self.base_url_trading, ENDPOINT_ACCOUNT_ACTIVITIES, Some(params))
            .await
    }

    /// Calls `GET /v2/stocks/bars`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_stock_bars(
        &self,
        params: &GetStockBarsParams,
    ) -> AlpacaHttpResult<AlpacaBarsResponse> {
        self.send_get(&self.base_url_data, ENDPOINT_STOCK_BARS, Some(params))
            .await
    }

    /// Calls `GET /v2/stocks/trades`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_stock_trades(
        &self,
        params: &GetStockTradesParams,
    ) -> AlpacaHttpResult<AlpacaTradesResponse> {
        self.send_get(&self.base_url_data, ENDPOINT_STOCK_TRADES, Some(params))
            .await
    }

    /// Calls `GET /v2/stocks/quotes`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_stock_quotes(
        &self,
        params: &GetStockQuotesParams,
    ) -> AlpacaHttpResult<AlpacaQuotesResponse> {
        self.send_get(&self.base_url_data, ENDPOINT_STOCK_QUOTES, Some(params))
            .await
    }

    async fn send_get<T, P>(
        &self,
        base_url: &str,
        endpoint: &str,
        params: Option<&P>,
    ) -> AlpacaHttpResult<T>
    where
        T: DeserializeOwned,
        P: Serialize,
    {
        let url = format!("{base_url}{endpoint}");
        let rate_limit_keys = Self::rate_limit_keys(endpoint);
        self.retry_manager
            .execute_with_retry(
                endpoint,
                || {
                    let url = url.clone();
                    let rate_limit_keys = rate_limit_keys.clone();

                    async move {
                        let response = self
                            .client
                            .request_with_params(
                                Method::GET,
                                url,
                                params,
                                None,
                                None,
                                None,
                                Some(rate_limit_keys),
                            )
                            .await?;
                        Self::parse_response(&response)
                    }
                },
                should_retry_alpaca_http_error,
                create_alpaca_http_timeout_error,
            )
            .await
    }

    fn parse_response<T>(response: &HttpResponse) -> AlpacaHttpResult<T>
    where
        T: DeserializeOwned,
    {
        Self::check_status(response)?;
        let payload: T = serde_json::from_slice(&response.body)?;
        Ok(payload)
    }

    fn expect_success(response: &HttpResponse) -> AlpacaHttpResult<()> {
        Self::check_status(response)
    }

    fn check_status(response: &HttpResponse) -> AlpacaHttpResult<()> {
        if response.status.is_success() {
            return Ok(());
        }

        let status = response.status.as_u16();
        let body = String::from_utf8_lossy(&response.body).to_string();

        // Status-first: a `{code,message}` body must not override the retry
        // decision for 5xx / 429.
        if status >= 500 {
            return Err(AlpacaHttpError::Http { status, body });
        }

        if status == 429 {
            return Err(AlpacaHttpError::RateLimit(body));
        }

        if let Ok(error_body) = serde_json::from_slice::<AlpacaErrorBody>(&response.body) {
            return Err(AlpacaHttpError::Venue {
                code: error_body.code.unwrap_or_else(|| i64::from(status)),
                message: error_body.message,
            });
        }

        Err(AlpacaHttpError::Http { status, body })
    }

    fn rate_limit_keys(endpoint: &str) -> Vec<String> {
        let route = endpoint
            .strip_prefix("/v2/")
            .unwrap_or(endpoint)
            .split('/')
            .next()
            .unwrap_or(endpoint);
        vec![ALPACA_REST_BUCKET.to_string(), format!("alpaca:{route}")]
    }
}

fn json_headers() -> HashMap<String, String> {
    HashMap::from([("Content-Type".to_string(), "application/json".to_string())])
}

/// Domain HTTP client for Alpaca REST operations.
///
/// This client wraps [`AlpacaRawHttpClient`], converts selected endpoint
/// responses into Nautilus domain types, and caches parsed instruments.
pub struct AlpacaHttpClient {
    pub(crate) inner: Arc<AlpacaRawHttpClient>,
    pub(crate) instruments_cache: Arc<AtomicMap<Ustr, InstrumentAny>>,
    clock: &'static AtomicTime,
    cache_initialized: AtomicBool,
}

impl Debug for AlpacaHttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(stringify!(AlpacaHttpClient))
            .field("inner", &self.inner)
            .finish()
    }
}

impl Clone for AlpacaHttpClient {
    fn clone(&self) -> Self {
        let cache_initialized = AtomicBool::new(self.cache_initialized.load(Ordering::Acquire));

        Self {
            inner: self.inner.clone(),
            instruments_cache: self.instruments_cache.clone(),
            clock: self.clock,
            cache_initialized,
        }
    }
}

impl AlpacaHttpClient {
    /// Creates a new unauthenticated [`AlpacaHttpClient`] (market data only).
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying raw HTTP client cannot be created.
    pub fn new(
        environment: AlpacaEnvironment,
        base_url_trading: Option<String>,
        base_url_data: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> AlpacaHttpResult<Self> {
        let raw_client = AlpacaRawHttpClient::new(
            environment,
            base_url_trading,
            base_url_data,
            timeout_secs,
            proxy_url,
            None,
        )?;
        Ok(Self::from_raw(raw_client))
    }

    /// Creates a new authenticated [`AlpacaHttpClient`].
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying raw HTTP client cannot be created.
    pub fn with_credentials(
        api_key: String,
        api_secret: String,
        environment: AlpacaEnvironment,
        base_url_trading: Option<String>,
        base_url_data: Option<String>,
        timeout_secs: u64,
        proxy_url: Option<String>,
    ) -> AlpacaHttpResult<Self> {
        let credential = Credential::new(api_key, api_secret);
        let raw_client = AlpacaRawHttpClient::new(
            environment,
            base_url_trading,
            base_url_data,
            timeout_secs,
            proxy_url,
            Some(credential),
        )?;
        Ok(Self::from_raw(raw_client))
    }

    /// Creates a new authenticated [`AlpacaHttpClient`] from environment
    /// variables (see [`crate::common::credential::credential_env_vars`]).
    ///
    /// # Errors
    ///
    /// Returns an error if credentials cannot be resolved or the client cannot
    /// be created.
    pub fn from_env(environment: AlpacaEnvironment) -> AlpacaHttpResult<Self> {
        let credential = Credential::resolve(None, None, environment)
            .map_err(|e| AlpacaHttpError::Validation(e.to_string()))?
            .ok_or(AlpacaHttpError::MissingCredentials)?;
        let raw_client =
            AlpacaRawHttpClient::new(environment, None, None, 60, None, Some(credential))?;
        Ok(Self::from_raw(raw_client))
    }

    /// Wraps an existing raw HTTP client.
    #[must_use]
    pub fn from_raw(raw_client: AlpacaRawHttpClient) -> Self {
        Self {
            inner: Arc::new(raw_client),
            instruments_cache: Arc::new(AtomicMap::new()),
            clock: get_atomic_clock_realtime(),
            cache_initialized: AtomicBool::new(false),
        }
    }

    /// Returns the configured Trading API base URL.
    #[must_use]
    pub fn base_url_trading(&self) -> &str {
        self.inner.base_url_trading()
    }

    /// Returns the configured Market Data API base URL.
    #[must_use]
    pub fn base_url_data(&self) -> &str {
        self.inner.base_url_data()
    }

    /// Returns the configured Alpaca environment.
    #[must_use]
    pub fn environment(&self) -> AlpacaEnvironment {
        self.inner.environment()
    }

    /// Overrides both base URLs. Intended for mock-server tests.
    ///
    /// # Panics
    ///
    /// Panics if the raw client is shared by another [`Arc`].
    pub fn set_base_urls(&mut self, base_url_trading: &str, base_url_data: &str) {
        let raw = Arc::get_mut(&mut self.inner).expect("cannot override URL: raw client shared");
        raw.set_base_url_trading(base_url_trading);
        raw.set_base_url_data(base_url_data);
    }

    /// Generates a timestamp for initialization.
    fn generate_ts_init(&self) -> UnixNanos {
        self.clock.get_time_ns()
    }

    /// Caches multiple instruments, replacing entries with the same symbol.
    pub fn cache_instruments(&self, instruments: &[InstrumentAny]) {
        self.instruments_cache.rcu(|m| {
            for inst in instruments {
                m.insert(inst.raw_symbol().inner(), inst.clone());
            }
        });
        self.cache_initialized.store(true, Ordering::Release);
    }

    /// Caches a single instrument, replacing any entry with the same symbol.
    pub fn cache_instrument(&self, instrument: InstrumentAny) {
        self.instruments_cache
            .insert(instrument.raw_symbol().inner(), instrument);
        self.cache_initialized.store(true, Ordering::Release);
    }

    /// Gets an instrument from the cache by symbol.
    #[must_use]
    pub fn get_instrument(&self, symbol: &Ustr) -> Option<InstrumentAny> {
        self.instruments_cache.get_cloned(symbol)
    }

    /// Gets an instrument from the cache by instrument ID.
    #[must_use]
    pub fn get_instrument_by_id(&self, instrument_id: &InstrumentId) -> Option<InstrumentAny> {
        self.instruments_cache
            .get_cloned(&instrument_id.symbol.inner())
    }

    /// Requests all active, tradable US equity instruments and caches them.
    ///
    /// Assets that fail to parse are skipped with a warning rather than
    /// failing the whole request.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying request fails.
    pub async fn request_instruments(&self) -> AlpacaHttpResult<Vec<InstrumentAny>> {
        let params = GetAssetsParamsBuilder::default()
            .status(AlpacaAssetStatus::Active)
            .asset_class(AlpacaAssetClass::UsEquity)
            .build()
            .map_err(|e| AlpacaHttpError::Validation(e.to_string()))?;
        let assets = self.inner.get_assets(&params).await?;
        let ts_init = self.generate_ts_init();

        let mut instruments = Vec::with_capacity(assets.len());
        for asset in assets.iter().filter(|a| a.tradable) {
            match parse_equity_instrument(asset, ts_init) {
                Ok(instrument) => instruments.push(instrument),
                Err(e) => log::warn!("Skipping asset {symbol}: {e}", symbol = asset.symbol),
            }
        }

        self.cache_instruments(&instruments);
        Ok(instruments)
    }

    /// Requests a single instrument by symbol and caches it.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the asset cannot be parsed.
    pub async fn request_instrument(&self, symbol: &str) -> AlpacaHttpResult<InstrumentAny> {
        let asset = self.inner.get_asset(symbol).await?;
        let ts_init = self.generate_ts_init();
        let instrument = parse_equity_instrument(&asset, ts_init)?;
        self.cache_instrument(instrument.clone());
        Ok(instrument)
    }

    /// Calls `GET /v2/account`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_account(&self) -> AlpacaHttpResult<AlpacaAccount> {
        self.inner.get_account().await
    }

    /// Calls `GET /v2/assets`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_assets(&self, params: &GetAssetsParams) -> AlpacaHttpResult<Vec<AlpacaAsset>> {
        self.inner.get_assets(params).await
    }

    /// Calls `POST /v2/orders`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the venue rejects the order.
    pub async fn submit_order(&self, params: &PostOrderParams) -> AlpacaHttpResult<AlpacaOrder> {
        self.inner.submit_order(params).await
    }

    /// Calls `GET /v2/orders`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_orders(&self, params: &GetOrdersParams) -> AlpacaHttpResult<Vec<AlpacaOrder>> {
        self.inner.get_orders(params).await
    }

    /// Calls `GET /v2/orders/{order_id}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_order(&self, order_id: &str) -> AlpacaHttpResult<AlpacaOrder> {
        self.inner.get_order(order_id).await
    }

    /// Calls `DELETE /v2/orders/{order_id}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the order is not cancelable.
    pub async fn cancel_order(&self, order_id: &str) -> AlpacaHttpResult<()> {
        self.inner.cancel_order(order_id).await
    }

    /// Calls `DELETE /v2/orders` (cancel all).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails.
    pub async fn cancel_all_orders(&self) -> AlpacaHttpResult<Vec<AlpacaCancelOrderStatus>> {
        self.inner.cancel_all_orders().await
    }

    /// Calls `PATCH /v2/orders/{order_id}`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the venue rejects the replace.
    pub async fn patch_order(
        &self,
        order_id: &str,
        params: &PatchOrderParams,
    ) -> AlpacaHttpResult<AlpacaOrder> {
        self.inner.patch_order(order_id, params).await
    }

    /// Calls `GET /v2/positions`.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_positions(&self) -> AlpacaHttpResult<Vec<AlpacaPosition>> {
        self.inner.get_positions().await
    }

    /// Calls `GET /v2/account/activities` (fills, fees, and other postings).
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_account_activities(
        &self,
        params: &GetAccountActivitiesParams,
    ) -> AlpacaHttpResult<Vec<AlpacaAccountActivity>> {
        self.inner.get_account_activities(params).await
    }

    /// Requests historical bars for a single request page.
    ///
    /// # Errors
    ///
    /// Returns an error if the request fails or the response is invalid.
    pub async fn get_stock_bars(
        &self,
        params: &GetStockBarsParams,
    ) -> AlpacaHttpResult<AlpacaBarsResponse> {
        self.inner.get_stock_bars(params).await
    }

    /// Requests historical bars, following `next_page_token` pagination.
    ///
    /// Returns bars merged across pages, keyed by symbol. Pagination stops
    /// after [`MAX_PAGINATION_PAGES`] pages as a runaway guard.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails.
    pub async fn get_stock_bars_paginated(
        &self,
        params: &GetStockBarsParams,
    ) -> AlpacaHttpResult<HashMap<String, Vec<AlpacaBar>>> {
        let mut params = params.clone();
        let mut merged: HashMap<String, Vec<AlpacaBar>> = HashMap::new();

        for _ in 0..MAX_PAGINATION_PAGES {
            let response = self.inner.get_stock_bars(&params).await?;
            for (symbol, bars) in response.bars {
                merged.entry(symbol).or_default().extend(bars);
            }
            match response.next_page_token {
                Some(token) => params.page_token = Some(token),
                None => return Ok(merged),
            }
        }

        log::warn!(
            "Bar pagination stopped after {MAX_PAGINATION_PAGES} pages for symbols {symbols}",
            symbols = params.symbols,
        );
        Ok(merged)
    }

    /// Requests historical trades, following `next_page_token` pagination.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails.
    pub async fn get_stock_trades_paginated(
        &self,
        params: &GetStockTradesParams,
    ) -> AlpacaHttpResult<HashMap<String, Vec<AlpacaHistoricalTrade>>> {
        let mut params = params.clone();
        let mut merged: HashMap<String, Vec<AlpacaHistoricalTrade>> = HashMap::new();

        for _ in 0..MAX_PAGINATION_PAGES {
            let response = self.inner.get_stock_trades(&params).await?;
            for (symbol, trades) in response.trades {
                merged.entry(symbol).or_default().extend(trades);
            }
            match response.next_page_token {
                Some(token) => params.page_token = Some(token),
                None => return Ok(merged),
            }
        }

        log::warn!(
            "Trade pagination stopped after {MAX_PAGINATION_PAGES} pages for symbols {symbols}",
            symbols = params.symbols,
        );
        Ok(merged)
    }

    /// Requests historical quotes, following `next_page_token` pagination.
    ///
    /// # Errors
    ///
    /// Returns an error if any page request fails.
    pub async fn get_stock_quotes_paginated(
        &self,
        params: &GetStockQuotesParams,
    ) -> AlpacaHttpResult<HashMap<String, Vec<AlpacaHistoricalQuote>>> {
        let mut params = params.clone();
        let mut merged: HashMap<String, Vec<AlpacaHistoricalQuote>> = HashMap::new();

        for _ in 0..MAX_PAGINATION_PAGES {
            let response = self.inner.get_stock_quotes(&params).await?;
            for (symbol, quotes) in response.quotes {
                merged.entry(symbol).or_default().extend(quotes);
            }
            match response.next_page_token {
                Some(token) => params.page_token = Some(token),
                None => return Ok(merged),
            }
        }

        log::warn!(
            "Quote pagination stopped after {MAX_PAGINATION_PAGES} pages for symbols {symbols}",
            symbols = params.symbols,
        );
        Ok(merged)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case("/v2/orders", vec!["alpaca:rest", "alpaca:orders"])]
    #[case("/v2/orders/abc-123", vec!["alpaca:rest", "alpaca:orders"])]
    #[case("/v2/stocks/bars", vec!["alpaca:rest", "alpaca:stocks"])]
    #[case("/v2/account", vec!["alpaca:rest", "alpaca:account"])]
    fn test_rate_limit_keys(#[case] endpoint: &str, #[case] expected: Vec<&str>) {
        assert_eq!(AlpacaRawHttpClient::rate_limit_keys(endpoint), expected);
    }

    #[rstest]
    fn test_unauthenticated_client_rejects_trading_calls() {
        let client =
            AlpacaRawHttpClient::new(AlpacaEnvironment::Paper, None, None, 60, None, None).unwrap();

        let result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(client.get_account());

        assert!(matches!(result, Err(AlpacaHttpError::MissingCredentials)));
    }

    #[rstest]
    fn test_default_base_urls() {
        let client =
            AlpacaRawHttpClient::new(AlpacaEnvironment::Paper, None, None, 60, None, None).unwrap();

        assert_eq!(
            client.base_url_trading(),
            "https://paper-api.alpaca.markets"
        );
        assert_eq!(client.base_url_data(), "https://data.alpaca.markets");
    }
}

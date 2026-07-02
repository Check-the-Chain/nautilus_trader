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

//! Live data client for the Alpaca adapter.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::Context;
use nautilus_common::{
    cache::InstrumentLookupError,
    clients::DataClient,
    live::{runner::get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent,
        data::{
            BarsResponse, DataResponse, InstrumentResponse, InstrumentsResponse, QuotesResponse,
            RequestBars, RequestInstrument, RequestInstruments, RequestQuotes, RequestTrades,
            SubscribeBars, SubscribeInstrument, SubscribeQuotes, SubscribeTrades, TradesResponse,
            UnsubscribeBars, UnsubscribeInstrument, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    AtomicMap,
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{BarType, Data},
    enums::{AggregationSource, BarAggregation, PriceType},
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{consts::ALPACA_VENUE, credential::Credential, enums::AlpacaDataFeed},
    config::AlpacaDataClientConfig,
    http::client::{AlpacaHttpClient, AlpacaRawHttpClient},
    websocket::{
        client::AlpacaWebSocketClient,
        messages::{AlpacaInstrumentInfo, NautilusWsMessage, WsFormat},
    },
};

/// Live data client for Alpaca US spot equities.
///
/// Streams trades, quotes, and 1-minute bars over the equities market data
/// WebSocket, and serves historical bars/trades/quotes plus instrument
/// definitions over the Market Data REST API.
#[derive(Debug)]
pub struct AlpacaDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: AlpacaDataClientConfig,
    credential: Option<Credential>,
    http_client: AlpacaHttpClient,
    ws_client: Option<AlpacaWebSocketClient>,
    is_connected: AtomicBool,
    cancellation_token: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    instruments: Arc<AtomicMap<InstrumentId, InstrumentAny>>,
}

impl AlpacaDataClient {
    /// Creates a new [`AlpacaDataClient`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if credential resolution or HTTP client construction
    /// fails.
    pub fn new(client_id: ClientId, config: AlpacaDataClientConfig) -> anyhow::Result<Self> {
        let clock = get_atomic_clock_realtime();
        let data_sender = get_data_event_sender();

        let credential = Credential::resolve(
            config.api_key.clone(),
            config.api_secret.clone(),
            config.environment,
        )?;

        let raw_client = AlpacaRawHttpClient::new(
            config.environment,
            None,
            config.base_url_http.clone(),
            config.http_timeout_secs,
            config.proxy_url.clone(),
            credential.clone(),
        )?;
        let http_client = AlpacaHttpClient::from_raw(raw_client);

        Ok(Self {
            clock,
            client_id,
            config,
            credential,
            http_client,
            ws_client: None,
            is_connected: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            tasks: Vec::new(),
            data_sender,
            instruments: Arc::new(AtomicMap::new()),
        })
    }

    fn venue(&self) -> Venue {
        *ALPACA_VENUE
    }

    fn has_credentials(&self) -> bool {
        self.credential.is_some()
    }

    fn ws(&self) -> anyhow::Result<&AlpacaWebSocketClient> {
        self.ws_client
            .as_ref()
            .context("Alpaca WebSocket client not connected")
    }

    fn abort_tasks(&mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
    }

    fn instrument_info(instrument: &InstrumentAny) -> AlpacaInstrumentInfo {
        AlpacaInstrumentInfo {
            instrument_id: instrument.id(),
            price_precision: instrument.price_precision(),
            size_precision: instrument.size_precision(),
        }
    }

    /// Fetches instruments, caches them, and returns them.
    async fn bootstrap_instruments(&self) -> anyhow::Result<Vec<InstrumentAny>> {
        let instruments = self
            .http_client
            .request_instruments()
            .await
            .context("failed to fetch instruments during bootstrap")?;

        self.instruments.rcu(|map| {
            for instrument in &instruments {
                map.insert(instrument.id(), instrument.clone());
            }
        });

        log::debug!("Bootstrapped {} Alpaca instruments", instruments.len());
        Ok(instruments)
    }

    async fn spawn_ws(&mut self, instruments: &[InstrumentAny]) -> anyhow::Result<()> {
        let credential = self
            .credential
            .clone()
            .context("Alpaca credentials required for the market data stream")?;

        let format = if self.config.use_msgpack {
            WsFormat::Msgpack
        } else {
            WsFormat::Json
        };
        let mut ws_client = AlpacaWebSocketClient::new(
            self.config.base_url_ws.clone(),
            self.config.data_feed,
            credential,
            format,
            self.config.transport_backend,
            self.config.proxy_url.clone(),
        );

        ws_client.initialize_instruments(instruments.iter().map(Self::instrument_info).collect());

        ws_client
            .connect()
            .await
            .context("failed to connect to Alpaca WebSocket")?;

        // Keep a receiver-less clone for subscribe commands; the connected
        // client (owning `out_rx` and the handler task handle) moves into the
        // consumer task below.
        self.ws_client = Some(ws_client.clone());

        let cancellation_token = self.cancellation_token.clone();
        let data_sender = self.data_sender.clone();

        let task = get_runtime().spawn(async move {
            log::debug!("Alpaca WebSocket consumption loop started");

            loop {
                tokio::select! {
                    () = cancellation_token.cancelled() => {
                        log::debug!("Alpaca WebSocket consumption loop cancelled");
                        break;
                    }
                    msg_opt = ws_client.next_event() => {
                        match msg_opt {
                            Some(NautilusWsMessage::Trades(trades)) => {
                                for trade in trades {
                                    if let Err(e) = data_sender
                                        .send(DataEvent::Data(Data::Trade(trade)))
                                    {
                                        log::error!("Failed to send trade tick: {e}");
                                    }
                                }
                            }
                            Some(NautilusWsMessage::Quote(quote)) => {
                                if let Err(e) = data_sender
                                    .send(DataEvent::Data(Data::Quote(quote)))
                                {
                                    log::error!("Failed to send quote tick: {e}");
                                }
                            }
                            Some(NautilusWsMessage::Bar(bar)) => {
                                if let Err(e) = data_sender.send(DataEvent::Data(Data::Bar(bar))) {
                                    log::error!("Failed to send bar: {e}");
                                }
                            }
                            Some(NautilusWsMessage::SubscriptionAck(ack)) => {
                                log::debug!("Alpaca subscription ack: {ack:?}");
                            }
                            Some(NautilusWsMessage::Authenticated) => {
                                log::debug!("Alpaca WebSocket authenticated");
                            }
                            Some(NautilusWsMessage::Error { code, msg }) => {
                                log::warn!("Alpaca WebSocket error {code:?}: {msg}");
                            }
                            Some(NautilusWsMessage::Reconnected) => {
                                log::info!("Alpaca WebSocket reconnected");
                            }
                            // Trade updates arrive on the execution client's
                            // dedicated stream, never on the market data feed.
                            Some(NautilusWsMessage::TradeUpdate(_)) => {}
                            None => {
                                log::debug!("Alpaca WebSocket next_event returned None");
                                tokio::select! {
                                    () = cancellation_token.cancelled() => {
                                        log::debug!(
                                            "Alpaca WebSocket consumption loop cancelled"
                                        );
                                        break;
                                    }
                                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                                }
                            }
                        }
                    }
                }
            }

            log::debug!("Alpaca WebSocket consumption loop finished");
        });

        self.tasks.push(task);
        log::debug!("Alpaca WebSocket consumption task spawned");

        Ok(())
    }

    fn spawn_instrument_refresh(&mut self) {
        let minutes = self.config.update_instruments_interval_mins;
        if minutes == 0 {
            log::debug!("Alpaca instrument refresh disabled (interval=0)");
            return;
        }

        let interval = Duration::from_secs(minutes.saturating_mul(60));
        let cancellation = self.cancellation_token.clone();
        let http_client = self.http_client.clone();
        let instruments_cache = Arc::clone(&self.instruments);
        let ws_client = self.ws_client.clone();
        let data_sender = self.data_sender.clone();

        let handle = get_runtime().spawn(async move {
            loop {
                let sleep = tokio::time::sleep(interval);
                tokio::pin!(sleep);
                tokio::select! {
                    () = cancellation.cancelled() => {
                        log::debug!("Alpaca instrument refresh task cancelled");
                        break;
                    }
                    () = &mut sleep => {
                        match http_client.request_instruments().await {
                            Ok(instruments) => {
                                instruments_cache.rcu(|map| {
                                    for instrument in &instruments {
                                        map.insert(instrument.id(), instrument.clone());
                                    }
                                });

                                if let Some(ws) = &ws_client {
                                    for instrument in &instruments {
                                        ws.add_instrument(Self::instrument_info(instrument));
                                    }
                                }

                                for instrument in instruments {
                                    if let Err(e) = data_sender
                                        .send(DataEvent::Instrument(instrument))
                                    {
                                        log::warn!("Failed to send instrument: {e}");
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Alpaca instrument refresh failed: {e:?}");
                            }
                        }
                    }
                }
            }
        });

        self.tasks.push(handle);
        log::debug!("Alpaca instrument refresh task spawned (interval={minutes}m)");
    }

    fn get_cached_instrument(&self, instrument_id: &InstrumentId) -> anyhow::Result<InstrumentAny> {
        self.instruments
            .load()
            .get(instrument_id)
            .cloned()
            .ok_or_else(|| InstrumentLookupError::not_found(*instrument_id).into())
    }
}

/// Validates that a bar type is streamable over the Alpaca WebSocket.
///
/// The venue only emits externally aggregated 1-minute bars on the `bars`
/// channel.
fn validate_ws_bar_type(bar_type: &BarType) -> anyhow::Result<()> {
    let spec = bar_type.spec();
    anyhow::ensure!(
        bar_type.aggregation_source() == AggregationSource::External,
        "Alpaca only streams externally aggregated bars; use INTERNAL aggregation for other specs",
    );
    anyhow::ensure!(
        spec.step.get() == 1
            && spec.aggregation == BarAggregation::Minute
            && spec.price_type == PriceType::Last,
        "Alpaca only streams 1-MINUTE-LAST bars, was {spec}",
    );
    Ok(())
}

/// Maps a Nautilus bar type onto an Alpaca historical bars `timeframe` string.
///
/// # Errors
///
/// Returns an error if the specification has no Alpaca equivalent.
fn bar_type_to_timeframe(bar_type: &BarType) -> anyhow::Result<String> {
    let spec = bar_type.spec();
    anyhow::ensure!(
        spec.price_type == PriceType::Last,
        "Alpaca bars are LAST-price aggregated, was {}",
        spec.price_type,
    );

    let step = spec.step.get();
    match spec.aggregation {
        BarAggregation::Minute if (1..=59).contains(&step) => Ok(format!("{step}Min")),
        BarAggregation::Hour if (1..=23).contains(&step) => Ok(format!("{step}Hour")),
        BarAggregation::Day if step == 1 => Ok("1Day".to_string()),
        BarAggregation::Week if step == 1 => Ok("1Week".to_string()),
        BarAggregation::Month if matches!(step, 1 | 2 | 3 | 4 | 6 | 12) => {
            Ok(format!("{step}Month"))
        }
        aggregation => anyhow::bail!(
            "unsupported bar specification for Alpaca: step={step}, aggregation={aggregation}",
        ),
    }
}

/// Maps the configured stream feed onto a historical REST `feed` parameter.
///
/// The historical endpoints accept `iex`/`sip` (plus `otc`/`boats`); the
/// delayed and test stream feeds have no historical equivalent, so those fall
/// back to the account's default feed.
const fn historical_feed(feed: AlpacaDataFeed) -> Option<AlpacaDataFeed> {
    match feed {
        AlpacaDataFeed::Iex | AlpacaDataFeed::Sip => Some(feed),
        AlpacaDataFeed::DelayedSip | AlpacaDataFeed::Test => None,
    }
}

#[async_trait::async_trait(?Send)]
impl DataClient for AlpacaDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Starting Alpaca data client: client_id={}, environment={:?}, data_feed={:?}, has_credentials={}",
            self.client_id,
            self.config.environment,
            self.config.data_feed,
            self.has_credentials(),
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping Alpaca data client {}", self.client_id);
        self.cancellation_token.cancel();
        self.abort_tasks();
        self.ws_client = None;
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        log::debug!("Resetting Alpaca data client {}", self.client_id);
        self.cancellation_token.cancel();
        self.abort_tasks();
        self.ws_client = None;
        self.is_connected.store(false, Ordering::Relaxed);
        self.cancellation_token = CancellationToken::new();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        log::debug!("Disposing Alpaca data client {}", self.client_id);
        self.stop()
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Acquire)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        // `stop()` cancels the token to tear down consumer tasks; rotate it
        // here so a reconnect does not clone an already-cancelled token.
        if self.cancellation_token.is_cancelled() {
            self.cancellation_token = CancellationToken::new();
        }

        let instruments = self
            .bootstrap_instruments()
            .await
            .context("failed to bootstrap Alpaca instruments")?;

        for instrument in &instruments {
            if let Err(e) = self
                .data_sender
                .send(DataEvent::Instrument(instrument.clone()))
            {
                log::warn!("Failed to send instrument: {e}");
            }
        }

        self.spawn_ws(&instruments)
            .await
            .context("failed to spawn Alpaca WebSocket consumer")?;
        self.spawn_instrument_refresh();

        self.is_connected.store(true, Ordering::Relaxed);
        log::info!("Connected: client_id={}", self.client_id);

        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if !self.is_connected() {
            return Ok(());
        }

        self.cancellation_token.cancel();

        for task in self.tasks.drain(..) {
            if let Err(e) = task.await {
                log::error!("Error waiting for Alpaca task to complete: {e}");
            }
        }

        if let Some(mut ws_client) = self.ws_client.take()
            && let Err(e) = ws_client.disconnect().await
        {
            log::warn!("Error disconnecting Alpaca WebSocket: {e}");
        }

        self.instruments.store(ahash::AHashMap::new());
        self.is_connected.store(false, Ordering::Relaxed);
        log::info!("Disconnected: client_id={}", self.client_id);

        Ok(())
    }

    fn subscribe_instrument(&mut self, cmd: SubscribeInstrument) -> anyhow::Result<()> {
        let instruments = self.instruments.load();
        if let Some(instrument) = instruments.get(&cmd.instrument_id) {
            if let Err(e) = self
                .data_sender
                .send(DataEvent::Instrument(instrument.clone()))
            {
                log::error!("Failed to send instrument {}: {e}", cmd.instrument_id);
            }
        } else {
            log::warn!("Instrument {} not found in cache", cmd.instrument_id);
        }
        Ok(())
    }

    fn unsubscribe_instrument(&mut self, cmd: &UnsubscribeInstrument) -> anyhow::Result<()> {
        log::debug!(
            "Unsubscribing from instrument: {} (cache replay only)",
            cmd.instrument_id,
        );
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: SubscribeTrades) -> anyhow::Result<()> {
        log::debug!("Subscribing to trades: {}", cmd.instrument_id);

        let ws = self.ws()?.clone();
        let instrument_id = cmd.instrument_id;

        get_runtime().spawn(async move {
            if let Err(e) = ws.subscribe_trades(instrument_id).await {
                log::error!("Failed to subscribe to Alpaca trades: {e:?}");
            }
        });

        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        log::debug!("Unsubscribing from trades: {}", cmd.instrument_id);

        let ws = self.ws()?.clone();
        let instrument_id = cmd.instrument_id;

        get_runtime().spawn(async move {
            if let Err(e) = ws.unsubscribe_trades(instrument_id).await {
                log::error!("Failed to unsubscribe from Alpaca trades: {e:?}");
            }
        });

        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: SubscribeQuotes) -> anyhow::Result<()> {
        log::debug!("Subscribing to quotes: {}", cmd.instrument_id);

        let ws = self.ws()?.clone();
        let instrument_id = cmd.instrument_id;

        get_runtime().spawn(async move {
            if let Err(e) = ws.subscribe_quotes(instrument_id).await {
                log::error!("Failed to subscribe to Alpaca quotes: {e:?}");
            }
        });

        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        log::debug!("Unsubscribing from quotes: {}", cmd.instrument_id);

        let ws = self.ws()?.clone();
        let instrument_id = cmd.instrument_id;

        get_runtime().spawn(async move {
            if let Err(e) = ws.unsubscribe_quotes(instrument_id).await {
                log::error!("Failed to unsubscribe from Alpaca quotes: {e:?}");
            }
        });

        Ok(())
    }

    fn subscribe_bars(&mut self, cmd: SubscribeBars) -> anyhow::Result<()> {
        log::debug!("Subscribing to bars: {}", cmd.bar_type);

        validate_ws_bar_type(&cmd.bar_type)?;

        let ws = self.ws()?.clone();
        let instrument_id = cmd.bar_type.instrument_id();

        get_runtime().spawn(async move {
            if let Err(e) = ws.subscribe_bars(instrument_id).await {
                log::error!("Failed to subscribe to Alpaca bars: {e:?}");
            }
        });

        Ok(())
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        log::debug!("Unsubscribing from bars: {}", cmd.bar_type);

        let ws = self.ws()?.clone();
        let instrument_id = cmd.bar_type.instrument_id();

        get_runtime().spawn(async move {
            if let Err(e) = ws.unsubscribe_bars(instrument_id).await {
                log::error!("Failed to unsubscribe from Alpaca bars: {e:?}");
            }
        });

        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        log::debug!("Requesting Alpaca instruments");

        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments_cache = Arc::clone(&self.instruments);
        let ws_client = self.ws_client.clone();
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let venue = self.venue();
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let clock = self.clock;

        get_runtime().spawn(async move {
            match http.request_instruments().await {
                Ok(instruments) => {
                    instruments_cache.rcu(|map| {
                        for instrument in &instruments {
                            map.insert(instrument.id(), instrument.clone());
                        }
                    });

                    if let Some(ws) = &ws_client {
                        for instrument in &instruments {
                            ws.add_instrument(Self::instrument_info(instrument));
                        }
                    }

                    let response = DataResponse::Instruments(InstrumentsResponse::new(
                        request_id,
                        client_id,
                        venue,
                        instruments,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send instruments response: {e}");
                    }
                }
                Err(e) => {
                    log::error!("Failed to fetch Alpaca instruments: {e:?}");
                }
            }
        });

        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        log::debug!("Requesting Alpaca instrument: {}", request.instrument_id);

        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments_cache = Arc::clone(&self.instruments);
        let ws_client = self.ws_client.clone();
        let instrument_id = request.instrument_id;
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let clock = self.clock;

        get_runtime().spawn(async move {
            match http.request_instrument(instrument_id.symbol.as_str()).await {
                Ok(instrument) => {
                    instruments_cache.rcu(|map| {
                        map.insert(instrument.id(), instrument.clone());
                    });

                    if let Some(ws) = &ws_client {
                        ws.add_instrument(Self::instrument_info(&instrument));
                    }

                    let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                        request_id,
                        client_id,
                        instrument.id(),
                        instrument,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    )));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send instrument response: {e}");
                    }
                }
                Err(e) => {
                    log::error!("Failed to fetch Alpaca instrument {instrument_id}: {e:?}");
                }
            }
        });

        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let bar_type = request.bar_type;
        log::debug!("Requesting Alpaca bars for {bar_type}");

        let timeframe = bar_type_to_timeframe(&bar_type)?;
        let instrument_id = bar_type.instrument_id();
        let instrument = self.get_cached_instrument(&instrument_id)?;

        let mut params_builder = crate::http::query::GetStockBarsParamsBuilder::default();
        params_builder
            .symbols(instrument_id.symbol.as_str())
            .timeframe(timeframe);
        if let Some(start) = request.start {
            params_builder.start(start.to_rfc3339());
        }
        if let Some(end) = request.end {
            params_builder.end(end.to_rfc3339());
        }
        if let Some(limit) = request.limit {
            params_builder.limit(u32::try_from(limit.get()).unwrap_or(u32::MAX));
        }
        if let Some(feed) = historical_feed(self.config.data_feed) {
            params_builder.feed(feed);
        }
        let query = params_builder.build().context("invalid bars query")?;

        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let clock = self.clock;
        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();
        let symbol = instrument_id.symbol.to_string();

        get_runtime().spawn(async move {
            match http.get_stock_bars_paginated(&query).await {
                Ok(mut merged) => {
                    let ts_init = clock.get_time_ns();
                    let bars: Vec<_> = merged
                        .remove(&symbol)
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|bar| {
                            crate::http::parse::parse_bar(
                                bar,
                                bar_type,
                                price_precision,
                                size_precision,
                                ts_init,
                            )
                            .map_err(|e| log::warn!("Skipping unparseable bar: {e}"))
                            .ok()
                        })
                        .collect();

                    let response = DataResponse::Bars(BarsResponse::new(
                        request_id,
                        client_id,
                        bar_type,
                        bars,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send bars response: {e}");
                    }
                }
                Err(e) => {
                    log::error!("Alpaca bars request failed for {instrument_id}: {e:?}");
                }
            }
        });

        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let instrument_id = request.instrument_id;
        log::debug!("Requesting Alpaca trades for {instrument_id}");

        let instrument = self.get_cached_instrument(&instrument_id)?;

        let mut params_builder = crate::http::query::GetStockTradesParamsBuilder::default();
        params_builder.symbols(instrument_id.symbol.as_str());
        if let Some(start) = request.start {
            params_builder.start(start.to_rfc3339());
        }
        if let Some(end) = request.end {
            params_builder.end(end.to_rfc3339());
        }
        if let Some(limit) = request.limit {
            params_builder.limit(u32::try_from(limit.get()).unwrap_or(u32::MAX));
        }
        if let Some(feed) = historical_feed(self.config.data_feed) {
            params_builder.feed(feed);
        }
        let query = params_builder.build().context("invalid trades query")?;

        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let clock = self.clock;
        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();
        let symbol = instrument_id.symbol.to_string();

        get_runtime().spawn(async move {
            match http.get_stock_trades_paginated(&query).await {
                Ok(mut merged) => {
                    let ts_init = clock.get_time_ns();
                    let trades: Vec<_> = merged
                        .remove(&symbol)
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|trade| {
                            crate::http::parse::parse_historical_trade(
                                trade,
                                instrument_id,
                                price_precision,
                                size_precision,
                                ts_init,
                            )
                            .map_err(|e| log::warn!("Skipping unparseable trade: {e}"))
                            .ok()
                        })
                        .collect();

                    let response = DataResponse::Trades(TradesResponse::new(
                        request_id,
                        client_id,
                        instrument_id,
                        trades,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send trades response: {e}");
                    }
                }
                Err(e) => {
                    log::error!("Alpaca trades request failed for {instrument_id}: {e:?}");
                }
            }
        });

        Ok(())
    }

    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        let instrument_id = request.instrument_id;
        log::debug!("Requesting Alpaca quotes for {instrument_id}");

        let instrument = self.get_cached_instrument(&instrument_id)?;

        let mut params_builder = crate::http::query::GetStockQuotesParamsBuilder::default();
        params_builder.symbols(instrument_id.symbol.as_str());
        if let Some(start) = request.start {
            params_builder.start(start.to_rfc3339());
        }
        if let Some(end) = request.end {
            params_builder.end(end.to_rfc3339());
        }
        if let Some(limit) = request.limit {
            params_builder.limit(u32::try_from(limit.get()).unwrap_or(u32::MAX));
        }
        if let Some(feed) = historical_feed(self.config.data_feed) {
            params_builder.feed(feed);
        }
        let query = params_builder.build().context("invalid quotes query")?;

        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start_nanos = datetime_to_unix_nanos(request.start);
        let end_nanos = datetime_to_unix_nanos(request.end);
        let params = request.params;
        let clock = self.clock;
        let price_precision = instrument.price_precision();
        let size_precision = instrument.size_precision();
        let symbol = instrument_id.symbol.to_string();

        get_runtime().spawn(async move {
            match http.get_stock_quotes_paginated(&query).await {
                Ok(mut merged) => {
                    let ts_init = clock.get_time_ns();
                    let quotes: Vec<_> = merged
                        .remove(&symbol)
                        .unwrap_or_default()
                        .iter()
                        .filter_map(|quote| {
                            crate::http::parse::parse_historical_quote(
                                quote,
                                instrument_id,
                                price_precision,
                                size_precision,
                                ts_init,
                            )
                            .map_err(|e| log::debug!("Skipping quote: {e}"))
                            .ok()
                        })
                        .collect();

                    let response = DataResponse::Quotes(QuotesResponse::new(
                        request_id,
                        client_id,
                        instrument_id,
                        quotes,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send quotes response: {e}");
                    }
                }
                Err(e) => {
                    log::error!("Alpaca quotes request failed for {instrument_id}: {e:?}");
                }
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nautilus_model::data::BarSpecification;
    use rstest::rstest;

    use super::*;

    fn bar_type(value: &str) -> BarType {
        value.parse().unwrap()
    }

    #[rstest]
    fn test_validate_ws_bar_type_accepts_one_minute_last_external() {
        let bar_type = bar_type("AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL");
        assert!(validate_ws_bar_type(&bar_type).is_ok());
    }

    #[rstest]
    #[case("AAPL.ALPACA-5-MINUTE-LAST-EXTERNAL")]
    #[case("AAPL.ALPACA-1-MINUTE-MID-EXTERNAL")]
    #[case("AAPL.ALPACA-1-MINUTE-LAST-INTERNAL")]
    fn test_validate_ws_bar_type_rejects_unsupported(#[case] value: &str) {
        let bar_type = bar_type(value);
        assert!(validate_ws_bar_type(&bar_type).is_err());
    }

    #[rstest]
    #[case("AAPL.ALPACA-1-MINUTE-LAST-EXTERNAL", "1Min")]
    #[case("AAPL.ALPACA-15-MINUTE-LAST-EXTERNAL", "15Min")]
    #[case("AAPL.ALPACA-1-HOUR-LAST-EXTERNAL", "1Hour")]
    #[case("AAPL.ALPACA-1-DAY-LAST-EXTERNAL", "1Day")]
    #[case("AAPL.ALPACA-1-WEEK-LAST-EXTERNAL", "1Week")]
    #[case("AAPL.ALPACA-3-MONTH-LAST-EXTERNAL", "3Month")]
    fn test_bar_type_to_timeframe(#[case] value: &str, #[case] expected: &str) {
        assert_eq!(bar_type_to_timeframe(&bar_type(value)).unwrap(), expected);
    }

    #[rstest]
    #[case("AAPL.ALPACA-2-DAY-LAST-EXTERNAL")]
    #[case("AAPL.ALPACA-1-DAY-MID-EXTERNAL")]
    fn test_bar_type_to_timeframe_rejects_unsupported(#[case] value: &str) {
        assert!(bar_type_to_timeframe(&bar_type(value)).is_err());
    }

    #[rstest]
    fn test_historical_feed_mapping() {
        assert_eq!(
            historical_feed(AlpacaDataFeed::Iex),
            Some(AlpacaDataFeed::Iex)
        );
        assert_eq!(
            historical_feed(AlpacaDataFeed::Sip),
            Some(AlpacaDataFeed::Sip)
        );
        assert_eq!(historical_feed(AlpacaDataFeed::DelayedSip), None);
        assert_eq!(historical_feed(AlpacaDataFeed::Test), None);
    }

    #[rstest]
    fn test_bar_spec_display_used_in_errors() {
        // Guards the ensure! message formatting against BarSpecification changes.
        let spec = BarSpecification::new(1, BarAggregation::Minute, PriceType::Last);
        assert!(format!("{spec}").contains("MINUTE"));
    }
}

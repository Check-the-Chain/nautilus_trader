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

//! Live read-only data client for Variational Omni.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use ahash::{AHashMap, AHashSet};
use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent,
        data::{
            DataResponse, FundingRatesResponse, InstrumentResponse, InstrumentsResponse,
            QuotesResponse, RequestFundingRates, RequestInstrument, RequestInstruments,
            RequestQuotes, SubscribeFundingRates, SubscribeIndexPrices, SubscribeInstrument,
            SubscribeInstruments, SubscribeMarkPrices, SubscribeQuotes, UnsubscribeFundingRates,
            UnsubscribeIndexPrices, UnsubscribeMarkPrices, UnsubscribeQuotes,
        },
    },
};
use nautilus_core::{UnixNanos, datetime::datetime_to_unix_nanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::Data,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::Instrument,
};
use nautilus_network::{
    RECONNECTED,
    websocket::{TransportBackend, WebSocketClient, WebSocketConfig, channel_message_handler},
};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        VariationalInstrumentMeta, VariationalInstrumentRegistry, funding_rate_update_from_listing,
        index_price_update_from_ws, listing_for_instrument, load_instrument_registry,
        mark_price_update_from_ws, quote_tick_from_listing, stats_by_ticker, venue,
    },
    config::VariationalDataClientConfig,
    http::client::VariationalHttpClient,
    websocket::messages::{VariationalWsMessage, VariationalWsSubscriptionRequest},
};

#[derive(Clone, Debug, Default)]
struct VariationalSubscriptions {
    quotes: AHashSet<InstrumentId>,
    marks: AHashSet<InstrumentId>,
    index: AHashSet<InstrumentId>,
    funding: AHashSet<InstrumentId>,
}

impl VariationalSubscriptions {
    fn is_rest_empty(&self) -> bool {
        self.quotes.is_empty() && self.funding.is_empty()
    }
}

#[derive(Debug)]
pub struct VariationalDataClient {
    client_id: ClientId,
    http_client: VariationalHttpClient,
    config: VariationalDataClientConfig,
    is_connected: AtomicBool,
    cancellation_token: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    registry: Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    subscriptions: Arc<tokio::sync::RwLock<VariationalSubscriptions>>,
    clock: &'static nautilus_core::time::AtomicTime,
}

impl VariationalDataClient {
    pub fn new(client_id: ClientId, config: VariationalDataClientConfig) -> anyhow::Result<Self> {
        let http_client = VariationalHttpClient::new(
            config.base_url_http.clone(),
            config.proxy_url.clone(),
            config.http_timeout_secs,
        )?;

        Ok(Self {
            client_id,
            http_client,
            config,
            is_connected: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            tasks: Vec::new(),
            data_sender: get_data_event_sender(),
            registry: Arc::new(tokio::sync::RwLock::new(
                VariationalInstrumentRegistry::default(),
            )),
            subscriptions: Arc::new(tokio::sync::RwLock::new(VariationalSubscriptions::default())),
            clock: get_atomic_clock_realtime(),
        })
    }

    async fn bootstrap_instruments(
        &self,
    ) -> anyhow::Result<Vec<nautilus_model::instruments::InstrumentAny>> {
        let registry =
            load_instrument_registry(&self.http_client, self.config.default_size_precision).await?;
        let instruments = registry.instruments();
        *self.registry.write().await = registry;
        Ok(instruments)
    }

    fn spawn_poll_loop(&mut self) {
        let http_client = self.http_client.clone();
        let registry = Arc::clone(&self.registry);
        let subscriptions = Arc::clone(&self.subscriptions);
        let sender = self.data_sender.clone();
        let cancellation_token = self.cancellation_token.clone();
        let poll_interval = Duration::from_secs(self.config.poll_interval_secs.max(1));
        let quote_tier = self.config.quote_tier;
        let clock = self.clock;

        let task = get_runtime().spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    () = cancellation_token.cancelled() => break,
                    _ = interval.tick() => {
                        if let Err(error) = poll_once(
                            &http_client,
                            &registry,
                            &subscriptions,
                            &sender,
                            quote_tier,
                            clock,
                        ).await {
                            log::warn!("Failed to poll Variational stats: {error}");
                        }
                    }
                }
            }
        });
        self.tasks.push(task);
    }

    fn spawn_ws_price_loop(&mut self) {
        let registry = Arc::clone(&self.registry);
        let subscriptions = Arc::clone(&self.subscriptions);
        let sender = self.data_sender.clone();
        let cancellation_token = self.cancellation_token.clone();
        let ws_url = self.config.ws_prices_url();
        let proxy_url = self.config.proxy_url.clone();
        let funding_interval_s = self.config.ws_price_funding_interval_secs;
        let clock = self.clock;

        let task = get_runtime().spawn(async move {
            let mut retry_delay = Duration::from_secs(1);

            loop {
                tokio::select! {
                    () = cancellation_token.cancelled() => break,
                    result = run_ws_price_loop(
                        ws_url.clone(),
                        proxy_url.clone(),
                        funding_interval_s,
                        Arc::clone(&registry),
                        Arc::clone(&subscriptions),
                        sender.clone(),
                        cancellation_token.clone(),
                        clock,
                    ) => {
                        match result {
                            Ok(()) => break,
                            Err(error) => {
                                log::warn!("Variational price WebSocket disconnected: {error}");
                                tokio::select! {
                                    () = cancellation_token.cancelled() => break,
                                    () = tokio::time::sleep(retry_delay) => {}
                                }
                                retry_delay = (retry_delay * 2).min(Duration::from_secs(30));
                            }
                        }
                    }
                }
            }
        });
        self.tasks.push(task);
    }

    fn send_response(&self, response: DataResponse) {
        if let Err(error) = self.data_sender.send(DataEvent::Response(response)) {
            log::warn!("Failed to send Variational data response: {error}");
        }
    }
}

#[async_trait(?Send)]
impl DataClient for VariationalDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(venue())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        self.is_connected.store(false, Ordering::Release);
        self.cancellation_token = CancellationToken::new();
        for task in self.tasks.drain(..) {
            task.abort();
        }
        self.registry
            .try_write()
            .expect("instrument registry lock poisoned")
            .clear();
        *self
            .subscriptions
            .try_write()
            .expect("subscription lock poisoned") = VariationalSubscriptions::default();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
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

        self.cancellation_token = CancellationToken::new();
        let instruments = self.bootstrap_instruments().await?;
        for instrument in instruments {
            if let Err(error) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                log::warn!("Failed to publish Variational instrument: {error}");
            }
        }

        self.spawn_poll_loop();
        self.spawn_ws_price_loop();
        self.is_connected.store(true, Ordering::Release);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.is_disconnected() {
            return Ok(());
        }

        self.cancellation_token.cancel();
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
        self.reset()?;
        self.is_connected.store(false, Ordering::Release);
        Ok(())
    }

    fn request_instruments(&self, request: RequestInstruments) -> anyhow::Result<()> {
        let instruments = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .instruments();
        self.send_response(DataResponse::Instruments(InstrumentsResponse::new(
            request.request_id,
            request.client_id.unwrap_or(self.client_id),
            venue(),
            instruments,
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
            self.clock.get_time_ns(),
            request.params,
        )));
        Ok(())
    }

    fn request_instrument(&self, request: RequestInstrument) -> anyhow::Result<()> {
        let registry = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned");
        let instrument = registry
            .meta_for_instrument_id(&request.instrument_id)
            .map(|meta| meta.instrument.clone())
            .ok_or_else(|| anyhow::anyhow!("Instrument not found: {}", request.instrument_id))?;

        self.send_response(DataResponse::Instrument(Box::new(InstrumentResponse::new(
            request.request_id,
            request.client_id.unwrap_or(self.client_id),
            request.instrument_id,
            instrument,
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
            self.clock.get_time_ns(),
            request.params,
        ))));
        Ok(())
    }

    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let registry = Arc::clone(&self.registry);
        let sender = self.data_sender.clone();
        let quote_tier = self.config.quote_tier;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;

        get_runtime().spawn(async move {
            let quotes = request_current_quote(
                &http_client,
                &registry,
                &request.instrument_id,
                quote_tier,
                clock.get_time_ns(),
            )
            .await
            .into_iter()
            .collect();

            let response = DataResponse::Quotes(QuotesResponse::new(
                request.request_id,
                client_id,
                request.instrument_id,
                quotes,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                clock.get_time_ns(),
                request.params,
            ));
            let _ = sender.send(DataEvent::Response(response));
        });
        Ok(())
    }

    fn request_funding_rates(&self, request: RequestFundingRates) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let registry = Arc::clone(&self.registry);
        let sender = self.data_sender.clone();
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;

        get_runtime().spawn(async move {
            let updates = request_current_funding(
                &http_client,
                &registry,
                &request.instrument_id,
                clock.get_time_ns(),
            )
            .await
            .into_iter()
            .collect();

            let response = DataResponse::FundingRates(FundingRatesResponse::new(
                request.request_id,
                client_id,
                request.instrument_id,
                updates,
                datetime_to_unix_nanos(request.start),
                datetime_to_unix_nanos(request.end),
                clock.get_time_ns(),
                request.params,
            ));
            let _ = sender.send(DataEvent::Response(response));
        });
        Ok(())
    }

    fn subscribe_instrument(&mut self, subscription: SubscribeInstrument) -> anyhow::Result<()> {
        if let Some(meta) = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .cloned()
        {
            let _ = self
                .data_sender
                .send(DataEvent::Instrument(meta.instrument));
        }
        Ok(())
    }

    fn subscribe_instruments(&mut self, _subscription: SubscribeInstruments) -> anyhow::Result<()> {
        for instrument in self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .instruments()
        {
            let _ = self.data_sender.send(DataEvent::Instrument(instrument));
        }
        Ok(())
    }

    fn subscribe_quotes(&mut self, subscription: SubscribeQuotes) -> anyhow::Result<()> {
        self.subscriptions
            .try_write()
            .expect("subscription lock poisoned")
            .quotes
            .insert(subscription.instrument_id);
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, subscription: &UnsubscribeQuotes) -> anyhow::Result<()> {
        self.subscriptions
            .try_write()
            .expect("subscription lock poisoned")
            .quotes
            .remove(&subscription.instrument_id);
        Ok(())
    }

    fn subscribe_mark_prices(&mut self, subscription: SubscribeMarkPrices) -> anyhow::Result<()> {
        self.subscriptions
            .try_write()
            .expect("subscription lock poisoned")
            .marks
            .insert(subscription.instrument_id);
        Ok(())
    }

    fn unsubscribe_mark_prices(
        &mut self,
        subscription: &UnsubscribeMarkPrices,
    ) -> anyhow::Result<()> {
        self.subscriptions
            .try_write()
            .expect("subscription lock poisoned")
            .marks
            .remove(&subscription.instrument_id);
        Ok(())
    }

    fn subscribe_index_prices(&mut self, subscription: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscriptions
            .try_write()
            .expect("subscription lock poisoned")
            .index
            .insert(subscription.instrument_id);
        Ok(())
    }

    fn unsubscribe_index_prices(
        &mut self,
        subscription: &UnsubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        self.subscriptions
            .try_write()
            .expect("subscription lock poisoned")
            .index
            .remove(&subscription.instrument_id);
        Ok(())
    }

    fn subscribe_funding_rates(
        &mut self,
        subscription: SubscribeFundingRates,
    ) -> anyhow::Result<()> {
        self.subscriptions
            .try_write()
            .expect("subscription lock poisoned")
            .funding
            .insert(subscription.instrument_id);
        Ok(())
    }

    fn unsubscribe_funding_rates(
        &mut self,
        subscription: &UnsubscribeFundingRates,
    ) -> anyhow::Result<()> {
        self.subscriptions
            .try_write()
            .expect("subscription lock poisoned")
            .funding
            .remove(&subscription.instrument_id);
        Ok(())
    }
}

async fn request_current_quote(
    http_client: &VariationalHttpClient,
    registry: &Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    instrument_id: &InstrumentId,
    quote_tier: crate::config::VariationalQuoteTier,
    ts_init: UnixNanos,
) -> Option<nautilus_model::data::QuoteTick> {
    let stats = http_client.stats().await.ok()?;
    let registry = registry.read().await;
    let meta = registry.meta_for_instrument_id(instrument_id)?;
    let listing = listing_for_instrument(&stats, meta)?;
    quote_tick_from_listing(meta, listing, quote_tier, ts_init)
}

async fn request_current_funding(
    http_client: &VariationalHttpClient,
    registry: &Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    instrument_id: &InstrumentId,
    ts_init: UnixNanos,
) -> Option<nautilus_model::data::FundingRateUpdate> {
    let stats = http_client.stats().await.ok()?;
    let registry = registry.read().await;
    let meta = registry.meta_for_instrument_id(instrument_id)?;
    let listing = listing_for_instrument(&stats, meta)?;
    funding_rate_update_from_listing(meta, listing, ts_init)
}

async fn poll_once(
    http_client: &VariationalHttpClient,
    registry: &Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    subscriptions: &Arc<tokio::sync::RwLock<VariationalSubscriptions>>,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    quote_tier: crate::config::VariationalQuoteTier,
    clock: &'static nautilus_core::time::AtomicTime,
) -> anyhow::Result<()> {
    let subscriptions = subscriptions.read().await.clone();
    if subscriptions.is_rest_empty() {
        return Ok(());
    }

    let stats = http_client.stats().await?;
    let by_ticker = stats_by_ticker(&stats);
    let registry = registry.read().await;
    let ts_init = clock.get_time_ns();

    for instrument_id in &subscriptions.quotes {
        let Some((meta, listing)) = current_listing(&registry, &by_ticker, instrument_id) else {
            continue;
        };
        if let Some(quote) = quote_tick_from_listing(meta, listing, quote_tier, ts_init) {
            let _ = sender.send(DataEvent::Data(Data::Quote(quote)));
        }
    }

    for instrument_id in &subscriptions.funding {
        let Some((meta, listing)) = current_listing(&registry, &by_ticker, instrument_id) else {
            continue;
        };
        if let Some(funding) = funding_rate_update_from_listing(meta, listing, ts_init) {
            let _ = sender.send(DataEvent::FundingRate(funding));
        }
    }

    Ok(())
}

async fn run_ws_price_loop(
    ws_url: String,
    proxy_url: Option<String>,
    funding_interval_s: u64,
    registry: Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    subscriptions: Arc<tokio::sync::RwLock<VariationalSubscriptions>>,
    sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    cancellation_token: CancellationToken,
    clock: &'static nautilus_core::time::AtomicTime,
) -> anyhow::Result<()> {
    let (message_handler, mut raw_rx) = channel_message_handler();
    let ws_config = WebSocketConfig {
        url: ws_url,
        headers: vec![],
        heartbeat: None,
        heartbeat_msg: None,
        reconnect_timeout_ms: Some(15_000),
        reconnect_delay_initial_ms: Some(250),
        reconnect_delay_max_ms: Some(5_000),
        reconnect_backoff_factor: Some(2.0),
        reconnect_jitter_ms: Some(200),
        reconnect_max_attempts: None,
        idle_timeout_ms: Some(15_000),
        backend: TransportBackend::Tungstenite,
        proxy_url,
    };
    let ws_client =
        WebSocketClient::connect(ws_config, Some(message_handler), None, None, vec![], None)
            .await?;
    let mut sync_interval = tokio::time::interval(Duration::from_millis(500));
    sync_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut subscribed = AHashSet::<String>::default();

    loop {
        tokio::select! {
            () = cancellation_token.cancelled() => {
                ws_client.disconnect().await;
                return Ok(());
            }
            _ = sync_interval.tick() => {
                sync_ws_price_subscriptions(
                    &ws_client,
                    &registry,
                    &subscriptions,
                    &mut subscribed,
                    funding_interval_s,
                ).await?;
            }
            raw_msg = raw_rx.recv() => {
                let Some(raw_msg) = raw_msg else {
                    ws_client.disconnect().await;
                    anyhow::bail!("Variational price WebSocket message stream closed");
                };

                match raw_msg {
                    Message::Text(text) => {
                        handle_ws_price_text(
                            text.as_str(),
                            &registry,
                            &subscriptions,
                            &sender,
                            &mut subscribed,
                            clock,
                        ).await;
                    }
                    Message::Ping(payload) => {
                        if let Err(error) = ws_client.send_pong(payload.to_vec()).await {
                            log::warn!("Failed to send Variational price WebSocket pong: {error}");
                        }
                    }
                    Message::Close(frame) => {
                        anyhow::bail!("Variational price WebSocket closed: {frame:?}");
                    }
                    _ => {}
                }
            }
        }
    }
}

async fn sync_ws_price_subscriptions(
    ws_client: &WebSocketClient,
    registry: &Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    subscriptions: &Arc<tokio::sync::RwLock<VariationalSubscriptions>>,
    subscribed: &mut AHashSet<String>,
    funding_interval_s: u64,
) -> anyhow::Result<()> {
    let desired = desired_ws_tickers(registry, subscriptions).await;
    let mut to_subscribe = desired
        .difference(subscribed)
        .cloned()
        .collect::<Vec<String>>();
    let mut to_unsubscribe = subscribed
        .difference(&desired)
        .cloned()
        .collect::<Vec<String>>();
    to_subscribe.sort_unstable();
    to_unsubscribe.sort_unstable();

    if !to_subscribe.is_empty() {
        send_ws_subscription(ws_client, true, to_subscribe.clone(), funding_interval_s).await?;
        subscribed.extend(to_subscribe);
    }

    if !to_unsubscribe.is_empty() {
        send_ws_subscription(ws_client, false, to_unsubscribe.clone(), funding_interval_s).await?;
        for ticker in to_unsubscribe {
            subscribed.remove(&ticker);
        }
    }

    Ok(())
}

async fn desired_ws_tickers(
    registry: &Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    subscriptions: &Arc<tokio::sync::RwLock<VariationalSubscriptions>>,
) -> AHashSet<String> {
    let subscriptions = subscriptions.read().await.clone();
    let registry = registry.read().await;
    subscriptions
        .marks
        .iter()
        .chain(subscriptions.index.iter())
        .filter_map(|instrument_id| registry.meta_for_instrument_id(instrument_id))
        .map(|meta| meta.ticker.clone())
        .collect()
}

async fn send_ws_subscription(
    ws_client: &WebSocketClient,
    subscribe: bool,
    tickers: Vec<String>,
    funding_interval_s: u64,
) -> anyhow::Result<()> {
    let request = if subscribe {
        VariationalWsSubscriptionRequest::subscribe_tickers(tickers, funding_interval_s)
    } else {
        VariationalWsSubscriptionRequest::unsubscribe_tickers(tickers, funding_interval_s)
    };
    let payload = serde_json::to_string(&request)?;
    ws_client.send_text(payload, None).await?;
    Ok(())
}

async fn handle_ws_price_text(
    text: &str,
    registry: &Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    subscriptions: &Arc<tokio::sync::RwLock<VariationalSubscriptions>>,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    subscribed: &mut AHashSet<String>,
    clock: &'static nautilus_core::time::AtomicTime,
) {
    if text == RECONNECTED {
        subscribed.clear();
        return;
    }

    match serde_json::from_str::<VariationalWsMessage>(text) {
        Ok(VariationalWsMessage::Heartbeat(_)) => {}
        Ok(VariationalWsMessage::Price(message)) => {
            publish_ws_price_message(&message, registry, subscriptions, sender, clock).await;
        }
        Err(error) => {
            if text.starts_with("unsupported instrument") || text.contains("closing connection") {
                log::warn!("Variational price WebSocket notice: {text}");
            } else {
                log::debug!("Failed to parse Variational price WebSocket message: {error}; {text}");
            }
        }
    }
}

async fn publish_ws_price_message(
    message: &crate::websocket::messages::VariationalWsPriceMessage,
    registry: &Arc<tokio::sync::RwLock<VariationalInstrumentRegistry>>,
    subscriptions: &Arc<tokio::sync::RwLock<VariationalSubscriptions>>,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    clock: &'static nautilus_core::time::AtomicTime,
) {
    let Some(ticker) = message.ticker() else {
        log::debug!(
            "Ignoring Variational price WebSocket message with unknown channel: {}",
            message.channel
        );
        return;
    };

    let registry = registry.read().await;
    let Some(meta) = registry.meta_for_ticker(ticker) else {
        log::debug!("Ignoring Variational price for unknown ticker: {ticker}");
        return;
    };
    let instrument_id = meta.instrument.id();
    let subscriptions = subscriptions.read().await;
    let send_mark = subscriptions.marks.contains(&instrument_id);
    let send_index = subscriptions.index.contains(&instrument_id);
    let ts_init = clock.get_time_ns();

    if send_mark && let Some(mark) = mark_price_update_from_ws(meta, message, ts_init) {
        let _ = sender.send(DataEvent::Data(Data::MarkPriceUpdate(mark)));
    }

    if send_index && let Some(index) = index_price_update_from_ws(meta, message, ts_init) {
        let _ = sender.send(DataEvent::Data(Data::IndexPriceUpdate(index)));
    }
}

fn current_listing<'a>(
    registry: &'a VariationalInstrumentRegistry,
    by_ticker: &'a AHashMap<&str, &crate::models::VariationalListing>,
    instrument_id: &InstrumentId,
) -> Option<(
    &'a VariationalInstrumentMeta,
    &'a crate::models::VariationalListing,
)> {
    let meta = registry.meta_for_instrument_id(instrument_id)?;
    let listing = by_ticker.get(meta.ticker.as_str()).copied()?;
    Some((meta, listing))
}

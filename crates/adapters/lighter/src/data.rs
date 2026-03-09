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

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use ahash::AHashMap;
use anyhow::Context;
use async_trait::async_trait;
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent,
        data::{
            BarsResponse, BookResponse, DataResponse, FundingRatesResponse, InstrumentResponse,
            InstrumentsResponse, QuotesResponse, RequestBars, RequestBookSnapshot,
            RequestFundingRates, RequestInstrument, RequestInstruments, RequestQuotes,
            RequestTrades, SubscribeBars, SubscribeBookDeltas, SubscribeFundingRates,
            SubscribeIndexPrices, SubscribeInstrument, SubscribeInstruments, SubscribeMarkPrices,
            SubscribeQuotes, SubscribeTrades, TradesResponse, UnsubscribeBars,
            UnsubscribeBookDeltas, UnsubscribeFundingRates, UnsubscribeIndexPrices,
            UnsubscribeMarkPrices, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{UnixNanos, datetime::datetime_to_unix_nanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{Data, OrderBookDeltas_API},
    enums::BookType,
    identifiers::{ClientId, Venue},
    orderbook::OrderBook,
};
use serde_json::Value;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        LighterBookState, LighterInstrumentRegistry, LighterMarketStatUpdate, bar_granularity,
        candles_to_bars, channel_market_id, funding_rate_update_from_history,
        load_instrument_registry, market_stats_to_updates, order_book_delta_updates,
        order_book_snapshot_deltas, populate_order_book, quote_tick_from_ticker,
        trade_tick_from_trade, venue,
    },
    config::{Config, LighterDataClientConfig},
    http::client::LighterHttpClient,
    models::{
        market::PerpsMarketStats,
        trade::Trade,
        ws::{WsMessage, WsOrderBookMessage, WsTickerUpdate},
    },
    websocket::client::LighterWebSocketClient,
};

#[derive(Debug)]
pub struct LighterDataClient {
    client_id: ClientId,
    http_client: LighterHttpClient,
    ws_client: LighterWebSocketClient,
    is_connected: AtomicBool,
    cancellation_token: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    registry: Arc<tokio::sync::RwLock<LighterInstrumentRegistry>>,
    book_offsets: Arc<tokio::sync::RwLock<AHashMap<i64, u64>>>,
    book_states: Arc<tokio::sync::RwLock<AHashMap<i64, LighterBookState>>>,
    market_stats_refcount: usize,
    clock: &'static nautilus_core::time::AtomicTime,
}

impl LighterDataClient {
    pub fn new(client_id: ClientId, config: LighterDataClientConfig) -> anyhow::Result<Self> {
        let mut runtime_config = Config::for_network(config.is_testnet)
            .with_http_base_url(config.http_url())
            .with_ws_base_url(config.ws_url());

        if let Some(proxy) = &config.http_proxy_url {
            runtime_config = runtime_config.with_proxy(proxy.clone());
        }
        if let Some(timeout) = config.http_timeout_secs {
            runtime_config = runtime_config.with_timeout_secs(timeout);
        }

        let http_client = LighterHttpClient::new_public(runtime_config)?;
        let ws_client = LighterWebSocketClient::new(config.ws_url(), None);

        Ok(Self {
            client_id,
            http_client,
            ws_client,
            is_connected: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            tasks: Vec::new(),
            data_sender: get_data_event_sender(),
            registry: Arc::new(tokio::sync::RwLock::new(
                LighterInstrumentRegistry::default(),
            )),
            book_offsets: Arc::new(tokio::sync::RwLock::new(AHashMap::new())),
            book_states: Arc::new(tokio::sync::RwLock::new(AHashMap::new())),
            market_stats_refcount: 0,
            clock: get_atomic_clock_realtime(),
        })
    }

    async fn bootstrap_instruments(
        &self,
    ) -> anyhow::Result<Vec<nautilus_model::instruments::InstrumentAny>> {
        let registry = load_instrument_registry(&self.http_client)
            .await
            .context("failed to load Lighter instruments")?;
        let instruments = registry.instruments();
        *self.registry.write().await = registry;
        Ok(instruments)
    }

    async fn spawn_ws_loop(&mut self) -> anyhow::Result<()> {
        let ws_client = self.ws_client.clone();
        ws_client
            .connect()
            .await
            .context("failed to connect Lighter websocket")?;

        let cancellation_token = self.cancellation_token.clone();
        let sender = self.data_sender.clone();
        let registry = Arc::clone(&self.registry);
        let book_offsets = Arc::clone(&self.book_offsets);
        let book_states = Arc::clone(&self.book_states);
        let clock = self.clock;

        let handle = get_runtime().spawn(async move {
            loop {
                tokio::select! {
                    () = cancellation_token.cancelled() => break,
                    message = ws_client.next_message() => {
                        let Some(message) = message else {
                            break;
                        };
                        if let Err(error) = handle_ws_message(
                            &message,
                            &sender,
                            &registry,
                            &book_offsets,
                            &book_states,
                            clock,
                        ).await {
                            log::warn!("Failed to handle Lighter websocket message: {error}");
                        }
                    }
                }
            }
        });

        self.tasks.push(handle);
        Ok(())
    }

    fn send_response(&self, response: DataResponse) {
        if let Err(error) = self.data_sender.send(DataEvent::Response(response)) {
            log::warn!("Failed to send Lighter data response: {error}");
        }
    }
}

#[async_trait(?Send)]
impl DataClient for LighterDataClient {
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
        self.book_offsets
            .try_write()
            .expect("book offsets lock poisoned")
            .clear();
        self.book_states
            .try_write()
            .expect("book states lock poisoned")
            .clear();
        self.registry
            .try_write()
            .expect("instrument registry lock poisoned")
            .clear();
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

        let instruments = self.bootstrap_instruments().await?;
        for instrument in instruments {
            if let Err(error) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                log::warn!("Failed to publish Lighter instrument: {error}");
            }
        }

        self.spawn_ws_loop().await?;
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
        self.ws_client.close().await?;
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

    fn request_book_snapshot(&self, request: RequestBookSnapshot) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let registry = Arc::clone(&self.registry);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;

        get_runtime().spawn(async move {
            let registry = registry.read().await;
            let Some(meta) = registry
                .meta_for_instrument_id(&request.instrument_id)
                .cloned()
            else {
                return;
            };
            let depth = request.depth.map(|value| value.get() as u32).unwrap_or(100);
            match http_client
                .rest()
                .get_order_book_orders(meta.market_id, depth)
                .await
            {
                Ok(snapshot) => {
                    let ts_event = clock.get_time_ns();
                    let mut book = OrderBook::new(request.instrument_id, BookType::L2_MBP);
                    populate_order_book(
                        &mut book,
                        &meta.instrument,
                        &snapshot
                            .bids
                            .iter()
                            .map(|order| crate::models::order_book::PriceLevel {
                                price: order.price.clone(),
                                size: order.remaining_base_amount.clone(),
                            })
                            .collect::<Vec<_>>(),
                        &snapshot
                            .asks
                            .iter()
                            .map(|order| crate::models::order_book::PriceLevel {
                                price: order.price.clone(),
                                size: order.remaining_base_amount.clone(),
                            })
                            .collect::<Vec<_>>(),
                        snapshot.total_bids.unwrap_or_default() as u64,
                        ts_event,
                    );

                    let response = DataResponse::Book(BookResponse::new(
                        request.request_id,
                        client_id,
                        request.instrument_id,
                        book,
                        None,
                        None,
                        ts_event,
                        request.params,
                    ));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Failed to request Lighter order book snapshot: {error}"),
            }
        });

        Ok(())
    }

    fn request_quotes(&self, request: RequestQuotes) -> anyhow::Result<()> {
        self.send_response(DataResponse::Quotes(QuotesResponse::new(
            request.request_id,
            request.client_id.unwrap_or(self.client_id),
            request.instrument_id,
            Vec::new(),
            datetime_to_unix_nanos(request.start),
            datetime_to_unix_nanos(request.end),
            self.clock.get_time_ns(),
            request.params,
        )));
        Ok(())
    }

    fn request_trades(&self, request: RequestTrades) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let registry = Arc::clone(&self.registry);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;

        get_runtime().spawn(async move {
            let registry = registry.read().await;
            let Some(meta) = registry
                .meta_for_instrument_id(&request.instrument_id)
                .cloned()
            else {
                return;
            };
            let limit = request.limit.map(|value| value.get() as u32).unwrap_or(200);
            match http_client
                .rest()
                .get_recent_trades(meta.market_id, limit)
                .await
            {
                Ok(response) => {
                    let ts_init = clock.get_time_ns();
                    let trades = response
                        .trades
                        .iter()
                        .map(|trade| trade_tick_from_trade(&meta.instrument, trade, ts_init))
                        .collect();
                    let response = DataResponse::Trades(TradesResponse::new(
                        request.request_id,
                        client_id,
                        request.instrument_id,
                        trades,
                        datetime_to_unix_nanos(request.start),
                        datetime_to_unix_nanos(request.end),
                        ts_init,
                        request.params,
                    ));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Failed to request Lighter trades: {error}"),
            }
        });

        Ok(())
    }

    fn request_bars(&self, request: RequestBars) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let registry = Arc::clone(&self.registry);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;

        get_runtime().spawn(async move {
            let registry = registry.read().await;
            let Some(meta) = registry
                .meta_for_instrument_id(&request.bar_type.instrument_id())
                .cloned()
            else {
                return;
            };
            let Ok(granularity) = bar_granularity(request.bar_type) else {
                return;
            };
            match http_client
                .rest()
                .get_candles(meta.market_id, &granularity, None)
                .await
            {
                Ok(response) => {
                    match candles_to_bars(&meta.instrument, request.bar_type, &response.candles) {
                        Ok(bars) => {
                            let response = DataResponse::Bars(BarsResponse::new(
                                request.request_id,
                                client_id,
                                request.bar_type,
                                bars,
                                datetime_to_unix_nanos(request.start),
                                datetime_to_unix_nanos(request.end),
                                clock.get_time_ns(),
                                request.params,
                            ));
                            let _ = sender.send(DataEvent::Response(response));
                        }
                        Err(error) => log::warn!("Failed to parse Lighter bars: {error}"),
                    }
                }
                Err(error) => log::warn!("Failed to request Lighter bars: {error}"),
            }
        });

        Ok(())
    }

    fn request_funding_rates(&self, request: RequestFundingRates) -> anyhow::Result<()> {
        let http_client = self.http_client.clone();
        let sender = self.data_sender.clone();
        let registry = Arc::clone(&self.registry);
        let client_id = request.client_id.unwrap_or(self.client_id);
        let clock = self.clock;

        get_runtime().spawn(async move {
            let registry = registry.read().await;
            let Some(meta) = registry
                .meta_for_instrument_id(&request.instrument_id)
                .cloned()
            else {
                return;
            };
            if !meta.market_type.is_perp() {
                return;
            }
            match http_client
                .rest()
                .get_funding_rates(meta.market_id, None)
                .await
            {
                Ok(response) => {
                    let ts_init = clock.get_time_ns();
                    let updates = response
                        .funding_rates
                        .iter()
                        .filter_map(|item| {
                            funding_rate_update_from_history(&meta.instrument, item, ts_init)
                        })
                        .collect();
                    let response = DataResponse::FundingRates(FundingRatesResponse::new(
                        request.request_id,
                        client_id,
                        request.instrument_id,
                        updates,
                        datetime_to_unix_nanos(request.start),
                        datetime_to_unix_nanos(request.end),
                        ts_init,
                        request.params,
                    ));
                    let _ = sender.send(DataEvent::Response(response));
                }
                Err(error) => log::warn!("Failed to request Lighter funding rates: {error}"),
            }
        });

        Ok(())
    }

    fn subscribe_instrument(&mut self, subscription: &SubscribeInstrument) -> anyhow::Result<()> {
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

    fn subscribe_instruments(
        &mut self,
        _subscription: &SubscribeInstruments,
    ) -> anyhow::Result<()> {
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

    fn subscribe_book_deltas(&mut self, subscription: &SubscribeBookDeltas) -> anyhow::Result<()> {
        if subscription.book_type != BookType::L2_MBP {
            anyhow::bail!("Lighter only supports L2_MBP order book data");
        }
        if let Some(meta) = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .cloned()
        {
            let ws = self.ws_client.clone();
            get_runtime().spawn(async move {
                let _ = ws
                    .subscribe(format!("order_book/{}", meta.market_id), None)
                    .await;
            });
        }
        Ok(())
    }

    fn unsubscribe_book_deltas(
        &mut self,
        subscription: &UnsubscribeBookDeltas,
    ) -> anyhow::Result<()> {
        if let Some(meta) = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .cloned()
        {
            let ws = self.ws_client.clone();
            get_runtime().spawn(async move {
                let _ = ws
                    .unsubscribe(format!("order_book/{}", meta.market_id))
                    .await;
            });
        }
        Ok(())
    }

    fn subscribe_quotes(&mut self, subscription: &SubscribeQuotes) -> anyhow::Result<()> {
        if let Some(meta) = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .cloned()
        {
            let ws = self.ws_client.clone();
            get_runtime().spawn(async move {
                let _ = ws
                    .subscribe(format!("ticker/{}", meta.market_id), None)
                    .await;
            });
        }
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, subscription: &UnsubscribeQuotes) -> anyhow::Result<()> {
        if let Some(meta) = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .cloned()
        {
            let ws = self.ws_client.clone();
            get_runtime().spawn(async move {
                let _ = ws.unsubscribe(format!("ticker/{}", meta.market_id)).await;
            });
        }
        Ok(())
    }

    fn subscribe_trades(&mut self, subscription: &SubscribeTrades) -> anyhow::Result<()> {
        if let Some(meta) = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .cloned()
        {
            let ws = self.ws_client.clone();
            get_runtime().spawn(async move {
                let _ = ws
                    .subscribe(format!("trade/{}", meta.market_id), None)
                    .await;
            });
        }
        Ok(())
    }

    fn unsubscribe_trades(&mut self, subscription: &UnsubscribeTrades) -> anyhow::Result<()> {
        if let Some(meta) = self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .cloned()
        {
            let ws = self.ws_client.clone();
            get_runtime().spawn(async move {
                let _ = ws.unsubscribe(format!("trade/{}", meta.market_id)).await;
            });
        }
        Ok(())
    }

    fn subscribe_mark_prices(&mut self, subscription: &SubscribeMarkPrices) -> anyhow::Result<()> {
        if self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .is_some_and(|meta| meta.market_type.is_perp())
        {
            self.market_stats_refcount += 1;
            if self.market_stats_refcount == 1 {
                let ws = self.ws_client.clone();
                get_runtime().spawn(async move {
                    let _ = ws.subscribe("market_stats/all".to_string(), None).await;
                });
            }
        }
        Ok(())
    }

    fn unsubscribe_mark_prices(
        &mut self,
        subscription: &UnsubscribeMarkPrices,
    ) -> anyhow::Result<()> {
        if self
            .registry
            .try_read()
            .expect("instrument registry lock poisoned")
            .meta_for_instrument_id(&subscription.instrument_id)
            .is_some_and(|meta| meta.market_type.is_perp())
            && self.market_stats_refcount > 0
        {
            self.market_stats_refcount -= 1;
            if self.market_stats_refcount == 0 {
                let ws = self.ws_client.clone();
                get_runtime().spawn(async move {
                    let _ = ws.unsubscribe("market_stats/all".to_string()).await;
                });
            }
        }
        Ok(())
    }

    fn subscribe_index_prices(
        &mut self,
        subscription: &SubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        self.subscribe_mark_prices(&SubscribeMarkPrices::new(
            subscription.instrument_id,
            subscription.client_id,
            subscription.venue,
            subscription.command_id,
            subscription.ts_init,
            subscription.correlation_id,
            subscription.params.clone(),
        ))
    }

    fn unsubscribe_index_prices(
        &mut self,
        subscription: &UnsubscribeIndexPrices,
    ) -> anyhow::Result<()> {
        self.unsubscribe_mark_prices(&UnsubscribeMarkPrices::new(
            subscription.instrument_id,
            subscription.client_id,
            subscription.venue,
            subscription.command_id,
            subscription.ts_init,
            subscription.correlation_id,
            subscription.params.clone(),
        ))
    }

    fn subscribe_funding_rates(
        &mut self,
        subscription: &SubscribeFundingRates,
    ) -> anyhow::Result<()> {
        self.subscribe_mark_prices(&SubscribeMarkPrices::new(
            subscription.instrument_id,
            subscription.client_id,
            subscription.venue,
            subscription.command_id,
            subscription.ts_init,
            subscription.correlation_id,
            subscription.params.clone(),
        ))
    }

    fn unsubscribe_funding_rates(
        &mut self,
        subscription: &UnsubscribeFundingRates,
    ) -> anyhow::Result<()> {
        self.unsubscribe_mark_prices(&UnsubscribeMarkPrices::new(
            subscription.instrument_id,
            subscription.client_id,
            subscription.venue,
            subscription.command_id,
            subscription.ts_init,
            subscription.correlation_id,
            subscription.params.clone(),
        ))
    }

    fn subscribe_bars(&mut self, _subscription: &SubscribeBars) -> anyhow::Result<()> {
        Ok(())
    }

    fn unsubscribe_bars(&mut self, _subscription: &UnsubscribeBars) -> anyhow::Result<()> {
        Ok(())
    }
}

async fn handle_ws_message(
    message: &str,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    registry: &Arc<tokio::sync::RwLock<LighterInstrumentRegistry>>,
    book_offsets: &Arc<tokio::sync::RwLock<AHashMap<i64, u64>>>,
    book_states: &Arc<tokio::sync::RwLock<AHashMap<i64, LighterBookState>>>,
    clock: &'static nautilus_core::time::AtomicTime,
) -> anyhow::Result<()> {
    let header: WsMessage = serde_json::from_str(message)?;
    if matches!(header.msg_type.as_str(), "connected" | "ping" | "pong") {
        return Ok(());
    }

    match header.msg_type.as_str() {
        "subscribed/order_book" | "update/order_book" => {
            let payload: WsOrderBookMessage = serde_json::from_str(message)?;
            let Some(market_id) = channel_market_id(&payload.channel) else {
                return Ok(());
            };
            let registry = registry.read().await;
            let Some(meta) = registry.meta_for_market_id(market_id).cloned() else {
                return Ok(());
            };
            let sequence = payload.offset as u64;
            let ts_event = crate::common::epoch_to_unix_nanos(Some(payload.timestamp));
            let ts_init = if ts_event == UnixNanos::default() {
                clock.get_time_ns()
            } else {
                ts_event
            };

            if payload.msg_type.starts_with("subscribed/") {
                book_offsets.write().await.insert(market_id, sequence);
                book_states
                    .write()
                    .await
                    .entry(market_id)
                    .or_default()
                    .set_snapshot(&payload.order_book.bids, &payload.order_book.asks);
                let deltas = order_book_snapshot_deltas(
                    &meta.instrument,
                    &payload.order_book.bids,
                    &payload.order_book.asks,
                    sequence,
                    ts_init,
                    ts_init,
                );
                let _ = sender.send(DataEvent::Data(Data::Deltas(OrderBookDeltas_API::new(
                    deltas,
                ))));
                return Ok(());
            }

            let last_sequence = book_offsets
                .read()
                .await
                .get(&market_id)
                .copied()
                .unwrap_or_default();
            if sequence <= last_sequence {
                return Ok(());
            }

            book_offsets.write().await.insert(market_id, sequence);
            book_states
                .write()
                .await
                .entry(market_id)
                .or_default()
                .apply_delta(&payload.order_book.bids, &payload.order_book.asks);
            let deltas = order_book_delta_updates(
                &meta.instrument,
                &payload.order_book.bids,
                &payload.order_book.asks,
                sequence,
                ts_event,
                ts_init,
            );
            let _ = sender.send(DataEvent::Data(Data::Deltas(OrderBookDeltas_API::new(
                deltas,
            ))));
        }
        "subscribed/ticker" | "update/ticker" => {
            let payload: WsTickerUpdate = serde_json::from_str(message)?;
            let Some(market_id) = payload.channel.as_deref().and_then(channel_market_id) else {
                return Ok(());
            };
            let Some(ticker) = payload.ticker else {
                return Ok(());
            };
            let registry = registry.read().await;
            let Some(meta) = registry.meta_for_market_id(market_id) else {
                return Ok(());
            };
            let ts_init = clock.get_time_ns();
            if let Some(quote) = quote_tick_from_ticker(&meta.instrument, &ticker, ts_init, ts_init)
            {
                let _ = sender.send(DataEvent::Data(Data::Quote(quote)));
            }
        }
        "subscribed/trade" | "update/trade" => {
            let payload: serde_json::Value = serde_json::from_str(message)?;
            let Some(market_id) = payload
                .get("channel")
                .and_then(|value| value.as_str())
                .and_then(channel_market_id)
            else {
                return Ok(());
            };
            let registry = registry.read().await;
            let Some(meta) = registry.meta_for_market_id(market_id) else {
                return Ok(());
            };
            let ts_init = clock.get_time_ns();
            let trades = payload
                .get("trades")
                .and_then(|value| value.as_array().cloned())
                .or_else(|| payload.get("trade").map(|value| vec![value.clone()]))
                .unwrap_or_default();
            for trade in trades {
                if let Ok(trade) = serde_json::from_value::<Trade>(trade) {
                    let _ = sender.send(DataEvent::Data(Data::Trade(trade_tick_from_trade(
                        &meta.instrument,
                        &trade,
                        ts_init,
                    ))));
                }
            }
        }
        "update/market_stats" | "subscribed/market_stats" => {
            let payload: serde_json::Value = serde_json::from_str(message)?;
            let ts_init = clock.get_time_ns();
            let stats = payload
                .get("market_stats")
                .cloned()
                .or_else(|| payload.get("market").cloned())
                .unwrap_or(Value::Null);

            let mut markets = Vec::new();
            match stats {
                Value::Object(map) => {
                    if map.get("market_id").is_some() {
                        markets.push(serde_json::from_value::<PerpsMarketStats>(Value::Object(
                            map,
                        ))?);
                    } else {
                        for (market_key, entry) in map {
                            if let Value::Object(mut entry_map) = entry {
                                if entry_map.get("market_id").is_none() {
                                    entry_map
                                        .insert("market_id".to_string(), Value::from(market_key));
                                }
                                if let Ok(market) = serde_json::from_value::<PerpsMarketStats>(
                                    Value::Object(entry_map),
                                ) {
                                    markets.push(market);
                                }
                            }
                        }
                    }
                }
                _ => {}
            }

            let registry = registry.read().await;
            for market in markets {
                let Some(market_id) = market.market_id else {
                    continue;
                };
                let Some(meta) = registry.meta_for_market_id(market_id) else {
                    continue;
                };
                for update in market_stats_to_updates(&meta.instrument, &market, ts_init, ts_init) {
                    match update {
                        LighterMarketStatUpdate::Mark(update) => {
                            let _ = sender.send(DataEvent::Data(Data::MarkPriceUpdate(update)));
                        }
                        LighterMarketStatUpdate::Index(update) => {
                            let _ = sender.send(DataEvent::Data(Data::IndexPriceUpdate(update)));
                        }
                        LighterMarketStatUpdate::Funding(update) => {
                            let _ = sender.send(DataEvent::FundingRate(update));
                        }
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}

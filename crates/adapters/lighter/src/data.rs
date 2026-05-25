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
#[cfg(feature = "latency-probe")]
use nautilus_core::latency;
use nautilus_core::{UnixNanos, datetime::datetime_to_unix_nanos, time::get_atomic_clock_realtime};
use nautilus_model::{
    data::{Data, OrderBookDeltas_API},
    enums::BookType,
    identifiers::{ClientId, Venue},
    orderbook::OrderBook,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        LighterInstrumentRegistry, LighterMarketStatUpdate, bar_granularity, candles_to_bars,
        channel_market_id, funding_rate_updates_from_history, load_instrument_registry,
        market_stats_to_updates, order_book_delta_updates, order_book_snapshot_deltas,
        populate_order_book, quote_tick_from_ticker, trade_tick_from_trade, venue,
    },
    config::{Config, LighterDataClientConfig},
    http::client::LighterHttpClient,
    models::ws::{
        WsMarketStatsUpdate, WsMessage, WsOrderBookMessage, WsTickerUpdate, WsTradeUpdate,
    },
    normalize::timestamp::{epoch_to_unix_nanos, message_event_time, ticker_event_time},
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
    market_stats_refcount: usize,
    clock: &'static nautilus_core::time::AtomicTime,
}

impl LighterDataClient {
    pub fn new(client_id: ClientId, config: &LighterDataClientConfig) -> anyhow::Result<Self> {
        let mut runtime_config = Config::for_environment(config.environment)
            .with_http_base_url(config.http_url())
            .with_ws_base_url(config.ws_url());

        if let Some(proxy) = &config.proxy_url {
            runtime_config = runtime_config.with_proxy(proxy.clone());
        }
        runtime_config = runtime_config.with_timeout_secs(config.http_timeout_secs);

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

    async fn spawn_ws_loop(&self) -> anyhow::Result<()> {
        let ws_client = self.ws_client.clone();
        let cancellation_token = self.cancellation_token.clone();
        let sender = self.data_sender.clone();
        let registry = self.registry.read().await.clone();
        let clock = self.clock;
        let mut book_offsets = AHashMap::new();

        ws_client
            .connect_with_event_handler(move |event| {
                if cancellation_token.is_cancelled() {
                    return;
                }

                if let Err(error) = handle_ws_message(
                    &event.text,
                    &sender,
                    &registry,
                    &mut book_offsets,
                    clock,
                    #[cfg(feature = "latency-probe")]
                    event.received_ns,
                ) {
                    log::warn!("Failed to handle Lighter websocket message: {error}");
                }
            })
            .await
            .context("failed to connect Lighter websocket")?;
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
            let depth = request.depth.map_or(100, |value| value.get() as u32);
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
            let limit = request.limit.map_or(200, |value| value.get() as u32);
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
        let start = datetime_to_unix_nanos(request.start);
        let end = datetime_to_unix_nanos(request.end);
        let limit = request.limit.map(|value| value.get());
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
                    let updates = funding_rate_updates_from_history(
                        &meta.instrument,
                        &response.funding_rates,
                        meta.market_id,
                        start,
                        end,
                        limit,
                        ts_init,
                    );
                    let response = DataResponse::FundingRates(FundingRatesResponse::new(
                        request.request_id,
                        client_id,
                        request.instrument_id,
                        updates,
                        start,
                        end,
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

    fn subscribe_book_deltas(&mut self, subscription: SubscribeBookDeltas) -> anyhow::Result<()> {
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

    fn subscribe_quotes(&mut self, subscription: SubscribeQuotes) -> anyhow::Result<()> {
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

    fn subscribe_trades(&mut self, subscription: SubscribeTrades) -> anyhow::Result<()> {
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

    fn subscribe_mark_prices(&mut self, subscription: SubscribeMarkPrices) -> anyhow::Result<()> {
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

    fn subscribe_index_prices(&mut self, subscription: SubscribeIndexPrices) -> anyhow::Result<()> {
        self.subscribe_mark_prices(SubscribeMarkPrices::new(
            subscription.instrument_id,
            subscription.client_id,
            subscription.venue,
            subscription.command_id,
            subscription.ts_init,
            subscription.correlation_id,
            subscription.params,
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
        subscription: SubscribeFundingRates,
    ) -> anyhow::Result<()> {
        self.subscribe_mark_prices(SubscribeMarkPrices::new(
            subscription.instrument_id,
            subscription.client_id,
            subscription.venue,
            subscription.command_id,
            subscription.ts_init,
            subscription.correlation_id,
            subscription.params,
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

    fn subscribe_bars(&mut self, _subscription: SubscribeBars) -> anyhow::Result<()> {
        Ok(())
    }

    fn unsubscribe_bars(&mut self, _subscription: &UnsubscribeBars) -> anyhow::Result<()> {
        Ok(())
    }
}

fn handle_ws_message(
    message: &str,
    sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
    registry: &LighterInstrumentRegistry,
    book_offsets: &mut AHashMap<i64, u64>,
    clock: &'static nautilus_core::time::AtomicTime,
    #[cfg(feature = "latency-probe")] raw_received_ns: u64,
) -> anyhow::Result<()> {
    #[cfg(feature = "latency-probe")]
    {
        latency::record_duration(
            "lighter.adapter.raw_to_loop",
            raw_received_ns,
            latency::timestamp_ns(),
        );
    }

    #[cfg(feature = "latency-probe")]
    let header_parse_start_ns = latency::timestamp_ns();
    let header: WsMessage = serde_json::from_str(message)?;
    #[cfg(feature = "latency-probe")]
    latency::record_duration(
        "lighter.adapter.header_parse",
        header_parse_start_ns,
        latency::timestamp_ns(),
    );
    if matches!(header.msg_type.as_str(), "connected" | "ping" | "pong") {
        return Ok(());
    }

    match header.msg_type.as_str() {
        "subscribed/order_book" | "update/order_book" => {
            #[cfg(feature = "latency-probe")]
            let payload_parse_start_ns = latency::timestamp_ns();
            let payload: WsOrderBookMessage = serde_json::from_str(message)?;
            #[cfg(feature = "latency-probe")]
            latency::record_duration(
                "lighter.adapter.book_payload_parse",
                payload_parse_start_ns,
                latency::timestamp_ns(),
            );
            let Some(market_id) = channel_market_id(&payload.channel) else {
                return Ok(());
            };
            let Some(meta) = registry.meta_for_market_id(market_id) else {
                return Ok(());
            };
            let sequence = payload.offset as u64;
            let ts_init = adapter_ts_init(
                clock,
                #[cfg(feature = "latency-probe")]
                raw_received_ns,
            );
            let ts_event = {
                let ts_event = epoch_to_unix_nanos(Some(payload.timestamp));
                if ts_event == UnixNanos::default() {
                    ts_init
                } else {
                    ts_event
                }
            };

            if payload.msg_type.starts_with("subscribed/") {
                #[cfg(feature = "latency-probe")]
                let normalize_start_ns = latency::timestamp_ns();
                book_offsets.insert(market_id, sequence);
                let deltas = order_book_snapshot_deltas(
                    &meta.instrument,
                    &payload.order_book.bids,
                    &payload.order_book.asks,
                    sequence,
                    ts_event,
                    ts_init,
                );
                #[cfg(feature = "latency-probe")]
                latency::record_duration(
                    "lighter.adapter.book_snapshot_normalize",
                    normalize_start_ns,
                    latency::timestamp_ns(),
                );
                let _ = sender.send(DataEvent::Data(Data::Deltas(OrderBookDeltas_API::new(
                    deltas,
                ))));
                #[cfg(feature = "latency-probe")]
                record_after_send("lighter.adapter.book_snapshot_after_send", raw_received_ns);
                return Ok(());
            }

            let last_sequence = book_offsets.get(&market_id).copied().unwrap_or_default();
            if sequence <= last_sequence {
                return Ok(());
            }

            #[cfg(feature = "latency-probe")]
            let normalize_start_ns = latency::timestamp_ns();
            book_offsets.insert(market_id, sequence);
            let deltas = order_book_delta_updates(
                &meta.instrument,
                &payload.order_book.bids,
                &payload.order_book.asks,
                sequence,
                ts_event,
                ts_init,
            );
            #[cfg(feature = "latency-probe")]
            latency::record_duration(
                "lighter.adapter.book_delta_normalize",
                normalize_start_ns,
                latency::timestamp_ns(),
            );
            let _ = sender.send(DataEvent::Data(Data::Deltas(OrderBookDeltas_API::new(
                deltas,
            ))));
            #[cfg(feature = "latency-probe")]
            record_after_send("lighter.adapter.book_delta_after_send", raw_received_ns);
        }
        "subscribed/ticker" | "update/ticker" => {
            #[cfg(feature = "latency-probe")]
            let payload_parse_start_ns = latency::timestamp_ns();
            let payload: WsTickerUpdate = serde_json::from_str(message)?;
            #[cfg(feature = "latency-probe")]
            latency::record_duration(
                "lighter.adapter.ticker_payload_parse",
                payload_parse_start_ns,
                latency::timestamp_ns(),
            );
            let Some(market_id) = payload.channel.as_deref().and_then(channel_market_id) else {
                return Ok(());
            };
            let Some(ticker) = payload.ticker else {
                return Ok(());
            };
            let Some(meta) = registry.meta_for_market_id(market_id) else {
                return Ok(());
            };
            let ts_init = adapter_ts_init(
                clock,
                #[cfg(feature = "latency-probe")]
                raw_received_ns,
            );
            let ts_event = ticker_event_time(
                ticker.last_updated_at,
                payload.last_updated_at,
                payload.timestamp,
                ts_init,
            );
            #[cfg(feature = "latency-probe")]
            let normalize_start_ns = latency::timestamp_ns();
            if let Some(quote) =
                quote_tick_from_ticker(&meta.instrument, &ticker, ts_event, ts_init)
            {
                #[cfg(feature = "latency-probe")]
                latency::record_duration(
                    "lighter.adapter.ticker_normalize",
                    normalize_start_ns,
                    latency::timestamp_ns(),
                );
                let _ = sender.send(DataEvent::Data(Data::Quote(quote)));
                #[cfg(feature = "latency-probe")]
                record_after_send("lighter.adapter.ticker_after_send", raw_received_ns);
            }
        }
        "subscribed/trade" | "update/trade" => {
            #[cfg(feature = "latency-probe")]
            let payload_parse_start_ns = latency::timestamp_ns();
            let payload: WsTradeUpdate = serde_json::from_str(message)?;
            #[cfg(feature = "latency-probe")]
            latency::record_duration(
                "lighter.adapter.trade_payload_parse",
                payload_parse_start_ns,
                latency::timestamp_ns(),
            );
            let Some(market_id) = channel_market_id(&payload.channel) else {
                return Ok(());
            };
            let Some(meta) = registry.meta_for_market_id(market_id) else {
                return Ok(());
            };
            let ts_init = adapter_ts_init(
                clock,
                #[cfg(feature = "latency-probe")]
                raw_received_ns,
            );
            #[cfg(feature = "latency-probe")]
            let normalize_start_ns = latency::timestamp_ns();
            for trade in payload.trades {
                let _ = sender.send(DataEvent::Data(Data::Trade(trade_tick_from_trade(
                    &meta.instrument,
                    &trade,
                    ts_init,
                ))));
            }
            #[cfg(feature = "latency-probe")]
            {
                latency::record_duration(
                    "lighter.adapter.trade_normalize_send",
                    normalize_start_ns,
                    latency::timestamp_ns(),
                );
                record_after_send("lighter.adapter.trade_after_send", raw_received_ns);
            }
        }
        "update/market_stats" | "subscribed/market_stats" => {
            #[cfg(feature = "latency-probe")]
            let payload_parse_start_ns = latency::timestamp_ns();
            let payload: WsMarketStatsUpdate = serde_json::from_str(message)?;
            #[cfg(feature = "latency-probe")]
            latency::record_duration(
                "lighter.adapter.market_stats_payload_parse",
                payload_parse_start_ns,
                latency::timestamp_ns(),
            );
            let ts_init = adapter_ts_init(
                clock,
                #[cfg(feature = "latency-probe")]
                raw_received_ns,
            );
            let ts_event = message_event_time(payload.timestamp, ts_init);

            #[cfg(feature = "latency-probe")]
            let normalize_start_ns = latency::timestamp_ns();
            for market in payload.markets {
                let Some(market_id) = market.market_id else {
                    continue;
                };
                let Some(meta) = registry.meta_for_market_id(market_id) else {
                    continue;
                };
                for update in market_stats_to_updates(&meta.instrument, &market, ts_event, ts_init)
                {
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
            #[cfg(feature = "latency-probe")]
            {
                latency::record_duration(
                    "lighter.adapter.market_stats_normalize_send",
                    normalize_start_ns,
                    latency::timestamp_ns(),
                );
                record_after_send("lighter.adapter.market_stats_after_send", raw_received_ns);
            }
        }
        _ => {}
    }

    Ok(())
}

#[cfg(feature = "latency-probe")]
fn adapter_ts_init(
    clock: &'static nautilus_core::time::AtomicTime,
    raw_received_ns: u64,
) -> UnixNanos {
    if latency::enabled() {
        UnixNanos::from(raw_received_ns)
    } else {
        clock.get_time_ns()
    }
}

#[cfg(not(feature = "latency-probe"))]
fn adapter_ts_init(clock: &'static nautilus_core::time::AtomicTime) -> UnixNanos {
    clock.get_time_ns()
}

#[cfg(feature = "latency-probe")]
fn record_after_send(stage: &'static str, raw_received_ns: u64) {
    latency::record_duration(stage, raw_received_ns, latency::timestamp_ns());
}

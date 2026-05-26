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

mod common;

use std::{
    cell::RefCell,
    collections::VecDeque,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use nautilus_common::{
    cache::Cache,
    clients::ExecutionClient,
    live::runner::set_exec_event_sender,
    messages::{
        ExecutionEvent,
        execution::{
            BatchCancelOrders, CancelAllOrders, CancelOrder, GenerateFillReports,
            GenerateOrderStatusReports, GeneratePositionStatusReports, QueryAccount, SubmitOrder,
            SubmitOrderList,
        },
    },
    testing::wait_until_async,
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_lighter::{
    client::{LighterCancelOrderRequest, LighterModifyOrderRequest, LighterSubmitOrderRequest},
    config::LighterExecClientConfig,
    execution::{LighterExecutionApi, LighterExecutionClient},
    models::{
        account::{AccountPosition, DetailedAccount, DetailedAccounts},
        asset::Asset,
        order::{Order as LighterOrder, Orders},
        trade::{Trade, Trades},
        transaction::{RespSendTx, RespSendTxBatch},
    },
};
use nautilus_live::ExecutionClientCore;
use nautilus_model::{
    accounts::{AccountAny, MarginAccount},
    data::QuoteTick,
    enums::{
        AccountType, ContingencyType, OmsType, OrderSide, OrderType, PositionSideSpecified,
        TimeInForce,
    },
    events::{AccountState, OrderAccepted, OrderEventAny, OrderInitialized},
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, OrderListId, StrategyId, TraderId,
        VenueOrderId,
    },
    orders::{Order, OrderTestBuilder, list::OrderList},
    reports::{FillReport, PositionStatusReport},
    types::{AccountBalance, Money, Price, Quantity},
};
use rstest::rstest;

use crate::common::{
    TEST_ACCOUNT_INDEX, TEST_INSTRUMENT_ID, TEST_MARKET_ID, public_http_client, start_mock_server,
};

#[derive(Debug)]
struct MockExecutionApi {
    auth_token_calls: AtomicUsize,
    auth_token_deadlines: Mutex<Vec<i64>>,
    request_account_calls: AtomicUsize,
    account: Mutex<DetailedAccounts>,
    active_orders: Mutex<Orders>,
    inactive_orders: Mutex<VecDeque<Orders>>,
    trades: Mutex<VecDeque<Trades>>,
    submit_requests: Mutex<Vec<LighterSubmitOrderRequest>>,
    submit_error: Mutex<Option<String>>,
    submit_batch_requests: Mutex<Vec<Vec<LighterSubmitOrderRequest>>>,
    submit_batch_error: Mutex<Option<String>>,
    modify_requests: Mutex<Vec<LighterModifyOrderRequest>>,
    modify_error: Mutex<Option<String>>,
    cancel_requests: Mutex<Vec<(i32, i64, Option<u8>)>>,
    cancel_error: Mutex<Option<String>>,
    cancel_batch_requests: Mutex<Vec<Vec<LighterCancelOrderRequest>>>,
    cancel_batch_error: Mutex<Option<String>>,
    cancel_all_calls: AtomicUsize,
}

impl Default for MockExecutionApi {
    fn default() -> Self {
        Self {
            auth_token_calls: AtomicUsize::new(0),
            auth_token_deadlines: Mutex::new(Vec::new()),
            request_account_calls: AtomicUsize::new(0),
            account: Mutex::new(detailed_accounts_with_position()),
            active_orders: Mutex::new(Orders {
                code: 200,
                message: None,
                orders: vec![sample_order(101, "O-OPEN", "open", "0.0000")],
                cursor: None,
            }),
            inactive_orders: Mutex::new(VecDeque::new()),
            trades: Mutex::new(VecDeque::new()),
            submit_requests: Mutex::new(Vec::new()),
            submit_error: Mutex::new(None),
            submit_batch_requests: Mutex::new(Vec::new()),
            submit_batch_error: Mutex::new(None),
            modify_requests: Mutex::new(Vec::new()),
            modify_error: Mutex::new(None),
            cancel_requests: Mutex::new(Vec::new()),
            cancel_error: Mutex::new(None),
            cancel_batch_requests: Mutex::new(Vec::new()),
            cancel_batch_error: Mutex::new(None),
            cancel_all_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl LighterExecutionApi for MockExecutionApi {
    async fn create_auth_token(
        &self,
        deadline_unix_secs: i64,
        _api_key_index: Option<u8>,
    ) -> anyhow::Result<String> {
        self.auth_token_calls.fetch_add(1, Ordering::Relaxed);
        self.auth_token_deadlines
            .lock()
            .unwrap()
            .push(deadline_unix_secs);
        Ok("test-auth-token".to_string())
    }

    async fn request_account(
        &self,
        _account_index: i64,
        _auth_token: &str,
    ) -> anyhow::Result<DetailedAccounts> {
        self.request_account_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.account.lock().unwrap().clone())
    }

    async fn request_account_active_orders(
        &self,
        _account_index: i64,
        _market_id: i64,
        _auth_token: &str,
    ) -> anyhow::Result<Orders> {
        Ok(self.active_orders.lock().unwrap().clone())
    }

    async fn request_account_inactive_orders(
        &self,
        _account_index: i64,
        _market_id: i64,
        _auth_token: &str,
        _cursor: Option<&str>,
    ) -> anyhow::Result<Orders> {
        Ok(self
            .inactive_orders
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(empty_orders))
    }

    async fn request_account_trades(
        &self,
        _account_index: i64,
        _auth_token: &str,
        _limit: u32,
        _cursor: Option<&str>,
    ) -> anyhow::Result<Trades> {
        Ok(self
            .trades
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(empty_trades))
    }

    async fn submit_order(&self, request: LighterSubmitOrderRequest) -> anyhow::Result<RespSendTx> {
        self.submit_requests.lock().unwrap().push(request);
        if let Some(error) = self.submit_error.lock().unwrap().clone() {
            return Err(anyhow::anyhow!(error));
        }
        Ok(ok_response())
    }

    async fn submit_order_batch(
        &self,
        requests: Vec<LighterSubmitOrderRequest>,
    ) -> anyhow::Result<RespSendTxBatch> {
        self.submit_batch_requests.lock().unwrap().push(requests);
        if let Some(error) = self.submit_batch_error.lock().unwrap().clone() {
            return Err(anyhow::anyhow!(error));
        }
        Ok(ok_batch_response())
    }

    async fn modify_order(&self, request: LighterModifyOrderRequest) -> anyhow::Result<RespSendTx> {
        self.modify_requests.lock().unwrap().push(request);
        if let Some(error) = self.modify_error.lock().unwrap().clone() {
            return Err(anyhow::anyhow!(error));
        }
        Ok(ok_response())
    }

    async fn cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        api_key_index: Option<u8>,
    ) -> anyhow::Result<RespSendTx> {
        self.cancel_requests
            .lock()
            .unwrap()
            .push((market_index, order_index, api_key_index));
        if let Some(error) = self.cancel_error.lock().unwrap().clone() {
            return Err(anyhow::anyhow!(error));
        }
        Ok(ok_response())
    }

    async fn cancel_order_batch(
        &self,
        requests: Vec<LighterCancelOrderRequest>,
    ) -> anyhow::Result<RespSendTxBatch> {
        self.cancel_batch_requests.lock().unwrap().push(requests);
        if let Some(error) = self.cancel_batch_error.lock().unwrap().clone() {
            return Err(anyhow::anyhow!(error));
        }
        Ok(ok_batch_response())
    }

    async fn cancel_all_orders(
        &self,
        _time_in_force: i32,
        _timestamp_ms: i64,
        _api_key_index: Option<u8>,
    ) -> anyhow::Result<RespSendTx> {
        self.cancel_all_calls.fetch_add(1, Ordering::Relaxed);
        Ok(ok_response())
    }
}

fn ok_response() -> RespSendTx {
    RespSendTx {
        code: 200,
        message: None,
        tx_hash: Some("0xabc".to_string()),
        predicted_execution_time_ms: None,
        volume_quota_remaining: None,
    }
}

fn ok_batch_response() -> RespSendTxBatch {
    RespSendTxBatch {
        code: 200,
        message: None,
        tx_hash: Some(vec!["0xabc".to_string()]),
        predicted_execution_time_ms: None,
        volume_quota_remaining: None,
    }
}

fn empty_orders() -> Orders {
    Orders {
        code: 200,
        message: None,
        orders: Vec::new(),
        cursor: None,
    }
}

fn empty_trades() -> Trades {
    Trades {
        code: 200,
        message: None,
        trades: Vec::new(),
        cursor: None,
    }
}

fn detailed_accounts_with_position() -> DetailedAccounts {
    DetailedAccounts {
        code: 200,
        message: None,
        accounts: vec![DetailedAccount {
            account_index: Some(TEST_ACCOUNT_INDEX),
            account_type: Some(1),
            l1_address: Some("0xtest".to_string()),
            available_balance: Some(100000.0),
            positions: Some(vec![AccountPosition {
                market_id: TEST_MARKET_ID,
                symbol: "BTC-USDC".to_string(),
                initial_margin_fraction: "500".to_string(),
                open_order_count: 1,
                pending_order_count: 0,
                position_tied_order_count: 0,
                sign: 1,
                position: "0.5000".to_string(),
                avg_entry_price: "100000.00".to_string(),
                position_value: "50000.00".to_string(),
                unrealized_pnl: "10.00".to_string(),
                realized_pnl: "5.00".to_string(),
                liquidation_price: "80000.00".to_string(),
                total_funding_paid_out: None,
                margin_mode: 0,
                allocated_margin: "1000.00".to_string(),
            }]),
            assets: Some(vec![Asset {
                symbol: "USDC".to_string(),
                asset_id: 2,
                balance: Some("100000.00".to_string()),
                locked_balance: Some("10.00".to_string()),
                margin_balance: None,
                extra: serde_json::Map::default(),
            }]),
        }],
    }
}

fn sample_order(
    order_index: i64,
    client_order_id: &str,
    status: &str,
    filled_qty: &str,
) -> LighterOrder {
    LighterOrder {
        order_index,
        client_order_index: 0,
        order_id: order_index.to_string(),
        client_order_id: client_order_id.to_string(),
        market_index: TEST_MARKET_ID,
        owner_account_index: TEST_ACCOUNT_INDEX,
        initial_base_amount: "0.1000".to_string(),
        price: "100000.00".to_string(),
        nonce: 0,
        remaining_base_amount: if status == "filled" {
            "0.0000".to_string()
        } else {
            "0.1000".to_string()
        },
        is_ask: false,
        base_size: 0,
        base_price: 0,
        filled_base_amount: filled_qty.to_string(),
        filled_quote_amount: "10000.00".to_string(),
        side: "buy".to_string(),
        order_type: "limit".to_string(),
        time_in_force: "gtc".to_string(),
        reduce_only: false,
        trigger_price: "0".to_string(),
        order_expiry: 1_704_153_600_000,
        status: status.to_string(),
        trigger_status: String::new(),
        trigger_time: 0,
        parent_order_index: 0,
        parent_order_id: String::new(),
        to_trigger_order_id_0: String::new(),
        to_trigger_order_id_1: String::new(),
        to_cancel_order_id_0: String::new(),
        block_height: 0,
        timestamp: 1_704_067_200_000,
        created_at: 1_704_067_200_000,
        updated_at: 1_704_067_205_000,
        transaction_time: 1_704_067_205_000,
    }
}

fn sample_trade() -> Trade {
    Trade {
        trade_id: 9001,
        tx_hash: String::new(),
        trade_type: String::new(),
        market_id: TEST_MARKET_ID,
        size: "0.1000".to_string(),
        price: "100010.00".to_string(),
        usd_amount: String::new(),
        ask_id: 0,
        bid_id: 101,
        ask_client_id: None,
        bid_client_id: None,
        ask_account_id: 99,
        bid_account_id: TEST_ACCOUNT_INDEX,
        is_maker_ask: true,
        block_height: 0,
        timestamp: 1_704_067_260_000,
        taker_fee: Some(200),
        taker_position_size_before: None,
        taker_entry_quote_before: None,
        taker_initial_margin_fraction_before: None,
        taker_position_sign_changed: None,
        maker_fee: Some(100),
        maker_position_size_before: None,
        maker_entry_quote_before: None,
        maker_initial_margin_fraction_before: None,
        maker_position_sign_changed: None,
        transaction_time: 0,
        ask_account_pnl: None,
        bid_account_pnl: None,
    }
}

fn add_test_account(cache: &Rc<RefCell<Cache>>, account_id: AccountId) {
    let account_state = AccountState::new(
        account_id,
        AccountType::Margin,
        vec![AccountBalance::new(
            Money::from("10000.0 USDC"),
            Money::from("0 USDC"),
            Money::from("10000.0 USDC"),
        )],
        vec![],
        true,
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        None,
    );
    let account = AccountAny::Margin(MarginAccount::new(account_state, true));
    cache.borrow_mut().add_account(account).unwrap();
}

fn add_limit_order(
    cache: &Rc<RefCell<Cache>>,
    client_order_id: &str,
) -> nautilus_model::orders::any::OrderAny {
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(InstrumentId::from(TEST_INSTRUMENT_ID))
        .client_order_id(ClientOrderId::from(client_order_id))
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(nautilus_model::identifiers::StrategyId::from("S-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.1000"))
        .price(Price::from("100000.00"))
        .time_in_force(TimeInForce::Gtc)
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(ClientId::from("LIGHTER")), false)
        .unwrap();
    order
}

fn add_post_only_limit_order(
    cache: &Rc<RefCell<Cache>>,
    client_order_id: &str,
) -> nautilus_model::orders::any::OrderAny {
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(InstrumentId::from(TEST_INSTRUMENT_ID))
        .client_order_id(ClientOrderId::from(client_order_id))
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(nautilus_model::identifiers::StrategyId::from("S-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.1000"))
        .price(Price::from("100000.00"))
        .time_in_force(TimeInForce::Gtc)
        .post_only(true)
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(ClientId::from("LIGHTER")), false)
        .unwrap();
    order
}

fn add_market_order(
    cache: &Rc<RefCell<Cache>>,
    client_order_id: &str,
) -> nautilus_model::orders::any::OrderAny {
    let order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(InstrumentId::from(TEST_INSTRUMENT_ID))
        .client_order_id(ClientOrderId::from(client_order_id))
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(nautilus_model::identifiers::StrategyId::from("S-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.1000"))
        .time_in_force(TimeInForce::Ioc)
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(ClientId::from("LIGHTER")), false)
        .unwrap();
    order
}

fn add_quote(cache: &Rc<RefCell<Cache>>, bid: &str, ask: &str) {
    cache
        .borrow_mut()
        .add_quote(QuoteTick::new(
            InstrumentId::from(TEST_INSTRUMENT_ID),
            Price::from(bid),
            Price::from(ask),
            Quantity::from("1.0000"),
            Quantity::from("1.0000"),
            UnixNanos::default(),
            UnixNanos::default(),
        ))
        .unwrap();
}

fn add_contingent_limit_order(
    cache: &Rc<RefCell<Cache>>,
    client_order_id: &str,
    order_list_id: OrderListId,
    linked_order_ids: Vec<ClientOrderId>,
) -> nautilus_model::orders::any::OrderAny {
    let order = OrderTestBuilder::new(OrderType::Limit)
        .instrument_id(InstrumentId::from(TEST_INSTRUMENT_ID))
        .client_order_id(ClientOrderId::from(client_order_id))
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(nautilus_model::identifiers::StrategyId::from("S-001"))
        .side(OrderSide::Buy)
        .quantity(Quantity::from("0.1000"))
        .price(Price::from("100000.00"))
        .time_in_force(TimeInForce::Gtc)
        .contingency_type(ContingencyType::Oco)
        .order_list_id(order_list_id)
        .linked_order_ids(linked_order_ids)
        .build();
    cache
        .borrow_mut()
        .add_order(order.clone(), None, Some(ClientId::from("LIGHTER")), false)
        .unwrap();
    order
}

fn order_accepted(
    order: &nautilus_model::orders::any::OrderAny,
    venue_order_id: &str,
) -> OrderAccepted {
    OrderAccepted::new(
        order.trader_id(),
        order.strategy_id(),
        order.instrument_id(),
        order.client_order_id(),
        VenueOrderId::from(venue_order_id),
        AccountId::from("LIGHTER-7"),
        UUID4::new(),
        UnixNanos::default(),
        UnixNanos::default(),
        false,
    )
}

fn create_test_execution_client(
    addr: std::net::SocketAddr,
    api: Arc<MockExecutionApi>,
) -> (
    LighterExecutionClient,
    tokio::sync::mpsc::UnboundedReceiver<ExecutionEvent>,
    Rc<RefCell<Cache>>,
) {
    let trader_id = TraderId::from("TRADER-001");
    let account_id = AccountId::from("LIGHTER-7");
    let client_id = ClientId::from("LIGHTER");
    let cache = Rc::new(RefCell::new(Cache::default()));

    let core = ExecutionClientCore::new(
        trader_id,
        client_id,
        nautilus_model::identifiers::Venue::from("LIGHTER"),
        OmsType::Netting,
        account_id,
        AccountType::Margin,
        None,
        cache.clone(),
    );

    let config = LighterExecClientConfig {
        account_index: Some(TEST_ACCOUNT_INDEX),
        api_key_index: Some(3),
        maker_api_key_index: Some(4),
        base_url_http: Some(format!("http://{addr}")),
        base_url_ws: Some(format!("ws://{addr}/stream")),
        ..LighterExecClientConfig::default()
    };

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    set_exec_event_sender(tx);

    let client =
        LighterExecutionClient::new_with_api(core, config, public_http_client(addr), api).unwrap();

    (client, rx, cache)
}

#[rstest]
#[tokio::test]
async fn test_exec_client_creation() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (client, _rx, _cache) = create_test_execution_client(addr, api);

    assert_eq!(client.client_id(), ClientId::from("LIGHTER"));
    assert_eq!(client.account_id(), AccountId::from("LIGHTER-7"));
    assert!(!client.is_connected());
}

#[rstest]
#[tokio::test]
async fn test_exec_client_connect_disconnect() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));

    client.connect().await.unwrap();
    assert!(client.is_connected());
    assert!(api.auth_token_calls.load(Ordering::Relaxed) >= 1);
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    {
        let deadlines = api.auth_token_deadlines.lock().unwrap();
        assert!(deadlines.iter().all(|deadline| *deadline > now_unix + 200));
    }
    assert!(api.request_account_calls.load(Ordering::Relaxed) >= 1);

    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
}

#[rstest]
#[tokio::test]
async fn test_submit_order_records_request() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();

    let order = add_limit_order(&cache, "O-100");
    let cmd = SubmitOrder::from_order(
        &order,
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        None,
        UUID4::new(),
        UnixNanos::default(),
    );

    client.submit_order(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.submit_requests.lock().unwrap().len() == 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let request = api.submit_requests.lock().unwrap()[0];
    assert_eq!(request.market_index, TEST_MARKET_ID as i32);
    assert_eq!(request.api_key_index, Some(3));
    assert_eq!(request.base_amount, 1000);
    assert_eq!(request.price, 10_000_000);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_submit_order_rejects_on_async_api_failure() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    *api.submit_error.lock().unwrap() = Some("network unavailable".to_string());
    let (mut client, mut rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.start().unwrap();
    client.connect().await.unwrap();
    while rx.try_recv().is_ok() {}

    let order = add_limit_order(&cache, "O-ASYNC-FAIL");
    let cmd = SubmitOrder::from_order(
        &order,
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        None,
        UUID4::new(),
        UnixNanos::default(),
    );

    client.submit_order(cmd).unwrap();

    let mut saw_submitted = false;
    let mut rejection_reason = None;
    for _ in 0..3 {
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        match event {
            ExecutionEvent::Order(OrderEventAny::Submitted(submitted))
                if submitted.client_order_id == order.client_order_id() =>
            {
                saw_submitted = true;
            }
            ExecutionEvent::Order(OrderEventAny::Rejected(rejected))
                if rejected.client_order_id == order.client_order_id() =>
            {
                rejection_reason = Some(rejected.reason.to_string());
                break;
            }
            _ => {}
        }
    }

    assert!(saw_submitted);
    assert_eq!(
        rejection_reason.as_deref(),
        Some("Lighter submission failed: network unavailable")
    );

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_submit_market_order_uses_cached_quote() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    add_quote(&cache, "99999.00", "100001.00");
    client.connect().await.unwrap();

    let order = add_market_order(&cache, "O-MARKET-100");
    let cmd = SubmitOrder::from_order(
        &order,
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        None,
        UUID4::new(),
        UnixNanos::default(),
    );

    client.submit_order(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.submit_requests.lock().unwrap().len() == 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let request = api.submit_requests.lock().unwrap()[0];
    assert_eq!(request.price, 10_100_101);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_submit_post_only_order_uses_maker_api_key() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();

    let order = add_post_only_limit_order(&cache, "O-MAKER-100");
    let cmd = SubmitOrder::from_order(
        &order,
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        None,
        UUID4::new(),
        UnixNanos::default(),
    );

    client.submit_order(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.submit_requests.lock().unwrap().len() == 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let request = api.submit_requests.lock().unwrap()[0];
    assert_eq!(request.api_key_index, Some(4));
    assert_eq!(request.time_in_force, 2);

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_submit_order_list_records_batch_request() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();

    let order_1 = add_limit_order(&cache, "O-201");
    let order_2 = add_limit_order(&cache, "O-202");
    let order_list = OrderList::new(
        OrderListId::from("OL-001"),
        InstrumentId::from(TEST_INSTRUMENT_ID),
        StrategyId::from("S-001"),
        vec![order_1.client_order_id(), order_2.client_order_id()],
        UnixNanos::default(),
    );
    let cmd = SubmitOrderList::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        StrategyId::from("S-001"),
        order_list,
        vec![
            OrderInitialized::from(&order_1),
            OrderInitialized::from(&order_2),
        ],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    );

    client.submit_order_list(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.submit_batch_requests.lock().unwrap().len() == 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    {
        let requests = api.submit_batch_requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].len(), 2);
        assert!(api.submit_requests.lock().unwrap().is_empty());
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_submit_order_list_selects_key_per_order() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();

    let order_1 = add_limit_order(&cache, "O-TAKER-201");
    let order_2 = add_post_only_limit_order(&cache, "O-MAKER-202");
    let order_list = OrderList::new(
        OrderListId::from("OL-KEYS"),
        InstrumentId::from(TEST_INSTRUMENT_ID),
        StrategyId::from("S-001"),
        vec![order_1.client_order_id(), order_2.client_order_id()],
        UnixNanos::default(),
    );
    let cmd = SubmitOrderList::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        StrategyId::from("S-001"),
        order_list,
        vec![
            OrderInitialized::from(&order_1),
            OrderInitialized::from(&order_2),
        ],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    );

    client.submit_order_list(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.submit_batch_requests.lock().unwrap().len() == 2 }
        },
        Duration::from_secs(5),
    )
    .await;

    {
        let requests = api.submit_batch_requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].len(), 1);
        assert_eq!(requests[1].len(), 1);
        assert_eq!(requests[0][0].api_key_index, Some(3));
        assert_eq!(requests[1][0].api_key_index, Some(4));
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_submit_order_list_denies_contingent_orders() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, mut rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.start().unwrap();
    client.connect().await.unwrap();
    while rx.try_recv().is_ok() {}

    let order_list_id = OrderListId::from("OL-CONTINGENT");
    let order_1 = add_contingent_limit_order(
        &cache,
        "O-CONT-1",
        order_list_id,
        vec![ClientOrderId::from("O-CONT-2")],
    );
    let order_2 = add_contingent_limit_order(
        &cache,
        "O-CONT-2",
        order_list_id,
        vec![ClientOrderId::from("O-CONT-1")],
    );
    let cmd = SubmitOrderList::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        StrategyId::from("S-001"),
        OrderList::new(
            order_list_id,
            InstrumentId::from(TEST_INSTRUMENT_ID),
            StrategyId::from("S-001"),
            vec![order_1.client_order_id(), order_2.client_order_id()],
            UnixNanos::default(),
        ),
        vec![
            OrderInitialized::from(&order_1),
            OrderInitialized::from(&order_2),
        ],
        None,
        None,
        None,
        UUID4::new(),
        UnixNanos::default(),
    );

    client.submit_order_list(cmd).unwrap();

    let mut denied_ids = Vec::new();
    for _ in 0..2 {
        let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap();
        if let ExecutionEvent::Order(OrderEventAny::Denied(denied)) = event {
            denied_ids.push(denied.client_order_id);
        }
    }

    assert_eq!(denied_ids.len(), 2);
    assert!(denied_ids.contains(&order_1.client_order_id()));
    assert!(denied_ids.contains(&order_2.client_order_id()));
    assert!(api.submit_batch_requests.lock().unwrap().is_empty());

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_modify_order_records_request() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();
    let _order = add_limit_order(&cache, "O-101");

    let cmd = nautilus_common::messages::execution::ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        nautilus_model::identifiers::StrategyId::from("S-001"),
        InstrumentId::from(TEST_INSTRUMENT_ID),
        ClientOrderId::from("O-101"),
        Some(VenueOrderId::from("101")),
        Some(Quantity::from("0.2000")),
        Some(Price::from("100100.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.modify_order(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.modify_requests.lock().unwrap().len() == 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let request = api.modify_requests.lock().unwrap()[0];
    assert_eq!(request.market_index, TEST_MARKET_ID as i32);
    assert_eq!(request.order_index, 101);
    assert_eq!(request.base_amount, 2000);
    assert_eq!(request.api_key_index, Some(3));

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_modify_post_only_order_uses_maker_api_key() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();
    let _order = add_post_only_limit_order(&cache, "O-MAKER-MODIFY");

    let cmd = nautilus_common::messages::execution::ModifyOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        nautilus_model::identifiers::StrategyId::from("S-001"),
        InstrumentId::from(TEST_INSTRUMENT_ID),
        ClientOrderId::from("O-MAKER-MODIFY"),
        Some(VenueOrderId::from("101")),
        Some(Quantity::from("0.2000")),
        Some(Price::from("100100.00")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.modify_order(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.modify_requests.lock().unwrap().len() == 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let request = api.modify_requests.lock().unwrap()[0];
    assert_eq!(request.api_key_index, Some(4));

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_cancel_order_records_request() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();
    let _order = add_limit_order(&cache, "O-102");

    let cmd = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        nautilus_model::identifiers::StrategyId::from("S-001"),
        InstrumentId::from(TEST_INSTRUMENT_ID),
        ClientOrderId::from("O-102"),
        Some(VenueOrderId::from("101")),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.cancel_order(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.cancel_requests.lock().unwrap().len() == 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let request = api.cancel_requests.lock().unwrap()[0];
    assert_eq!(request, (TEST_MARKET_ID as i32, 101, Some(3)));

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_cancel_post_only_order_uses_maker_api_key() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();
    let _order = add_post_only_limit_order(&cache, "O-MAKER-CANCEL");

    let cmd = CancelOrder::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        nautilus_model::identifiers::StrategyId::from("S-001"),
        InstrumentId::from(TEST_INSTRUMENT_ID),
        ClientOrderId::from("O-MAKER-CANCEL"),
        Some(VenueOrderId::from("101")),
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.cancel_order(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.cancel_requests.lock().unwrap().len() == 1 }
        },
        Duration::from_secs(5),
    )
    .await;

    let request = api.cancel_requests.lock().unwrap()[0];
    assert_eq!(request, (TEST_MARKET_ID as i32, 101, Some(4)));

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_cancel_all_orders_uses_batch_cancel_requests() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();

    let mut order_1 = add_limit_order(&cache, "O-301");
    order_1
        .apply(OrderEventAny::Accepted(order_accepted(&order_1, "301")))
        .unwrap();
    cache.borrow_mut().update_order(&order_1).unwrap();

    let mut order_2 = add_post_only_limit_order(&cache, "O-302");
    order_2
        .apply(OrderEventAny::Accepted(order_accepted(&order_2, "302")))
        .unwrap();
    cache.borrow_mut().update_order(&order_2).unwrap();

    let cmd = CancelAllOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        StrategyId::from("S-001"),
        InstrumentId::from(TEST_INSTRUMENT_ID),
        OrderSide::Buy,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.cancel_all_orders(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.cancel_batch_requests.lock().unwrap().len() == 2 }
        },
        Duration::from_secs(5),
    )
    .await;

    {
        let requests = api.cancel_batch_requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].len(), 1);
        assert_eq!(requests[1].len(), 1);
        assert_eq!(requests[0][0].api_key_index, Some(3));
        assert_eq!(requests[1][0].api_key_index, Some(4));
        assert_eq!(api.cancel_all_calls.load(Ordering::Relaxed), 0);
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_batch_cancel_orders_records_batch_request() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, _rx, cache) = create_test_execution_client(addr, api.clone());
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.connect().await.unwrap();
    let _order_1 = add_limit_order(&cache, "O-401");
    let _order_2 = add_post_only_limit_order(&cache, "O-402");

    let cancels = vec![
        CancelOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("LIGHTER")),
            StrategyId::from("S-001"),
            InstrumentId::from(TEST_INSTRUMENT_ID),
            ClientOrderId::from("O-401"),
            Some(VenueOrderId::from("401")),
            UUID4::new(),
            UnixNanos::default(),
            None,
        ),
        CancelOrder::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("LIGHTER")),
            StrategyId::from("S-001"),
            InstrumentId::from(TEST_INSTRUMENT_ID),
            ClientOrderId::from("O-402"),
            Some(VenueOrderId::from("402")),
            UUID4::new(),
            UnixNanos::default(),
            None,
        ),
    ];
    let cmd = BatchCancelOrders::new(
        TraderId::from("TRADER-001"),
        Some(ClientId::from("LIGHTER")),
        StrategyId::from("S-001"),
        InstrumentId::from(TEST_INSTRUMENT_ID),
        cancels,
        UUID4::new(),
        UnixNanos::default(),
        None,
    );

    client.batch_cancel_orders(cmd).unwrap();
    wait_until_async(
        || {
            let api = api.clone();
            async move { api.cancel_batch_requests.lock().unwrap().len() == 2 }
        },
        Duration::from_secs(5),
    )
    .await;

    {
        let requests = api.cancel_batch_requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].len(), 1);
        assert_eq!(requests[1].len(), 1);
        assert_eq!(requests[0][0].api_key_index, Some(3));
        assert_eq!(requests[1][0].api_key_index, Some(4));
        assert!(api.cancel_requests.lock().unwrap().is_empty());
    }

    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_paginates_inactive_orders() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    api.inactive_orders.lock().unwrap().push_back(Orders {
        code: 200,
        message: None,
        orders: vec![sample_order(102, "O-FILLED", "filled", "0.1000")],
        cursor: Some("next".to_string()),
    });
    api.inactive_orders
        .lock()
        .unwrap()
        .push_back(empty_orders());

    let (client, _rx, _cache) = create_test_execution_client(addr, api);
    let reports = client
        .generate_order_status_reports(&GenerateOrderStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            false,
            Some(InstrumentId::from(TEST_INSTRUMENT_ID)),
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();

    assert_eq!(reports.len(), 2);
    assert_eq!(reports[0].venue_order_id, VenueOrderId::from("101"));
    assert_eq!(reports[1].venue_order_id, VenueOrderId::from("102"));
}

#[rstest]
#[tokio::test]
async fn test_generate_order_status_reports_apply_cached_contingency_metadata() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    api.active_orders.lock().unwrap().orders =
        vec![sample_order(111, "O-CACHED", "open", "0.0000")];

    let (client, _rx, cache) = create_test_execution_client(addr, api);
    let order = add_contingent_limit_order(
        &cache,
        "O-CACHED",
        OrderListId::from("OL-CACHED"),
        vec![ClientOrderId::from("O-LINKED")],
    );

    let reports = client
        .generate_order_status_reports(&GenerateOrderStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            true,
            Some(InstrumentId::from(TEST_INSTRUMENT_ID)),
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].client_order_id, Some(order.client_order_id()));
    assert_eq!(reports[0].order_list_id, order.order_list_id());
    assert_eq!(
        reports[0].linked_order_ids,
        order
            .linked_order_ids()
            .map(|linked_ids| linked_ids.to_vec())
    );
    assert_eq!(reports[0].contingency_type, ContingencyType::Oco);
}

#[rstest]
#[tokio::test]
async fn test_generate_fill_reports_returns_trade_reports() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    api.trades.lock().unwrap().push_back(Trades {
        code: 200,
        message: None,
        trades: vec![sample_trade()],
        cursor: Some("cursor-1".to_string()),
    });
    api.trades.lock().unwrap().push_back(empty_trades());

    let (client, _rx, _cache) = create_test_execution_client(addr, api);
    let reports: Vec<FillReport> = client
        .generate_fill_reports(GenerateFillReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from(TEST_INSTRUMENT_ID)),
            None,
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();

    assert_eq!(reports.len(), 1);
    assert_eq!(reports[0].venue_order_id, VenueOrderId::from("101"));
}

#[rstest]
#[tokio::test]
async fn test_generate_position_status_reports_returns_account_positions() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (client, _rx, _cache) = create_test_execution_client(addr, api);
    let reports: Vec<PositionStatusReport> = client
        .generate_position_status_reports(&GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from(TEST_INSTRUMENT_ID)),
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0].instrument_id,
        InstrumentId::from(TEST_INSTRUMENT_ID)
    );
}

#[rstest]
#[tokio::test]
async fn test_generate_position_status_reports_returns_flat_for_missing_instrument_position() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    {
        let mut account = api.account.lock().unwrap();
        account.accounts[0].positions = Some(Vec::new());
    }
    let (client, _rx, _cache) = create_test_execution_client(addr, api);
    let reports: Vec<PositionStatusReport> = client
        .generate_position_status_reports(&GeneratePositionStatusReports::new(
            UUID4::new(),
            UnixNanos::default(),
            Some(InstrumentId::from(TEST_INSTRUMENT_ID)),
            None,
            None,
            None,
            None,
        ))
        .await
        .unwrap();

    assert_eq!(reports.len(), 1);
    assert_eq!(
        reports[0].instrument_id,
        InstrumentId::from(TEST_INSTRUMENT_ID)
    );
    assert_eq!(reports[0].position_side, PositionSideSpecified::Flat);
    assert!(reports[0].is_flat());
}

#[rstest]
#[tokio::test]
async fn test_query_account_emits_account_event() {
    let addr = start_mock_server().await;
    let api = Arc::new(MockExecutionApi::default());
    let (mut client, mut rx, cache) = create_test_execution_client(addr, api);
    add_test_account(&cache, AccountId::from("LIGHTER-7"));
    client.start().unwrap();
    client.connect().await.unwrap();
    while rx.try_recv().is_ok() {}

    client
        .query_account(QueryAccount::new(
            TraderId::from("TRADER-001"),
            Some(ClientId::from("LIGHTER")),
            AccountId::from("LIGHTER-7"),
            UUID4::new(),
            UnixNanos::default(),
            None,
        ))
        .unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(event, ExecutionEvent::Account(_)));

    client.disconnect().await.unwrap();
}

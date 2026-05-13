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

use std::time::Duration;

use nautilus_common::{
    clients::DataClient,
    live::runner::set_data_event_sender,
    messages::{
        DataEvent,
        data::{SubscribeBookDeltas, SubscribeQuotes, SubscribeTrades},
    },
};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_lighter::data::LighterDataClient;
use nautilus_model::{
    data::Data,
    enums::BookType,
    identifiers::{ClientId, InstrumentId},
};
use rstest::rstest;

use crate::common::{TEST_INSTRUMENT_ID, data_client_config, start_mock_server};

#[rstest]
#[tokio::test]
async fn test_data_client_connect_disconnect() {
    let addr = start_mock_server().await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = data_client_config(addr);
    let mut client = LighterDataClient::new(ClientId::from("LIGHTER"), &config).unwrap();

    assert!(!client.is_connected());
    client.connect().await.unwrap();
    assert!(client.is_connected());
    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
}

#[rstest]
#[tokio::test]
async fn test_data_client_emits_instruments_on_connect() {
    let addr = start_mock_server().await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = data_client_config(addr);
    let mut client = LighterDataClient::new(ClientId::from("LIGHTER"), &config).unwrap();
    client.connect().await.unwrap();

    let mut instrument_count = 0;
    while let Ok(event) = rx.try_recv() {
        if matches!(event, DataEvent::Instrument(_)) {
            instrument_count += 1;
        }
    }

    assert_eq!(instrument_count, 1);
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_subscribe_trades() {
    let addr = start_mock_server().await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = data_client_config(addr);
    let mut client = LighterDataClient::new(ClientId::from("LIGHTER"), &config).unwrap();
    client.connect().await.unwrap();
    while rx.try_recv().is_ok() {}

    let cmd = SubscribeTrades::new(
        InstrumentId::from(TEST_INSTRUMENT_ID),
        Some(ClientId::from("LIGHTER")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    client.subscribe_trades(cmd).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(event, DataEvent::Data(Data::Trade(_))));
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_subscribe_quotes() {
    let addr = start_mock_server().await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = data_client_config(addr);
    let mut client = LighterDataClient::new(ClientId::from("LIGHTER"), &config).unwrap();
    client.connect().await.unwrap();
    while rx.try_recv().is_ok() {}

    let cmd = SubscribeQuotes::new(
        InstrumentId::from(TEST_INSTRUMENT_ID),
        Some(ClientId::from("LIGHTER")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        None,
    );
    client.subscribe_quotes(cmd).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(event, DataEvent::Data(Data::Quote(_))));
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_subscribe_book_deltas() {
    let addr = start_mock_server().await;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = data_client_config(addr);
    let mut client = LighterDataClient::new(ClientId::from("LIGHTER"), &config).unwrap();
    client.connect().await.unwrap();
    while rx.try_recv().is_ok() {}

    let cmd = SubscribeBookDeltas::new(
        InstrumentId::from(TEST_INSTRUMENT_ID),
        BookType::L2_MBP,
        Some(ClientId::from("LIGHTER")),
        None,
        UUID4::new(),
        UnixNanos::default(),
        None,
        false,
        None,
        None,
    );
    client.subscribe_book_deltas(cmd).unwrap();

    let event = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(event, DataEvent::Data(Data::Deltas(_))));
    client.disconnect().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_data_client_reset_clears_connection_state() {
    let addr = start_mock_server().await;
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<DataEvent>();
    set_data_event_sender(tx);

    let config = data_client_config(addr);
    let mut client = LighterDataClient::new(ClientId::from("LIGHTER"), &config).unwrap();

    client.connect().await.unwrap();
    assert!(client.is_connected());

    client.reset().unwrap();
    assert!(!client.is_connected());
}

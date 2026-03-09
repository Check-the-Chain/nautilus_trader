use std::collections::HashMap;

use crate::models::order::Order;
use crate::models::order_book::PriceLevel;
use crate::models::ws::{
    WsAccountAllPositionsUpdate, WsAccountAllUpdate, WsMarketStatsUpdate, WsOrderBook,
    WsOrderBookState, WsUserStatsUpdate,
};

/// Manages local order book state from WebSocket delta updates.
#[derive(Debug, Default)]
pub struct OrderBookHandler {
    states: HashMap<i64, WsOrderBookState>,
}

impl OrderBookHandler {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Initialize or replace the full order book snapshot for a market.
    pub fn set_snapshot(&mut self, market_id: i64, state: WsOrderBookState) {
        self.states.insert(market_id, state);
    }

    /// Apply a delta update to the local order book state.
    /// Returns the updated state if it exists.
    ///
    /// After applying deltas, bids are sorted descending and asks ascending
    /// by price so that `.first()` always returns the best level.
    pub fn apply_update(
        &mut self,
        market_id: i64,
        update: &WsOrderBook,
    ) -> Option<&WsOrderBookState> {
        let state = self.states.get_mut(&market_id)?;
        apply_price_level_deltas(&mut state.asks, &update.asks);
        apply_price_level_deltas(&mut state.bids, &update.bids);
        sort_price_levels_ascending(&mut state.asks);
        sort_price_levels_descending(&mut state.bids);
        Some(state)
    }

    /// Get the current order book state for a market.
    pub fn get(&self, market_id: i64) -> Option<&WsOrderBookState> {
        self.states.get(&market_id)
    }
}

/// Apply delta price levels to an existing price level list.
/// If a delta has size 0.0, the matching price level is removed.
/// If the price already exists, its size is updated.
/// If the price is new, the level is appended.
fn apply_price_level_deltas(existing: &mut Vec<PriceLevel>, deltas: &[PriceLevel]) {
    for delta in deltas {
        let delta_price = match delta.price_f64() {
            Some(p) => p,
            None => continue,
        };

        let is_removal = delta.size_f64().map(|v| v == 0.0).unwrap_or(false);

        if let Some(pos) = existing
            .iter()
            .position(|l| l.price_f64() == Some(delta_price))
        {
            if is_removal {
                existing.remove(pos);
            } else {
                existing[pos].size.clone_from(&delta.size);
            }
        } else if !is_removal {
            existing.push(delta.clone());
        }
    }
}

fn parse_price(level: &PriceLevel) -> f64 {
    level.price_f64().unwrap_or(0.0)
}

/// Sort asks ascending (best/lowest ask first).
fn sort_price_levels_ascending(levels: &mut [PriceLevel]) {
    levels.sort_by(|a, b| {
        parse_price(a)
            .partial_cmp(&parse_price(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

/// Sort bids descending (best/highest bid first).
fn sort_price_levels_descending(levels: &mut [PriceLevel]) {
    levels.sort_by(|a, b| {
        parse_price(b)
            .partial_cmp(&parse_price(a))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
}

/// Manages local account state from WebSocket updates.
#[derive(Debug, Default)]
pub struct AccountHandler {
    states: HashMap<i64, WsAccountAllUpdate>,
}

impl AccountHandler {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Set or replace the full account state.
    pub fn set_state(&mut self, account_id: i64, state: WsAccountAllUpdate) {
        self.states.insert(account_id, state);
    }

    /// Get the current account state.
    pub fn get(&self, account_id: i64) -> Option<&WsAccountAllUpdate> {
        self.states.get(&account_id)
    }
}

/// Manages local account orders state from WebSocket updates.
#[derive(Debug, Default)]
pub struct AccountOrdersHandler {
    /// Legacy/global view: market_id -> orders.
    states_by_market: HashMap<i64, Vec<Order>>,
    /// Precise view: (account_id, market_id) -> orders.
    states_by_account_market: HashMap<(i64, i64), Vec<Order>>,
}

impl AccountOrdersHandler {
    pub fn new() -> Self {
        Self {
            states_by_market: HashMap::new(),
            states_by_account_market: HashMap::new(),
        }
    }

    /// Set or replace the orders for a market.
    pub fn set_orders(&mut self, market_id: i64, orders: Vec<Order>) {
        self.states_by_market.insert(market_id, orders);
    }

    /// Set or replace the orders for an (account, market) pair.
    pub fn set_orders_for_account(&mut self, account_id: i64, market_id: i64, orders: Vec<Order>) {
        self.states_by_account_market
            .insert((account_id, market_id), orders);
    }

    /// Get the current orders for a market.
    pub fn get(&self, market_id: i64) -> Option<&Vec<Order>> {
        self.states_by_market.get(&market_id)
    }

    /// Get the current orders for an (account, market) pair.
    pub fn get_for_account(&self, account_id: i64, market_id: i64) -> Option<&Vec<Order>> {
        self.states_by_account_market.get(&(account_id, market_id))
    }
}

/// Manages local account-all-positions state from WebSocket updates.
#[derive(Debug, Default)]
pub struct AccountAllPositionsHandler {
    /// account_id -> latest account_all_positions payload
    states: HashMap<i64, WsAccountAllPositionsUpdate>,
}

impl AccountAllPositionsHandler {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Set or replace the latest account_all_positions payload.
    pub fn set_state(&mut self, account_id: i64, state: WsAccountAllPositionsUpdate) {
        self.states.insert(account_id, state);
    }

    /// Get the latest account_all_positions payload for an account.
    pub fn get(&self, account_id: i64) -> Option<&WsAccountAllPositionsUpdate> {
        self.states.get(&account_id)
    }
}

/// Manages local user stats state from WebSocket updates.
#[derive(Debug, Default)]
pub struct UserStatsHandler {
    /// account_id -> latest stats
    states: HashMap<i64, WsUserStatsUpdate>,
}

impl UserStatsHandler {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Set or replace the user stats for an account.
    pub fn set_state(&mut self, account_id: i64, state: WsUserStatsUpdate) {
        self.states.insert(account_id, state);
    }

    /// Get the current user stats for an account.
    pub fn get(&self, account_id: i64) -> Option<&WsUserStatsUpdate> {
        self.states.get(&account_id)
    }
}

/// Manages local market stats state from WebSocket updates.
#[derive(Debug, Default)]
pub struct MarketStatsHandler {
    /// market_id -> latest market stats payload
    states: HashMap<i64, WsMarketStatsUpdate>,
}

impl MarketStatsHandler {
    pub fn new() -> Self {
        Self {
            states: HashMap::new(),
        }
    }

    /// Set or replace the market stats for a market.
    pub fn set_state(&mut self, market_id: i64, state: WsMarketStatsUpdate) {
        self.states.insert(market_id, state);
    }

    /// Get the current market stats for a market.
    pub fn get(&self, market_id: i64) -> Option<&WsMarketStatsUpdate> {
        self.states.get(&market_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::models::order::Order;

    use super::AccountOrdersHandler;

    fn sample_order(order_index: i64, market_index: i64) -> Order {
        Order {
            order_index,
            client_order_index: 0,
            order_id: String::new(),
            client_order_id: String::new(),
            market_index,
            owner_account_index: 0,
            initial_base_amount: "1".to_string(),
            price: "1".to_string(),
            nonce: 0,
            remaining_base_amount: "1".to_string(),
            is_ask: true,
            base_size: 0,
            base_price: 0,
            filled_base_amount: "0".to_string(),
            filled_quote_amount: String::new(),
            side: "ask".to_string(),
            order_type: "limit".to_string(),
            time_in_force: "good-till-time".to_string(),
            reduce_only: false,
            trigger_price: String::new(),
            order_expiry: 0,
            status: "open".to_string(),
            trigger_status: String::new(),
            trigger_time: 0,
            parent_order_index: 0,
            parent_order_id: String::new(),
            to_trigger_order_id_0: String::new(),
            to_trigger_order_id_1: String::new(),
            to_cancel_order_id_0: String::new(),
            block_height: 0,
            timestamp: 0,
            created_at: 0,
            updated_at: 0,
            transaction_time: 0,
        }
    }

    #[test]
    fn account_scoped_orders_do_not_clobber_each_other() {
        let mut handler = AccountOrdersHandler::new();
        handler.set_orders_for_account(100, 89, vec![sample_order(1, 89)]);
        handler.set_orders_for_account(200, 89, vec![sample_order(2, 89)]);

        let a = handler.get_for_account(100, 89).unwrap();
        let b = handler.get_for_account(200, 89).unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        assert_eq!(a[0].order_index, 1);
        assert_eq!(b[0].order_index, 2);
    }
}

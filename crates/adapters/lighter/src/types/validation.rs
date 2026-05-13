use crate::constants::*;
use crate::error::{Result, SdkError};
use crate::types::tx_info::*;

fn validate_account_index(index: i64) -> Result<()> {
    if index < MIN_ACCOUNT_INDEX {
        return Err(SdkError::AccountIndexTooLow);
    }
    if index > MAX_ACCOUNT_INDEX {
        return Err(SdkError::AccountIndexTooHigh);
    }
    Ok(())
}

fn validate_api_key_index(index: u8) -> Result<()> {
    if index > MAX_API_KEY_INDEX {
        return Err(SdkError::ApiKeyIndexTooHigh);
    }
    Ok(())
}

fn validate_nonce(nonce: i64) -> Result<()> {
    if nonce < MIN_NONCE {
        return Err(SdkError::NonceTooLow);
    }
    Ok(())
}

fn validate_expired_at(expired_at: i64) -> Result<()> {
    if !(0..=MAX_TIMESTAMP).contains(&expired_at) {
        return Err(SdkError::ExpiredAtInvalid);
    }
    Ok(())
}

fn is_valid_market_index(market_index: i16) -> (bool, bool) {
    let is_perps = (MIN_PERPS_MARKET_INDEX..=MAX_PERPS_MARKET_INDEX).contains(&market_index);
    let is_spot = (MIN_SPOT_MARKET_INDEX..=MAX_SPOT_MARKET_INDEX).contains(&market_index);
    (is_perps, is_spot)
}

fn validate_market_index(market_index: i16) -> Result<(bool, bool)> {
    let (is_perps, is_spot) = is_valid_market_index(market_index);
    if !is_perps && !is_spot {
        return Err(SdkError::InvalidMarketIndex);
    }
    Ok((is_perps, is_spot))
}

impl TxInfo for L2ChangePubKeyTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_CHANGE_PUB_KEY
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;
        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        if self.pub_key.len() != PUB_KEY_LENGTH {
            return Err(SdkError::PubKeyInvalid);
        }
        Ok(())
    }
}

impl TxInfo for L2CreateSubAccountTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_CREATE_SUB_ACCOUNT
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        if self.account_index < MIN_ACCOUNT_INDEX {
            return Err(SdkError::AccountIndexTooLow);
        }
        if self.account_index > MAX_MASTER_ACCOUNT_INDEX {
            return Err(SdkError::AccountIndexTooHigh);
        }
        validate_api_key_index(self.api_key_index)?;
        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2CreateOrderTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_CREATE_ORDER
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;

        let (is_perps, is_spot) = validate_market_index(self.order_info.market_index)?;
        let _ = is_spot; // used below

        // ClientOrderIndex
        if self.order_info.client_order_index != NIL_CLIENT_ORDER_INDEX {
            if self.order_info.client_order_index < MIN_CLIENT_ORDER_INDEX {
                return Err(SdkError::ClientOrderIndexTooLow);
            }
            if self.order_info.client_order_index > MAX_CLIENT_ORDER_INDEX {
                return Err(SdkError::ClientOrderIndexTooHigh);
            }
        }

        // BaseAmount
        if self.order_info.reduce_only != 1 && self.order_info.base_amount == NIL_ORDER_BASE_AMOUNT
        {
            return Err(SdkError::BaseAmountTooLow);
        }
        if self.order_info.base_amount != NIL_ORDER_BASE_AMOUNT
            && self.order_info.base_amount < MIN_ORDER_BASE_AMOUNT
        {
            return Err(SdkError::BaseAmountTooLow);
        }
        if self.order_info.base_amount > MAX_ORDER_BASE_AMOUNT {
            return Err(SdkError::BaseAmountTooHigh);
        }

        // Price
        if self.order_info.price < MIN_ORDER_PRICE {
            return Err(SdkError::PriceTooLow);
        }
        // IsAsk
        if self.order_info.is_ask != 0 && self.order_info.is_ask != 1 {
            return Err(SdkError::IsAskInvalid);
        }

        // TimeInForce
        if self.order_info.time_in_force != IMMEDIATE_OR_CANCEL
            && self.order_info.time_in_force != GOOD_TILL_TIME
            && self.order_info.time_in_force != POST_ONLY
        {
            return Err(SdkError::OrderTimeInForceInvalid);
        }

        // ReduceOnly
        if (self.order_info.reduce_only != 0 && self.order_info.reduce_only != 1)
            || (is_spot && self.order_info.reduce_only == 1)
        {
            return Err(SdkError::OrderReduceOnlyInvalid);
        }

        // OrderExpiry
        if self.order_info.order_expiry != NIL_ORDER_EXPIRY
            && self.order_info.order_expiry < MIN_ORDER_EXPIRY
        {
            return Err(SdkError::OrderExpiryInvalid);
        }

        // Type-specific validation
        match self.order_info.order_type {
            MARKET_ORDER => {
                if self.order_info.time_in_force != IMMEDIATE_OR_CANCEL {
                    return Err(SdkError::OrderTimeInForceInvalid);
                }
                if self.order_info.order_expiry != NIL_ORDER_EXPIRY {
                    return Err(SdkError::OrderExpiryInvalid);
                }
                if self.order_info.trigger_price != NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
            }
            LIMIT_ORDER => {
                if self.order_info.trigger_price != NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
                if self.order_info.time_in_force == IMMEDIATE_OR_CANCEL
                    && self.order_info.order_expiry != NIL_ORDER_EXPIRY
                {
                    return Err(SdkError::OrderExpiryInvalid);
                }
                if self.order_info.time_in_force != IMMEDIATE_OR_CANCEL
                    && self.order_info.order_expiry == NIL_ORDER_EXPIRY
                {
                    return Err(SdkError::OrderExpiryInvalid);
                }
            }
            STOP_LOSS_ORDER | TAKE_PROFIT_ORDER => {
                if !is_perps {
                    return Err(SdkError::OrderTypeInvalid);
                }
                if self.order_info.time_in_force != IMMEDIATE_OR_CANCEL {
                    return Err(SdkError::OrderTimeInForceInvalid);
                }
                if self.order_info.trigger_price == NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
                if self.order_info.order_expiry == NIL_ORDER_EXPIRY {
                    return Err(SdkError::OrderExpiryInvalid);
                }
            }
            STOP_LOSS_LIMIT_ORDER | TAKE_PROFIT_LIMIT_ORDER => {
                if !is_perps {
                    return Err(SdkError::OrderTypeInvalid);
                }
                if self.order_info.trigger_price == NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
                if self.order_info.order_expiry == NIL_ORDER_EXPIRY {
                    return Err(SdkError::OrderExpiryInvalid);
                }
            }
            TWAP_ORDER => {
                if self.order_info.time_in_force != GOOD_TILL_TIME {
                    return Err(SdkError::OrderTimeInForceInvalid);
                }
                if self.order_info.trigger_price != NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
                if self.order_info.order_expiry == NIL_ORDER_EXPIRY {
                    return Err(SdkError::OrderExpiryInvalid);
                }
            }
            _ => return Err(SdkError::OrderTypeInvalid),
        }

        // TriggerPrice range
        if self.order_info.trigger_price != NIL_ORDER_TRIGGER_PRICE
            && self.order_info.trigger_price < MIN_ORDER_TRIGGER_PRICE
        {
            return Err(SdkError::OrderTriggerPriceInvalid);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2CreateGroupedOrdersTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_CREATE_GROUPED_ORDERS
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;

        if self.orders.is_empty() || self.orders.len() as i64 > MAX_GROUPED_ORDER_COUNT {
            return Err(SdkError::OrderGroupSizeInvalid);
        }

        // First order market index must be perps
        if !(MIN_PERPS_MARKET_INDEX..=MAX_PERPS_MARKET_INDEX).contains(&self.orders[0].market_index)
        {
            return Err(SdkError::InvalidMarketIndex);
        }

        for order in &self.orders {
            if order.market_index != self.orders[0].market_index {
                return Err(SdkError::MarketIndexMismatch);
            }
            if order.client_order_index != NIL_CLIENT_ORDER_INDEX {
                return Err(SdkError::ClientOrderIndexNotNil);
            }
            if order.reduce_only != 1 && order.base_amount == NIL_ORDER_BASE_AMOUNT {
                return Err(SdkError::BaseAmountTooLow);
            }
            if order.base_amount != NIL_ORDER_BASE_AMOUNT
                && order.base_amount < MIN_ORDER_BASE_AMOUNT
            {
                return Err(SdkError::BaseAmountTooLow);
            }
            if order.base_amount > MAX_ORDER_BASE_AMOUNT {
                return Err(SdkError::BaseAmountTooHigh);
            }
            if order.price < MIN_ORDER_PRICE {
                return Err(SdkError::PriceTooLow);
            }
            if order.is_ask != 0 && order.is_ask != 1 {
                return Err(SdkError::IsAskInvalid);
            }
            if order.time_in_force != IMMEDIATE_OR_CANCEL
                && order.time_in_force != GOOD_TILL_TIME
                && order.time_in_force != POST_ONLY
            {
                return Err(SdkError::OrderTimeInForceInvalid);
            }
            if order.reduce_only != 0 && order.reduce_only != 1 {
                return Err(SdkError::OrderReduceOnlyInvalid);
            }
            if order.order_expiry != NIL_ORDER_EXPIRY && order.order_expiry < MIN_ORDER_EXPIRY {
                return Err(SdkError::OrderExpiryInvalid);
            }
            if order.trigger_price != NIL_ORDER_TRIGGER_PRICE
                && order.trigger_price < MIN_ORDER_TRIGGER_PRICE
            {
                return Err(SdkError::OrderTriggerPriceInvalid);
            }
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;

        match self.grouping_type {
            GROUPING_TYPE_ONE_CANCELS_THE_OTHER => self.validate_oco(),
            GROUPING_TYPE_ONE_TRIGGERS_THE_OTHER => self.validate_oto(),
            GROUPING_TYPE_ONE_TRIGGERS_A_ONE_CANCELS_THE_OTHER => self.validate_otoco(),
            _ => Err(SdkError::GroupingTypeInvalid),
        }
    }
}

impl L2CreateGroupedOrdersTxInfo {
    fn validate_parent_order(&self, order: &super::order_info::OrderInfo) -> Result<()> {
        match order.order_type {
            MARKET_ORDER => {
                if order.time_in_force != IMMEDIATE_OR_CANCEL {
                    return Err(SdkError::OrderTimeInForceInvalid);
                }
                if order.order_expiry != NIL_ORDER_EXPIRY {
                    return Err(SdkError::OrderExpiryInvalid);
                }
                if order.trigger_price != NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
            }
            LIMIT_ORDER => {
                if order.trigger_price != NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
                if order.time_in_force == IMMEDIATE_OR_CANCEL
                    && order.order_expiry != NIL_ORDER_EXPIRY
                {
                    return Err(SdkError::OrderExpiryInvalid);
                }
                if order.time_in_force != IMMEDIATE_OR_CANCEL
                    && order.order_expiry == NIL_ORDER_EXPIRY
                {
                    return Err(SdkError::OrderExpiryInvalid);
                }
            }
            _ => return Err(SdkError::OrderTypeInvalid),
        }
        Ok(())
    }

    fn validate_child_order(&self, order: &super::order_info::OrderInfo) -> Result<()> {
        match order.order_type {
            STOP_LOSS_ORDER | TAKE_PROFIT_ORDER => {
                if order.time_in_force != IMMEDIATE_OR_CANCEL {
                    return Err(SdkError::OrderTimeInForceInvalid);
                }
                if order.trigger_price == NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
                if order.order_expiry == NIL_ORDER_EXPIRY {
                    return Err(SdkError::OrderExpiryInvalid);
                }
            }
            STOP_LOSS_LIMIT_ORDER | TAKE_PROFIT_LIMIT_ORDER => {
                if order.trigger_price == NIL_ORDER_TRIGGER_PRICE {
                    return Err(SdkError::OrderTriggerPriceInvalid);
                }
                if order.order_expiry == NIL_ORDER_EXPIRY {
                    return Err(SdkError::OrderExpiryInvalid);
                }
            }
            _ => return Err(SdkError::OrderTypeInvalid),
        }
        Ok(())
    }

    fn validate_sibling_orders(&self, orders: &[super::order_info::OrderInfo]) -> Result<()> {
        if orders.len() != 2 {
            return Err(SdkError::OrderGroupSizeInvalid);
        }
        let mut sl_flag = false;
        let mut tp_flag = false;
        for order in orders {
            self.validate_child_order(order)?;
            if order.order_type == STOP_LOSS_ORDER || order.order_type == STOP_LOSS_LIMIT_ORDER {
                sl_flag = true;
            } else if order.order_type == TAKE_PROFIT_ORDER
                || order.order_type == TAKE_PROFIT_LIMIT_ORDER
            {
                tp_flag = true;
            }
        }
        if !sl_flag || !tp_flag {
            return Err(SdkError::OrderTypeInvalid);
        }
        Ok(())
    }

    fn validate_oco(&self) -> Result<()> {
        if self.orders.len() != 2 {
            return Err(SdkError::OrderGroupSizeInvalid);
        }
        if self.orders[0].base_amount != self.orders[1].base_amount {
            return Err(SdkError::BaseAmountsNotEqual);
        }
        if self.orders[0].is_ask != self.orders[1].is_ask {
            return Err(SdkError::IsAskInvalid);
        }
        if self.orders[0].reduce_only != 1 || self.orders[1].reduce_only != 1 {
            return Err(SdkError::OrderReduceOnlyInvalid);
        }
        if self.orders[0].order_expiry != self.orders[1].order_expiry {
            return Err(SdkError::OrderExpiryInvalid);
        }
        self.validate_sibling_orders(&self.orders)
    }

    fn validate_oto(&self) -> Result<()> {
        if self.orders.len() != 2 {
            return Err(SdkError::OrderGroupSizeInvalid);
        }
        if self.orders[1].base_amount != NIL_ORDER_BASE_AMOUNT {
            return Err(SdkError::BaseAmountNotNil);
        }
        if self.orders[0].is_ask == self.orders[1].is_ask {
            return Err(SdkError::IsAskInvalid);
        }
        if self.orders[0].order_expiry != NIL_ORDER_EXPIRY
            && self.orders[0].order_expiry != self.orders[1].order_expiry
        {
            return Err(SdkError::OrderExpiryInvalid);
        }
        self.validate_parent_order(&self.orders[0])?;
        self.validate_child_order(&self.orders[1])
    }

    fn validate_otoco(&self) -> Result<()> {
        if self.orders.len() != 3 {
            return Err(SdkError::OrderGroupSizeInvalid);
        }
        if self.orders[1].base_amount != NIL_ORDER_BASE_AMOUNT
            || self.orders[2].base_amount != NIL_ORDER_BASE_AMOUNT
        {
            return Err(SdkError::BaseAmountNotNil);
        }
        if self.orders[0].is_ask == self.orders[1].is_ask
            || self.orders[0].is_ask == self.orders[2].is_ask
        {
            return Err(SdkError::IsAskInvalid);
        }
        if self.orders[1].order_expiry != self.orders[2].order_expiry {
            return Err(SdkError::OrderExpiryInvalid);
        }
        if self.orders[0].order_expiry != NIL_ORDER_EXPIRY
            && self.orders[0].order_expiry != self.orders[1].order_expiry
        {
            return Err(SdkError::OrderExpiryInvalid);
        }
        self.validate_parent_order(&self.orders[0])?;
        self.validate_sibling_orders(&self.orders[1..])
    }
}

impl TxInfo for L2CancelOrderTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_CANCEL_ORDER
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;
        validate_market_index(self.market_index)?;

        if self.index < MIN_CLIENT_ORDER_INDEX {
            return Err(SdkError::OrderIndexTooLow);
        }
        if self.index > MAX_ORDER_INDEX {
            return Err(SdkError::OrderIndexTooHigh);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2ModifyOrderTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_MODIFY_ORDER
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;
        validate_market_index(self.market_index)?;

        if self.index < MIN_CLIENT_ORDER_INDEX {
            return Err(SdkError::ClientOrderIndexTooLow);
        }
        if self.index > MAX_ORDER_INDEX {
            return Err(SdkError::ClientOrderIndexTooHigh);
        }

        if self.base_amount != NIL_ORDER_BASE_AMOUNT && self.base_amount < MIN_ORDER_BASE_AMOUNT {
            return Err(SdkError::BaseAmountTooLow);
        }
        if self.base_amount > MAX_ORDER_BASE_AMOUNT {
            return Err(SdkError::BaseAmountTooHigh);
        }

        if self.price < MIN_ORDER_PRICE {
            return Err(SdkError::PriceTooLow);
        }

        if self.trigger_price != NIL_ORDER_TRIGGER_PRICE
            && self.trigger_price < MIN_ORDER_TRIGGER_PRICE
        {
            return Err(SdkError::OrderTriggerPriceInvalid);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2CancelAllOrdersTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_CANCEL_ALL_ORDERS
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;

        if self.api_key_index > MAX_API_KEY_INDEX && self.api_key_index != NIL_API_KEY_INDEX {
            return Err(SdkError::ApiKeyIndexTooHigh);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;

        match self.time_in_force {
            IMMEDIATE_CANCEL_ALL => {
                if self.time != NIL_ORDER_EXPIRY {
                    return Err(SdkError::CancelAllTimeIsNotNil);
                }
            }
            SCHEDULED_CANCEL_ALL => {
                if self.time < MIN_ORDER_EXPIRY {
                    return Err(SdkError::CancelAllTimeIsNotInRange);
                }
            }
            ABORT_SCHEDULED_CANCEL_ALL => {
                if self.time != 0 {
                    return Err(SdkError::CancelAllTimeIsNotNil);
                }
            }
            _ => return Err(SdkError::InvalidCancelAllTimeInForce),
        }

        Ok(())
    }
}

impl TxInfo for L2TransferTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_TRANSFER
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        if self.from_account_index < MIN_ACCOUNT_INDEX + 1 {
            return Err(SdkError::FromAccountIndexTooLow);
        }
        if self.from_account_index > MAX_ACCOUNT_INDEX {
            return Err(SdkError::FromAccountIndexTooHigh);
        }
        validate_api_key_index(self.api_key_index)?;

        if self.to_account_index < MIN_ACCOUNT_INDEX {
            return Err(SdkError::ToAccountIndexTooLow);
        }
        if self.to_account_index > MAX_ACCOUNT_INDEX {
            return Err(SdkError::ToAccountIndexTooHigh);
        }

        if self.asset_index < MIN_ASSET_INDEX {
            return Err(SdkError::AssetIndexTooLow);
        }
        if self.asset_index > MAX_ASSET_INDEX {
            return Err(SdkError::AssetIndexTooHigh);
        }

        if self.from_route_type != ASSET_ROUTE_TYPE_PERPS
            && self.from_route_type != ASSET_ROUTE_TYPE_SPOT
        {
            return Err(SdkError::RouteTypeInvalid);
        }
        if self.to_route_type != ASSET_ROUTE_TYPE_PERPS
            && self.to_route_type != ASSET_ROUTE_TYPE_SPOT
        {
            return Err(SdkError::RouteTypeInvalid);
        }

        if self.amount <= 0 {
            return Err(SdkError::TransferAmountTooLow);
        }
        if self.amount > MAX_TRANSFER_AMOUNT {
            return Err(SdkError::TransferAmountTooHigh);
        }

        if self.usdc_fee < 0 {
            return Err(SdkError::TransferFeeNegative);
        }
        if self.usdc_fee > MAX_TRANSFER_AMOUNT {
            return Err(SdkError::TransferFeeTooHigh);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2WithdrawTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_WITHDRAW
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        if self.from_account_index < MIN_ACCOUNT_INDEX {
            return Err(SdkError::FromAccountIndexTooLow);
        }
        if self.from_account_index > MAX_ACCOUNT_INDEX {
            return Err(SdkError::FromAccountIndexTooHigh);
        }
        validate_api_key_index(self.api_key_index)?;

        if self.asset_index < MIN_ASSET_INDEX {
            return Err(SdkError::AssetIndexTooLow);
        }
        if self.asset_index > MAX_ASSET_INDEX {
            return Err(SdkError::AssetIndexTooHigh);
        }

        if self.route_type != ASSET_ROUTE_TYPE_PERPS && self.route_type != ASSET_ROUTE_TYPE_SPOT {
            return Err(SdkError::RouteTypeInvalid);
        }

        if self.amount == 0 {
            return Err(SdkError::WithdrawalAmountTooLow);
        }
        if self.amount > MAX_WITHDRAWAL_AMOUNT {
            return Err(SdkError::WithdrawalAmountTooHigh);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2CreatePublicPoolTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_CREATE_PUBLIC_POOL
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        if self.account_index < MIN_ACCOUNT_INDEX {
            return Err(SdkError::AccountIndexTooLow);
        }
        if self.account_index > MAX_MASTER_ACCOUNT_INDEX {
            return Err(SdkError::AccountIndexTooHigh);
        }
        validate_api_key_index(self.api_key_index)?;

        if self.operator_fee < 0 || self.operator_fee > FEE_TICK {
            return Err(SdkError::InvalidPoolOperatorFee);
        }
        if self.initial_total_shares <= 0 || self.initial_total_shares > MAX_INITIAL_TOTAL_SHARES {
            return Err(if self.initial_total_shares <= 0 {
                SdkError::PoolInitialTotalSharesTooLow
            } else {
                SdkError::PoolInitialTotalSharesTooHigh
            });
        }
        if self.min_operator_share_rate > SHARE_TICK {
            return Err(SdkError::PoolMinOperatorShareRateTooHigh);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2UpdatePublicPoolTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_UPDATE_PUBLIC_POOL
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;

        if self.public_pool_index < MIN_ACCOUNT_INDEX {
            return Err(SdkError::PublicPoolIndexTooLow);
        }
        if self.public_pool_index > MAX_ACCOUNT_INDEX {
            return Err(SdkError::PublicPoolIndexTooHigh);
        }

        if self.status != 0 && self.status != 1 {
            return Err(SdkError::InvalidPoolStatus);
        }
        if self.operator_fee < 0 || self.operator_fee > FEE_TICK {
            return Err(SdkError::InvalidPoolOperatorFee);
        }
        if self.min_operator_share_rate > SHARE_TICK {
            return Err(SdkError::PoolMinOperatorShareRateTooHigh);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2MintSharesTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_MINT_SHARES
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;

        if self.public_pool_index < MIN_ACCOUNT_INDEX {
            return Err(SdkError::PublicPoolIndexTooLow);
        }
        if self.public_pool_index > MAX_ACCOUNT_INDEX {
            return Err(SdkError::PublicPoolIndexTooHigh);
        }

        if self.share_amount < MIN_POOL_SHARES_TO_MINT_OR_BURN {
            return Err(SdkError::PoolMintShareAmountTooLow);
        }
        if self.share_amount > MAX_POOL_SHARES_TO_MINT_OR_BURN {
            return Err(SdkError::PoolMintShareAmountTooHigh);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2BurnSharesTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_BURN_SHARES
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;

        if self.public_pool_index < MIN_ACCOUNT_INDEX {
            return Err(SdkError::PublicPoolIndexTooLow);
        }
        if self.public_pool_index > MAX_ACCOUNT_INDEX {
            return Err(SdkError::PublicPoolIndexTooHigh);
        }

        if self.share_amount < MIN_POOL_SHARES_TO_MINT_OR_BURN {
            return Err(SdkError::PoolBurnShareAmountTooLow);
        }
        if self.share_amount > MAX_POOL_SHARES_TO_MINT_OR_BURN {
            return Err(SdkError::PoolBurnShareAmountTooHigh);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2UpdateLeverageTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_UPDATE_LEVERAGE
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;

        // Must be perps market
        if self.market_index < MIN_PERPS_MARKET_INDEX || self.market_index > MAX_PERPS_MARKET_INDEX
        {
            return Err(SdkError::InvalidMarketIndex);
        }

        if self.margin_mode != CROSS_MARGIN && self.margin_mode != ISOLATED_MARGIN {
            return Err(SdkError::InvalidMarginMode);
        }
        if self.initial_margin_fraction == 0
            || self.initial_margin_fraction as i64 > MARGIN_FRACTION_TICK
        {
            return Err(if self.initial_margin_fraction == 0 {
                SdkError::InitialMarginFractionTooLow
            } else {
                SdkError::InitialMarginFractionTooHigh
            });
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

impl TxInfo for L2UpdateMarginTxInfo {
    fn tx_type(&self) -> u8 {
        TX_TYPE_L2_UPDATE_MARGIN
    }
    fn tx_hash(&self) -> &str {
        &self.signed_hash
    }
    fn validate(&self) -> Result<()> {
        validate_account_index(self.account_index)?;
        validate_api_key_index(self.api_key_index)?;

        // Must be perps market
        if self.market_index < MIN_PERPS_MARKET_INDEX || self.market_index > MAX_PERPS_MARKET_INDEX
        {
            return Err(SdkError::InvalidMarketIndex);
        }

        if self.usdc_amount <= 0 || self.usdc_amount > MAX_TRANSFER_AMOUNT {
            return Err(if self.usdc_amount <= 0 {
                SdkError::TransferAmountTooLow
            } else {
                SdkError::TransferAmountTooHigh
            });
        }

        if self.direction != REMOVE_FROM_ISOLATED_MARGIN && self.direction != ADD_TO_ISOLATED_MARGIN
        {
            return Err(SdkError::InvalidUpdateMarginDirection);
        }

        validate_nonce(self.nonce)?;
        validate_expired_at(self.expired_at)?;
        Ok(())
    }
}

// Transaction types
pub const TX_TYPE_EMPTY: u8 = 0;
pub const TX_TYPE_L1_DEPOSIT: u8 = 1;
pub const TX_TYPE_L1_CHANGE_PUB_KEY: u8 = 2;
pub const TX_TYPE_L1_CREATE_MARKET: u8 = 3;
pub const TX_TYPE_L1_UPDATE_MARKET: u8 = 4;
pub const TX_TYPE_L1_CANCEL_ALL_ORDERS: u8 = 5;
pub const TX_TYPE_L1_WITHDRAW: u8 = 6;
pub const TX_TYPE_L1_CREATE_ORDER: u8 = 7;
pub const TX_TYPE_L2_CHANGE_PUB_KEY: u8 = 8;
pub const TX_TYPE_L2_CREATE_SUB_ACCOUNT: u8 = 9;
pub const TX_TYPE_L2_CREATE_PUBLIC_POOL: u8 = 10;
pub const TX_TYPE_L2_UPDATE_PUBLIC_POOL: u8 = 11;
pub const TX_TYPE_L2_TRANSFER: u8 = 12;
pub const TX_TYPE_L2_WITHDRAW: u8 = 13;
pub const TX_TYPE_L2_CREATE_ORDER: u8 = 14;
pub const TX_TYPE_L2_CANCEL_ORDER: u8 = 15;
pub const TX_TYPE_L2_CANCEL_ALL_ORDERS: u8 = 16;
pub const TX_TYPE_L2_MODIFY_ORDER: u8 = 17;
pub const TX_TYPE_L2_MINT_SHARES: u8 = 18;
pub const TX_TYPE_L2_BURN_SHARES: u8 = 19;
pub const TX_TYPE_L2_UPDATE_LEVERAGE: u8 = 20;
pub const TX_TYPE_INTERNAL_CLAIM_ORDER: u8 = 21;
pub const TX_TYPE_INTERNAL_CANCEL_ORDER: u8 = 22;
pub const TX_TYPE_INTERNAL_DELEVERAGE: u8 = 23;
pub const TX_TYPE_INTERNAL_EXIT_POSITION: u8 = 24;
pub const TX_TYPE_INTERNAL_CANCEL_ALL_ORDERS: u8 = 25;
pub const TX_TYPE_INTERNAL_LIQUIDATE_POSITION: u8 = 26;
pub const TX_TYPE_INTERNAL_CREATE_ORDER: u8 = 27;
pub const TX_TYPE_L2_CREATE_GROUPED_ORDERS: u8 = 28;
pub const TX_TYPE_L2_UPDATE_MARGIN: u8 = 29;
pub const TX_TYPE_L1_BURN_SHARES: u8 = 30;

// Order types
pub const LIMIT_ORDER: u8 = 0;
pub const MARKET_ORDER: u8 = 1;
pub const STOP_LOSS_ORDER: u8 = 2;
pub const STOP_LOSS_LIMIT_ORDER: u8 = 3;
pub const TAKE_PROFIT_ORDER: u8 = 4;
pub const TAKE_PROFIT_LIMIT_ORDER: u8 = 5;
pub const TWAP_ORDER: u8 = 6;
pub const TWAP_SUB_ORDER: u8 = 7;
pub const LIQUIDATION_ORDER: u8 = 8;
pub const API_MAX_ORDER_TYPE: u8 = TWAP_ORDER;

// Order time-in-force
pub const IMMEDIATE_OR_CANCEL: u8 = 0;
pub const GOOD_TILL_TIME: u8 = 1;
pub const POST_ONLY: u8 = 2;

// Grouping types
pub const GROUPING_TYPE: u8 = 0;
pub const GROUPING_TYPE_ONE_TRIGGERS_THE_OTHER: u8 = 1;
pub const GROUPING_TYPE_ONE_CANCELS_THE_OTHER: u8 = 2;
pub const GROUPING_TYPE_ONE_TRIGGERS_A_ONE_CANCELS_THE_OTHER: u8 = 3;

// Cancel all orders time-in-force
pub const IMMEDIATE_CANCEL_ALL: u8 = 0;
pub const SCHEDULED_CANCEL_ALL: u8 = 1;
pub const ABORT_SCHEDULED_CANCEL_ALL: u8 = 2;

// Asset margin mode
pub const ASSET_MARGIN_MODE_DISABLED: u8 = 0;
pub const ASSET_MARGIN_MODE_ENABLED: u8 = 1;

// Asset route type
pub const ASSET_ROUTE_TYPE_PERPS: u8 = 0;
pub const ASSET_ROUTE_TYPE_SPOT: u8 = 1;

// Position margin mode
pub const CROSS_MARGIN: u8 = 0;
pub const ISOLATED_MARGIN: u8 = 1;

// Margin update direction
pub const REMOVE_FROM_ISOLATED_MARGIN: u8 = 0;
pub const ADD_TO_ISOLATED_MARGIN: u8 = 1;

// Value constants
pub const ONE_USDC: i64 = 1_000_000;
pub const FEE_TICK: i64 = 1_000_000;
pub const MARGIN_FRACTION_TICK: i64 = 10_000;
pub const SHARE_TICK: u16 = 10_000;

// Account index bounds
pub const MIN_ACCOUNT_INDEX: i64 = 0;
pub const MAX_ACCOUNT_INDEX: i64 = 281_474_976_710_654; // (1 << 48) - 2
pub const MAX_MASTER_ACCOUNT_INDEX: i64 = 140_737_488_355_327; // (1 << 47) - 1
pub const MIN_SUB_ACCOUNT_INDEX: i64 = 140_737_488_355_328; // (1 << 47)

// API key index bounds
pub const MIN_API_KEY_INDEX: u8 = 0;
pub const MAX_API_KEY_INDEX: u8 = 254; // (1 << 8) - 2
pub const NIL_API_KEY_INDEX: u8 = 255;

// Market index bounds
pub const MIN_MARKET_INDEX: i16 = 0;
pub const MIN_PERPS_MARKET_INDEX: i16 = 0;
pub const MAX_PERPS_MARKET_INDEX: i16 = 254; // (1 << 8) - 2
pub const NIL_MARKET_INDEX: i16 = 255;
pub const MIN_SPOT_MARKET_INDEX: i16 = 2048; // (1 << 11)
pub const MAX_SPOT_MARKET_INDEX: i16 = 4094; // (1 << 12) - 2

// Asset index bounds
pub const NATIVE_ASSET_INDEX: u16 = 1;
pub const USDC_ASSET_INDEX: u16 = 3;
pub const MIN_ASSET_INDEX: i16 = 1;
pub const MAX_ASSET_INDEX: i16 = 62; // (1 << 6) - 2
pub const NIL_ASSET_INDEX: i16 = 0;

// Pool constants
pub const MAX_INVESTED_PUBLIC_POOL_COUNT: i64 = 16;
pub const INITIAL_POOL_SHARE_VALUE: i64 = 1_000; // 0.001 USDC
pub const MIN_INITIAL_TOTAL_SHARES: i64 = 1_000 * (ONE_USDC / INITIAL_POOL_SHARE_VALUE);
pub const MAX_INITIAL_TOTAL_SHARES: i64 = 1_000_000_000 * (ONE_USDC / INITIAL_POOL_SHARE_VALUE);
pub const MAX_POOL_SHARES: i64 = (1 << 60) - 1;
pub const MAX_BURNT_SHARE_USDC_VALUE: i64 = (1 << 60) - 1;
pub const MAX_POOL_ENTRY_USDC: i64 = (1 << 56) - 1;
pub const MIN_POOL_SHARES_TO_MINT_OR_BURN: i64 = 1;
pub const MAX_POOL_SHARES_TO_MINT_OR_BURN: i64 = (1 << 60) - 1;

// Nonce bounds
pub const MIN_NONCE: i64 = 0;

// Order nonce bounds
pub const MIN_ORDER_NONCE: i64 = 0;
pub const MAX_ORDER_NONCE: i64 = (1 << 48) - 1;

// Client order index bounds
pub const NIL_CLIENT_ORDER_INDEX: i64 = 0;
pub const NIL_ORDER_INDEX: i64 = 0;
pub const MIN_CLIENT_ORDER_INDEX: i64 = 1;
pub const MAX_CLIENT_ORDER_INDEX: i64 = (1 << 48) - 1;
pub const MIN_ORDER_INDEX: i64 = MAX_CLIENT_ORDER_INDEX + 1;
pub const MAX_ORDER_INDEX: i64 = (1 << 60) - 1;

// Order amount bounds
pub const MIN_ORDER_BASE_AMOUNT: i64 = 1;
pub const MAX_ORDER_BASE_AMOUNT: i64 = (1 << 48) - 1;
pub const NIL_ORDER_BASE_AMOUNT: i64 = 0;

// Order price bounds
pub const NIL_ORDER_PRICE: u32 = 0;
pub const MIN_ORDER_PRICE: u32 = 1;
pub const MAX_ORDER_PRICE: u32 = u32::MAX;

// Cancel all period bounds
pub const MIN_ORDER_CANCEL_ALL_PERIOD: i64 = 1000 * 60 * 5; // 5 minutes
pub const MAX_ORDER_CANCEL_ALL_PERIOD: i64 = 1000 * 60 * 60 * 24 * 15; // 15 days

// Order expiry bounds
pub const NIL_ORDER_EXPIRY: i64 = 0;
pub const MIN_ORDER_EXPIRY: i64 = 1;
pub const MAX_ORDER_EXPIRY: i64 = i64::MAX;

// Order expiry period bounds
pub const MIN_ORDER_EXPIRY_PERIOD: i64 = 1000 * 60 * 5; // 5 minutes
pub const MAX_ORDER_EXPIRY_PERIOD: i64 = 1000 * 60 * 60 * 24 * 30; // 30 days

// Order trigger price bounds
pub const NIL_ORDER_TRIGGER_PRICE: u32 = 0;
pub const MIN_ORDER_TRIGGER_PRICE: u32 = 1;
pub const MAX_ORDER_TRIGGER_PRICE: u32 = u32::MAX;

// Grouped orders
pub const MAX_GROUPED_ORDER_COUNT: i64 = 3;

// Timestamp
pub const MAX_TIMESTAMP: i64 = (1 << 48) - 1;

// Exchange USDC limits
pub const MAX_EXCHANGE_USDC: i64 = (1 << 60) - 1;
pub const MIN_TRANSFER_AMOUNT: i64 = 1;
pub const MAX_TRANSFER_AMOUNT: i64 = MAX_EXCHANGE_USDC;
pub const MIN_WITHDRAWAL_AMOUNT: u64 = 1;
pub const MAX_WITHDRAWAL_AMOUNT: u64 = MAX_EXCHANGE_USDC as u64;

// Chain IDs
pub const MAINNET_CHAIN_ID: u32 = 304;
pub const TESTNET_CHAIN_ID: u32 = 300;

// Signature / key lengths
pub const SIGNATURE_LENGTH: usize = 80;
pub const L1_SIGNATURE_LENGTH: usize = 65;
pub const PUB_KEY_LENGTH: usize = 40;

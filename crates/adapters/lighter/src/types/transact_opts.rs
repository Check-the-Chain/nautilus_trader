#[derive(Debug, Clone, Default)]
pub struct TransactOpts {
    pub from_account_index: Option<i64>,
    pub api_key_index: Option<u8>,
    pub expired_at: i64,
    pub nonce: Option<i64>,
    pub dry_run: bool,
}

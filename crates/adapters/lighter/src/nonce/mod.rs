pub mod api;
pub mod manager;
pub mod optimistic;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceManagerType {
    Optimistic,
    Api,
}

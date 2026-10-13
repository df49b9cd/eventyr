//! The domain: where the bank's rules live, independent of delivery.

pub mod account;

pub use account::{
    Account, AccountCommand, AccountError, AccountEvent, AccountId, AccountState, Cash, Deposited,
    Opened, Withdrawn,
};

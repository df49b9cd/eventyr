//! The account aggregate: open it, credit it, debit it. The one rule a
//! bank cannot break is here — an account never goes below zero.
//!
//! The payloads carry the account id so `Tagged` can derive each event's
//! tag from the payload itself (the §8 sourcery pattern): a stored tag
//! would be wrong for every event written before the type gained one.

use std::fmt;
use std::str::FromStr;

use eventyr::prelude::*;

/// A cents-denominated amount. Banks count in cents, so the sample
/// does too; `u64` because an account never holds a negative amount.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct Cash(pub u64);

impl Cash {
    /// Zero.
    pub const ZERO: Self = Self(0);

    /// A positive amount: the only kind a command may move.
    pub const fn is_positive(self) -> bool {
        self.0 > 0
    }
}

impl fmt::Display for Cash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// An account instance's identifier.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(transparent)]
pub struct AccountId(pub u64);

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for AccountId {
    type Err = std::num::ParseIntError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.parse().map(AccountId)
    }
}

/// The account was opened.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Opened {
    /// The account holder's name.
    pub owner: String,
}

/// Cash came in.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Deposited {
    /// The account: the event tags itself from its payload.
    pub account: u64,
    /// The amount, in cents.
    pub amount: Cash,
}

/// Cash went out.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Withdrawn {
    /// The account.
    pub account: u64,
    /// The amount, in cents.
    pub amount: Cash,
}

/// The account's commands.
#[derive(Clone, Debug)]
pub enum AccountCommand {
    /// Open the account.
    Open {
        /// The account holder's name.
        owner: String,
    },
    /// Deposit a positive amount.
    Deposit {
        /// The amount, in cents.
        amount: Cash,
    },
    /// Withdraw a positive amount, never more than the balance.
    Withdraw {
        /// The amount, in cents.
        amount: Cash,
    },
}

/// The account's rejections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountError {
    /// The account already exists.
    AlreadyOpen,
    /// The account does not exist yet.
    NotOpen,
    /// An amount must be positive.
    BadAmount,
    /// The balance does not cover the withdrawal.
    InsufficientFunds,
}

impl fmt::Display for AccountError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AlreadyOpen => "account is already open",
            Self::NotOpen => "account is not open",
            Self::BadAmount => "amount must be positive",
            Self::InsufficientFunds => "insufficient funds",
        })
    }
}

impl std::error::Error for AccountError {}

/// The account's folded state. The id rides along so `decide` — which
/// the trait keeps to state and command — can put it into the payloads.
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AccountState {
    id: AccountId,
    open: bool,
    balance: u64,
}

impl AccountState {
    fn opened(id: &AccountId) -> Self {
        Self {
            id: *id,
            ..Self::default()
        }
    }

    /// The current balance, in cents.
    pub fn balance(&self) -> Cash {
        Cash(self.balance)
    }
}

fn apply(state: &mut AccountState, event: &AccountEvent) {
    match event {
        AccountEvent::Opened(Opened { .. }) => state.open = true,
        AccountEvent::Deposited(Deposited { amount, .. }) => state.balance += amount.0,
        AccountEvent::Withdrawn(Withdrawn { amount, .. }) => state.balance -= amount.0,
    }
}

fn decide(
    state: &AccountState,
    command: &AccountCommand,
) -> Result<Vec<AccountEvent>, AccountError> {
    let account = state.id.0;
    match command {
        AccountCommand::Open { .. } if state.open => Err(AccountError::AlreadyOpen),
        AccountCommand::Open { owner } => Ok(vec![
            Opened {
                owner: owner.clone(),
            }
            .into(),
        ]),
        AccountCommand::Deposit { .. } | AccountCommand::Withdraw { .. } if !state.open => {
            Err(AccountError::NotOpen)
        }
        AccountCommand::Deposit { amount } if !amount.is_positive() => Err(AccountError::BadAmount),
        AccountCommand::Deposit { amount } => Ok(vec![
            Deposited {
                account,
                amount: *amount,
            }
            .into(),
        ]),
        AccountCommand::Withdraw { amount } if !amount.is_positive() => {
            Err(AccountError::BadAmount)
        }
        AccountCommand::Withdraw { amount } if amount.0 > state.balance => {
            Err(AccountError::InsufficientFunds)
        }
        AccountCommand::Withdraw { amount } => Ok(vec![
            Withdrawn {
                account,
                amount: *amount,
            }
            .into(),
        ]),
    }
}

/// The account — one aggregate, three events, one rule: no overdraft.
#[derive(Aggregate)]
#[eventyr(
    id = AccountId,
    state = AccountState,
    error = AccountError,
    command = AccountCommand,
    initial = AccountState::opened(id),
    events(Opened, Deposited, Withdrawn),
    event_derive(serde::Serialize, serde::Deserialize)
)]
pub struct Account;

// Tags (roadmap 0.7.1): a pure function of the event's payload — a
// stored tag would be wrong for every event written before the type
// gained one. The fee saga reads through a filter on this tag.
impl Tagged for AccountEvent {
    fn tags(&self) -> Vec<Tag> {
        let account = match self {
            AccountEvent::Opened(_) => return Vec::new(),
            AccountEvent::Deposited(Deposited { account, .. })
            | AccountEvent::Withdrawn(Withdrawn { account, .. }) => *account,
        };
        vec![Tag::of("account", account)]
    }
}

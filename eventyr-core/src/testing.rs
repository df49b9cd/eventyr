//! Test support: the scripted driver and a canonical test aggregate.
//!
//! Everything here is pure — no store, no async, no I/O — so it lives in
//! the core and serves every crate's test suite.

use alloc::vec::Vec;

use crate::aggregate::Aggregate;
use crate::write::{WriteAction, WriteInput, WriteMachine};

/// Drives `machine` through `start()` and every input in `script`,
/// recording each action in order.
///
/// The script is fed verbatim: inputs after the machine finished are
/// answered with protocol-violation outcomes, exactly as they would be
/// at runtime — which is itself worth asserting on.
pub fn drive_scripted<A: Aggregate>(
    machine: &mut WriteMachine<A>,
    script: impl IntoIterator<Item = WriteInput<A::Event>>,
) -> Vec<WriteAction<A::Event, A::Error>> {
    let mut actions = Vec::new();
    actions.push(machine.start());
    for input in script {
        actions.push(machine.handle(input));
    }
    actions
}

/// A canonical bank-account aggregate for tests, doctests, and examples.
///
/// One shared definition so every crate's test suite exercises the same
/// domain: open, deposit, withdraw, and a no-op balance check (which
/// exercises the `Noop` outcome). Doctests stay self-contained — they
/// must read standalone.
pub mod account {
    use alloc::string::String;
    use alloc::vec;
    use alloc::vec::Vec;
    use core::fmt;

    use crate::aggregate::Aggregate;

    /// The account instance's identifier.
    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct AccountId(
        /// The raw id value.
        pub u64,
    );

    impl fmt::Display for AccountId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// The account's domain events.
    #[derive(Clone, PartialEq, Debug)]
    pub enum AccountEvent {
        /// The account was opened by `owner`.
        Opened {
            /// The owner's name.
            owner: String,
        },
        /// Funds arrived.
        Deposited {
            /// How much.
            amount: u64,
        },
        /// Funds left.
        Withdrawn {
            /// How much.
            amount: u64,
        },
    }

    /// The account's commands.
    #[derive(Clone, Debug)]
    pub enum AccountCommand {
        /// Open the account.
        Open {
            /// The owner's name.
            owner: String,
        },
        /// Deposit funds.
        Deposit {
            /// How much.
            amount: u64,
        },
        /// Withdraw funds.
        Withdraw {
            /// How much.
            amount: u64,
        },
        /// Check the balance: decides no events (the `Noop` outcome).
        CheckBalance,
    }

    /// The account's domain rejections.
    #[derive(Debug, PartialEq)]
    pub enum AccountError {
        /// The account is already open.
        AlreadyOpen,
        /// The account is not open yet.
        NotOpen,
        /// The balance is too low.
        InsufficientFunds,
    }

    impl fmt::Display for AccountError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(match self {
                AccountError::AlreadyOpen => "account is already open",
                AccountError::NotOpen => "account is not open",
                AccountError::InsufficientFunds => "insufficient funds",
            })
        }
    }

    /// The account's folded state.
    #[derive(Debug)]
    pub struct AccountState {
        /// Whether the account exists.
        pub open: bool,
        /// The current balance.
        pub balance: u64,
    }

    /// The account aggregate: a unit struct, as aggregates usually are.
    pub struct Account;

    impl Aggregate for Account {
        const NAME: &'static str = "account";
        type Id = AccountId;
        type State = AccountState;
        type Event = AccountEvent;
        type Command = AccountCommand;
        type Error = AccountError;

        fn initial(_id: &Self::Id) -> Self::State {
            AccountState {
                open: false,
                balance: 0,
            }
        }

        fn apply(state: &mut Self::State, event: &Self::Event) {
            match event {
                AccountEvent::Opened { .. } => {
                    state.open = true;
                    state.balance = 0;
                }
                AccountEvent::Deposited { amount } => state.balance += amount,
                AccountEvent::Withdrawn { amount } => state.balance -= amount,
            }
        }

        fn decide(
            state: &Self::State,
            command: &Self::Command,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            match command {
                AccountCommand::Open { .. } if state.open => Err(AccountError::AlreadyOpen),
                AccountCommand::Open { owner } => Ok(vec![AccountEvent::Opened {
                    owner: owner.clone(),
                }]),
                AccountCommand::Deposit { .. }
                | AccountCommand::Withdraw { .. }
                | AccountCommand::CheckBalance
                    if !state.open =>
                {
                    Err(AccountError::NotOpen)
                }
                AccountCommand::Deposit { amount } => {
                    Ok(vec![AccountEvent::Deposited { amount: *amount }])
                }
                AccountCommand::Withdraw { amount } if *amount > state.balance => {
                    Err(AccountError::InsufficientFunds)
                }
                AccountCommand::Withdraw { amount } => {
                    Ok(vec![AccountEvent::Withdrawn { amount: *amount }])
                }
                AccountCommand::CheckBalance => Ok(vec![]),
            }
        }
    }
}

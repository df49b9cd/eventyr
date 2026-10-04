//! End-to-end tests for the derives: the conventions, every override,
//! the generated event enum, and the derived aggregate driven through
//! the real `WriteMachine`.

use std::fmt;

use eventyr_core::prelude::*;
use eventyr_macros::{Aggregate, EventName};

/// The id type the override fixtures share — only the convention module
/// defines its own, since it tests the `{Ident}Id` naming.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TestId(pub u64);
impl fmt::Display for TestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// -- the canonical convention shape ------------------------------------

mod convention {
    use super::*;

    #[derive(Clone, PartialEq, Eq, Hash, Debug)]
    pub struct CounterId(pub u64);
    impl fmt::Display for CounterId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// A domain event payload.
    #[derive(Clone, Debug, PartialEq)]
    pub struct Incremented {
        pub by: u32,
    }

    #[derive(Debug)]
    pub enum CounterCommand {
        Increment(u32),
    }

    #[derive(Debug, PartialEq)]
    pub enum CounterError {
        TooHigh,
    }
    impl fmt::Display for CounterError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("counter would go too high")
        }
    }

    #[derive(Debug, Default, PartialEq)]
    pub struct CounterState {
        pub count: u32,
    }

    pub fn apply(state: &mut CounterState, event: &CounterEvent) {
        match event {
            CounterEvent::Incremented(Incremented { by }) => state.count += by,
        }
    }

    pub fn decide(
        state: &CounterState,
        command: &CounterCommand,
    ) -> Result<Vec<CounterEvent>, CounterError> {
        match command {
            CounterCommand::Increment(by) if state.count + by > 100 => Err(CounterError::TooHigh),
            CounterCommand::Increment(by) => Ok(vec![Incremented { by: *by }.into()]),
        }
    }

    #[derive(Aggregate)]
    #[eventyr(state = CounterState, events(Incremented))]
    pub struct Counter;
}

#[test]
fn conventions_wire_the_trait() {
    use convention::*;

    assert_eq!(Counter::NAME, "counter");
    assert_eq!(Counter::initial(&CounterId(7)), CounterState { count: 0 },);
    let mut state = CounterState { count: 1 };
    Counter::apply(
        &mut state,
        &CounterEvent::Incremented(Incremented { by: 2 }),
    );
    assert_eq!(state.count, 3);
    assert_eq!(
        Counter::decide(&CounterState { count: 1 }, &CounterCommand::Increment(2))
            .unwrap()
            .len(),
        1,
    );
    assert_eq!(
        Counter::decide(&CounterState { count: 99 }, &CounterCommand::Increment(5)),
        Err(CounterError::TooHigh),
    );
}

#[test]
fn the_event_enum_names_variants_after_payloads() {
    use convention::*;

    let event = CounterEvent::Incremented(Incremented { by: 2 });
    assert_eq!(event.event_name(), "Incremented");
    // The `From<Payload>` conversion is what `decide` builds events with.
    let converted: CounterEvent = Incremented { by: 2 }.into();
    assert_eq!(converted, event);
}

#[test]
fn the_derived_aggregate_drives_the_write_machine() {
    use convention::*;

    let mut machine = WriteMachine::<Counter>::new(
        CounterId(7),
        CounterCommand::Increment(5),
        RetryPolicy::default(),
    );

    let WriteAction::LoadStream { stream_id, from } = machine.start() else {
        panic!("expected a load action")
    };
    assert_eq!(stream_id.as_str(), "counter-7");
    assert_eq!(from, Version::EMPTY);

    let action = machine.handle(WriteInput::Loaded { events: vec![] });
    let WriteAction::Append {
        expected, events, ..
    } = action
    else {
        panic!("expected an append action")
    };
    assert_eq!(expected, ExpectedVersion::Empty);
    assert_eq!(events.len(), 1);

    let action = machine.handle(WriteInput::Appended { committed: vec![] });
    assert!(matches!(
        action,
        WriteAction::Done(WriteOutcome::Committed { .. })
    ));
}

// -- every override ----------------------------------------------------

mod overrides {
    use super::*;

    /// A domain event payload.
    #[derive(Clone, Debug, PartialEq)]
    pub struct Credited {
        pub amount: u64,
    }

    #[derive(Debug)]
    pub enum TopUp {
        Add(u64),
    }

    #[derive(Debug, PartialEq)]
    #[allow(dead_code)]
    pub enum WalletRejection {
        Closed,
    }
    impl fmt::Display for WalletRejection {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("wallet is closed")
        }
    }

    #[derive(Debug)]
    pub struct WalletState {
        pub balance: u64,
    }

    pub fn new_wallet(_id: &TestId) -> WalletState {
        WalletState { balance: 100 }
    }

    pub fn fold(state: &mut WalletState, event: &WalletEvent) {
        match event {
            WalletEvent::Credited(Credited { amount }) => state.balance += amount,
        }
    }

    pub fn decide_top_up(
        _state: &WalletState,
        command: &TopUp,
    ) -> Result<Vec<WalletEvent>, WalletRejection> {
        match command {
            TopUp::Add(amount) => Ok(vec![Credited { amount: *amount }.into()]),
        }
    }

    #[derive(Aggregate)]
    #[eventyr(
        name = "wallet",
        id = TestId,
        state = WalletState,
        event_enum = WalletEvent,
        command = TopUp,
        error = WalletRejection,
        initial = new_wallet(id),
        apply = fold,
        decide = decide_top_up,
        events(Credited),
    )]
    pub struct MyWallet;
}

#[test]
fn overrides_replace_every_convention() {
    use overrides::*;

    assert_eq!(MyWallet::NAME, "wallet");
    assert_eq!(MyWallet::initial(&TestId(1)).balance, 100);
    let mut state = MyWallet::initial(&TestId(1));
    MyWallet::apply(&mut state, &WalletEvent::Credited(Credited { amount: 5 }));
    assert_eq!(state.balance, 105);
    assert_eq!(
        MyWallet::decide(&state, &TopUp::Add(5)).unwrap(),
        vec![WalletEvent::Credited(Credited { amount: 5 })],
    );
    // The stream id follows the overridden name.
    assert_eq!(
        StreamId::for_aggregate::<MyWallet>(&TestId(3)).as_str(),
        "wallet-3",
    );
}

// -- the unit-struct-is-its-own-state shape -----------------------------

mod unit_state {
    use super::*;

    #[derive(Clone, Debug, PartialEq)]
    pub struct Toggled;

    #[derive(Debug)]
    pub struct Toggle;

    #[derive(Debug, PartialEq)]
    pub struct NoRejection;
    impl fmt::Display for NoRejection {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("unreachable")
        }
    }

    pub fn apply(_state: &mut Flag, _event: &FlagEvent) {}

    pub fn decide(_state: &Flag, _command: &Toggle) -> Result<Vec<FlagEvent>, NoRejection> {
        Ok(vec![Toggled.into()])
    }

    #[derive(Aggregate)]
    #[eventyr(
        id = TestId,
        command = Toggle,
        error = NoRejection,
        events(Toggled),
    )]
    pub struct Flag;
}

#[test]
fn a_unit_struct_is_its_own_state() {
    use unit_state::*;

    assert_eq!(Flag::NAME, "flag");
    // `State = Self`, `initial = Self`: the marker is the state.
    let mut state = Flag;
    Flag::apply(&mut state, &FlagEvent::Toggled(Toggled));
    assert_eq!(
        Flag::decide(&state, &Toggle).unwrap(),
        vec![FlagEvent::Toggled(Toggled)],
    );
}

// -- EventName ----------------------------------------------------------

#[test]
fn event_name_names_variants_after_themselves() {
    #[derive(EventName)]
    #[allow(dead_code)]
    enum TransferEvent {
        Started,
        #[eventyr(name = "transfer.completed")]
        Completed {
            amount: u64,
        },
    }

    assert_eq!(TransferEvent::Started.event_name(), "Started");
    assert_eq!(
        TransferEvent::Completed { amount: 1 }.event_name(),
        "transfer.completed",
    );
}

#[test]
fn event_name_on_a_struct_names_the_type() {
    #[derive(EventName)]
    #[eventyr(name = "order.placed")]
    #[allow(dead_code)]
    struct OrderPlaced {
        order: u64,
    }

    assert_eq!(OrderPlaced { order: 1 }.event_name(), "order.placed");
}

#[test]
fn event_name_on_a_struct_defaults_to_the_type_name() {
    #[derive(EventName)]
    #[allow(dead_code)]
    struct OrderShipped {
        order: u64,
    }

    assert_eq!(OrderShipped { order: 1 }.event_name(), "OrderShipped");
}

// -- the umbrella-crate retarget ----------------------------------------

mod umbrella {
    use super::*;

    /// A domain event payload.
    #[derive(Clone, Debug, PartialEq)]
    pub struct ItemAdded {
        pub sku: u64,
    }

    #[derive(Debug)]
    pub enum CartCommand {
        Add(u64),
    }

    #[derive(Debug, PartialEq)]
    pub enum CartError {
        Full,
    }
    impl fmt::Display for CartError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("cart is full")
        }
    }

    #[derive(Debug, Default)]
    pub struct CartState {
        pub items: u32,
    }

    pub fn apply(state: &mut CartState, event: &CartEvent) {
        match event {
            CartEvent::ItemAdded(ItemAdded { .. }) => state.items += 1,
        }
    }

    pub fn decide(state: &CartState, command: &CartCommand) -> Result<Vec<CartEvent>, CartError> {
        match command {
            CartCommand::Add(_) if state.items >= 10 => Err(CartError::Full),
            CartCommand::Add(sku) => Ok(vec![ItemAdded { sku: *sku }.into()]),
        }
    }

    #[derive(Aggregate)]
    #[eventyr(crate = "eventyr", id = TestId, state = CartState, events(ItemAdded))]
    pub struct Cart;
}

#[test]
fn the_umbrella_crate_retarget_resolves() {
    use umbrella::*;

    assert_eq!(Cart::NAME, "cart");
    assert_eq!(
        Cart::decide(&CartState { items: 0 }, &CartCommand::Add(1)).unwrap(),
        vec![CartEvent::ItemAdded(ItemAdded { sku: 1 })],
    );
    assert_eq!(
        CartEvent::ItemAdded(ItemAdded { sku: 1 }).event_name(),
        "ItemAdded"
    );
}

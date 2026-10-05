//! Event types the contracts are written against, for stores to plug
//! in: a store whose events must be serializable (every durable store)
//! uses these instead of declaring its own, behind this crate's `serde`
//! feature.

use eventyr_core::event_name::EventName;

/// The suite's payload for [`ContractEvent`](crate::ContractEvent) stores: one
/// variant carrying the `u64` the checks append. Its stored name is
/// always `"Payload"`, and with the `serde` feature it serializes as
/// `{"Payload":{"value":N}}` — the shape raw-SQL tests insert.
#[derive(Clone, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum PayloadEvent {
    /// The value a check appended.
    Payload {
        /// The appended value.
        value: u64,
    },
}

impl EventName for PayloadEvent {
    fn event_name(&self) -> &'static str {
        match self {
            PayloadEvent::Payload { .. } => "Payload",
        }
    }
}

impl From<u64> for PayloadEvent {
    fn from(value: u64) -> Self {
        PayloadEvent::Payload { value }
    }
}

/// The payload [`filtered_read_contract`](crate::filtered_read_contract)
/// needs: a `u64` whose stored name depends on it — even values are
/// `"Even"`, odd ones `"Odd"`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ParityEvent(pub u64);

impl EventName for ParityEvent {
    fn event_name(&self) -> &'static str {
        if self.0.is_multiple_of(2) {
            "Even"
        } else {
            "Odd"
        }
    }
}

impl From<u64> for ParityEvent {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

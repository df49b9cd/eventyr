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

/// An event that counts how often it is decoded — the fixture for a
/// store's page-bound read checks (a short global read must not decode
/// the whole tail). The counter is global; call
/// [`Counted::reset_decodes`] before the check that reads it.
#[derive(Clone, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct Counted(pub u64);

#[cfg(feature = "serde")]
static DECODED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Counted {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        DECODED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        <u64 as serde::Deserialize>::deserialize(deserializer).map(Self)
    }
}

impl EventName for Counted {
    fn event_name(&self) -> &'static str {
        "Counted"
    }
}

#[cfg(feature = "serde")]
impl Counted {
    /// How many decodes the counter has seen since the last reset.
    pub fn decodes() -> usize {
        DECODED.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Zero the decode counter — call before the check that reads it.
    pub fn reset_decodes() {
        DECODED.store(0, std::sync::atomic::Ordering::SeqCst);
    }
}

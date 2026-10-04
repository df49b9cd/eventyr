//! Stable event type names — the storage-facing names of event variants.

/// A stable, human-readable name for each event variant.
///
/// This is the string a store persists in its event-type column and the
/// key the upcasting chain selects by (compare
/// [`RawEvent::event_type`](crate::upcast::RawEvent)). Stored names are
/// part of the storage schema: renaming a Rust variant changes its
/// name, so pin the stored name with `#[eventyr(name = "...")]` when
/// using `#[derive(EventName)]` from `eventyr-macros`.
///
/// The hand-written equivalent (everything the derive generates is
/// writable by hand):
///
/// ```
/// use eventyr_core::event_name::EventName;
///
/// #[derive(Debug, PartialEq)]
/// enum TransferEvent {
///     Started,
///     Completed,
/// }
///
/// impl EventName for TransferEvent {
///     fn event_name(&self) -> &'static str {
///         match self {
///             Self::Started => "Started",
///             Self::Completed => "Completed",
///         }
///     }
/// }
///
/// assert_eq!(TransferEvent::Started.event_name(), "Started");
/// assert_eq!(TransferEvent::Completed.event_name(), "Completed");
/// ```
pub trait EventName {
    /// The stable storage name of this event.
    fn event_name(&self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::EventName;
    use crate::testing::account::AccountEvent;
    use alloc::string::String;

    #[test]
    fn the_canonical_account_names_its_events() {
        let opened = AccountEvent::Opened {
            owner: String::from("me"),
        };
        assert_eq!(opened.event_name(), "Opened");
        assert_eq!(
            AccountEvent::Deposited { amount: 1 }.event_name(),
            "Deposited"
        );
        assert_eq!(
            AccountEvent::Withdrawn { amount: 1 }.event_name(),
            "Withdrawn"
        );
    }
}

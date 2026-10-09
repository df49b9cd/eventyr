//! Upcasting: transforming historical event shapes into the current one.

use alloc::string::String;
use alloc::vec::Vec;

use crate::error::UpcastError;
use crate::version_registry::EventSchemaVersion;

/// A stored event before upcasting: its type name, payload, and the
/// schema version it was written at.
///
/// The upcaster registry (`eventyr-projection`'s `registry`) reads `schema_version` to walk the
/// event type's version ladder; a stored event that predates versioning
/// carries [`EventSchemaVersion::V1`], the shape the code started with.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct RawEvent {
    /// The stored event type name.
    pub event_type: String,
    /// The schema version `payload` was written at. [`EventSchemaVersion::V1`]
    /// for payloads that predate versioning.
    pub schema_version: EventSchemaVersion,
    /// The stored payload, as raw bytes.
    pub payload: Vec<u8>,
}

impl RawEvent {
    /// A `V1` event: the shape the code started with, before any
    /// upcaster.
    pub fn v1(event_type: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        Self {
            event_type: event_type.into(),
            schema_version: EventSchemaVersion::V1,
            payload: payload.into(),
        }
    }
}

/// Transforms one historical event shape into the current one.
///
/// The chain (roadmap 0.2) selects upcasters by event
/// type; a selected upcaster that cannot parse its payload is an
/// [`Err`](UpcastError) — never a silent drop.
///
/// # Examples
///
/// The contract in miniature: a `V1` payload upcast into the current
/// event, and a payload this upcaster does not recognize failing
/// loudly instead of dropping the event:
///
/// ```
/// use eventyr_core::error::UpcastError;
/// use eventyr_core::upcast::{RawEvent, Upcaster};
///
/// #[derive(Debug, PartialEq)]
/// enum BankEvent { Deposited(u64) }
///
/// struct ParseAmount;
/// impl Upcaster<BankEvent> for ParseAmount {
///     fn upcast(&self, raw: RawEvent) -> Result<BankEvent, UpcastError> {
///         match raw.event_type.as_str() {
///             "Deposited" => {
///                 let amount = core::str::from_utf8(&raw.payload)
///                     .ok()
///                     .and_then(|text| text.parse().ok())
///                     .ok_or_else(|| UpcastError {
///                         event_type: raw.event_type.clone(),
///                         message: "the payload is not an amount".into(),
///                     })?;
///                 Ok(BankEvent::Deposited(amount))
///             }
///             other => Err(UpcastError {
///                 event_type: other.to_owned(),
///                 message: "not this upcaster's event type".into(),
///             }),
///         }
///     }
/// }
///
/// let upcaster = ParseAmount;
/// let upcast = upcaster.upcast(RawEvent::v1("Deposited", b"50")).expect("upcast");
/// assert_eq!(upcast, BankEvent::Deposited(50));
///
/// // A type this upcaster does not know: a loud miss, never a drop.
/// let miss = upcaster.upcast(RawEvent::v1("Withdrawn", b"10"));
/// assert!(miss.is_err());
/// ```
pub trait Upcaster<E>: Send + Sync {
    /// Transform `raw` into the current event shape.
    ///
    /// # Errors
    ///
    /// [`UpcastError`] naming the event type when the stored payload is
    /// not the shape this upcaster knows — a loud miss, never a silent
    /// drop.
    fn upcast(&self, raw: RawEvent) -> Result<E, UpcastError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chain's selection step, in miniature: pick the upcaster by
    /// event type, and let its failure be loud.
    fn select<'a>(
        upcasters: &[(&'a str, &'a dyn Upcaster<u64>)],
        raw: &RawEvent,
    ) -> Result<u64, UpcastError> {
        let (_, upcaster) = upcasters
            .iter()
            .find(|(event_type, _)| *event_type == raw.event_type)
            .ok_or_else(|| UpcastError {
                event_type: raw.event_type.clone(),
                message: "no upcaster for this event type".into(),
            })?;
        upcaster.upcast(RawEvent {
            event_type: raw.event_type.clone(),
            schema_version: raw.schema_version,
            payload: raw.payload.clone(),
        })
    }

    struct ParseAmount;

    impl Upcaster<u64> for ParseAmount {
        fn upcast(&self, raw: RawEvent) -> Result<u64, UpcastError> {
            let payload = core::str::from_utf8(&raw.payload)
                .map_err(|_| UpcastError {
                    event_type: raw.event_type.clone(),
                    message: "payload is not utf-8".into(),
                })?
                .parse()
                .map_err(|_| UpcastError {
                    event_type: raw.event_type,
                    message: "payload is not a number".into(),
                })?;
            Ok(payload)
        }
    }

    #[test]
    fn a_selected_upcaster_parses_its_payload() {
        let upcasters: &[(&str, &dyn Upcaster<u64>)] = &[("AmountV1", &ParseAmount)];
        let raw = RawEvent {
            event_type: "AmountV1".into(),
            schema_version: crate::version_registry::EventSchemaVersion::V1,
            payload: b"42".to_vec(),
        };
        assert_eq!(select(upcasters, &raw).expect("parses"), 42);
    }

    #[test]
    fn a_selected_upcaster_fails_loudly_never_drops() {
        let upcasters: &[(&str, &dyn Upcaster<u64>)] = &[("AmountV1", &ParseAmount)];
        let raw = RawEvent {
            event_type: "AmountV1".into(),
            schema_version: crate::version_registry::EventSchemaVersion::V1,
            payload: b"not a number".to_vec(),
        };
        let error = select(upcasters, &raw).expect_err("a bad payload is data loss, not a skip");
        assert_eq!(error.event_type, "AmountV1");
    }

    #[test]
    fn an_unselected_event_type_is_an_error_not_a_skip() {
        let upcasters: &[(&str, &dyn Upcaster<u64>)] = &[("AmountV1", &ParseAmount)];
        let raw = RawEvent {
            event_type: "AmountV0".into(),
            schema_version: crate::version_registry::EventSchemaVersion::V1,
            payload: b"42".to_vec(),
        };
        let error = select(upcasters, &raw).expect_err("no upcaster selected");
        assert_eq!(error.event_type, "AmountV0");
    }
}

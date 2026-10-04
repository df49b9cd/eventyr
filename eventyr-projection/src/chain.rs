//! The upcaster chain: selection by event type, lifted from the
//! private test helper in `eventyr-core`'s `upcast` module into a real,
//! composable type (DESIGN.md §3: read-side glue lives store-side).
//!
//! The contract is [`Upcaster`]'s own: a selected upcaster that cannot
//! parse its payload — and an event type with no registered upcaster —
//! is an [`Err`](UpcastError), never a silent drop. A projection fed
//! through a chain either sees every event correctly upcast or stops
//! loudly; there is no skip, because silently dropping a stored fact is
//! data loss.
//!
//! Lookup is a linear scan over registration order. The number of
//! upcasters per chain is tiny (one per historical shape), and explicit
//! order keeps version ladders readable; an indexed map is premature.

use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

use eventyr_core::error::UpcastError;
use eventyr_core::upcast::{RawEvent, Upcaster};

// Manual `Default`/`Clone`: the derives would impose undesired
// `E: Default`/`E: Clone` bounds on a chain of trait objects.
impl<E> Default for UpcasterChain<E> {
    fn default() -> Self {
        Self {
            upcasters: Vec::new(),
        }
    }
}

impl<E> Clone for UpcasterChain<E> {
    fn clone(&self) -> Self {
        Self {
            upcasters: self.upcasters.clone(),
        }
    }
}

/// Selects a registered upcaster by [`RawEvent::event_type`] and applies it.
///
/// A miss on the event type, or the selected upcaster's own failure, is
/// a loud [`UpcastError`] carrying the event type — the error surfaces
/// through [`UpcastingSource`](crate::source::UpcastingSource) as a
/// store failure, so the subscription machine aborts instead of acking
/// past a poison event.
///
/// A chain is itself an [`Upcaster`], so chains nest: a read model's
/// terminal chain can embed a shared domain chain as one entry.
pub struct UpcasterChain<E> {
    upcasters: Vec<(String, Arc<dyn Upcaster<E>>)>,
}

impl<E> UpcasterChain<E> {
    /// An empty chain: every event type is a miss until one is registered.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `upcaster` for `event_type`, builder-style.
    ///
    /// A later registration for the same event type shadows the earlier
    /// one (the scan finds it first) — registering wide-open shapes
    /// first and overrides last composes like a patch list.
    pub fn with<U>(mut self, event_type: &str, upcaster: U) -> Self
    where
        U: Upcaster<E> + 'static,
    {
        // Latest registration wins: scan order is registration order,
        // so the override must be found first.
        self.upcasters
            .insert(0, (event_type.to_owned(), Arc::new(upcaster)));
        self
    }

    /// Upcast `raw` through the registered entry for its event type.
    ///
    /// # Errors
    ///
    /// [`UpcastError`] naming the event type when no upcaster is
    /// registered for it, or the registered upcaster's own error,
    /// propagated unwrapped.
    pub fn upcast(&self, raw: RawEvent) -> Result<E, UpcastError> {
        let (_, upcaster) = self
            .upcasters
            .iter()
            .find(|(event_type, _)| *event_type == raw.event_type)
            .ok_or_else(|| UpcastError {
                event_type: raw.event_type.clone(),
                message: "no upcaster registered for this event type".into(),
            })?;
        upcaster.upcast(raw)
    }

    /// How many entries are registered (not the number of event types —
    /// overrides count).
    pub fn len(&self) -> usize {
        self.upcasters.len()
    }

    /// Whether nothing is registered.
    pub fn is_empty(&self) -> bool {
        self.upcasters.is_empty()
    }
}

/// A chain is an upcaster: chains compose into chains.
impl<E> Upcaster<E> for UpcasterChain<E>
where
    E: Send + Sync,
{
    fn upcast(&self, raw: RawEvent) -> Result<E, UpcastError> {
        Self::upcast(self, raw)
    }
}

/// Adapts a step function `F: Fn(RawEvent) -> Result<E, UpcastError>`
/// into a named [`Upcaster`], so version ladders compose without a
/// bespoke struct per rung:
///
/// ```
/// # use eventyr_projection::prelude::*;
/// # use eventyr_core::prelude::RawEvent;
/// # use eventyr_core::error::UpcastError;
/// let chain = UpcasterChain::new().with(
///     "AmountV1",
///     ClosureUpcaster::new(|raw: RawEvent| -> Result<u64, UpcastError> {
///         // Parse the old shape, return the current one. A V2 -> V1
///         // rung for an older pair is just another `with` entry.
///         std::str::from_utf8(&raw.payload)
///             .ok()
///             .and_then(|text| text.parse().ok())
///             .ok_or_else(|| UpcastError {
///                 event_type: raw.event_type.clone(),
///                 message: "payload is not a number".into(),
///             })
///     }),
/// );
/// ```
///
/// The chain routes by `event_type`; the step body owns parsing, so the
/// crate takes no serde dependency.
pub struct ClosureUpcaster<F> {
    step: F,
}

impl<F> ClosureUpcaster<F> {
    /// Wrap a step function as an upcaster.
    pub fn new(step: F) -> Self {
        Self { step }
    }
}

impl<E, F> Upcaster<E> for ClosureUpcaster<F>
where
    F: Fn(RawEvent) -> Result<E, UpcastError> + Send + Sync,
    E: Send + Sync,
{
    fn upcast(&self, raw: RawEvent) -> Result<E, UpcastError> {
        (self.step)(raw)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_amount(raw: RawEvent) -> Result<u64, UpcastError> {
        std::str::from_utf8(&raw.payload)
            .ok()
            .and_then(|text| text.parse().ok())
            .ok_or_else(|| UpcastError {
                event_type: raw.event_type.clone(),
                message: "payload is not a utf-8 number".into(),
            })
    }

    fn raw(event_type: &str, payload: &str) -> RawEvent {
        RawEvent {
            event_type: event_type.into(),
            schema_version: eventyr_core::version_registry::EventSchemaVersion::V1,
            payload: payload.as_bytes().to_vec(),
        }
    }

    /// The chain's own tests: selection, loud misses, loud failures.

    #[test]
    fn a_registered_event_type_upcasts() {
        let chain = UpcasterChain::new().with("AmountV1", ClosureUpcaster::new(parse_amount));
        assert_eq!(chain.upcast(raw("AmountV1", "42")).expect("upcast"), 42);
    }

    #[test]
    fn a_miss_is_a_loud_error_naming_the_event_type() {
        let chain = UpcasterChain::new().with("AmountV1", ClosureUpcaster::new(parse_amount));
        let error = chain
            .upcast(raw("AmountV0", "42"))
            .expect_err("an unregistered type is never skipped");
        assert_eq!(error.event_type, "AmountV0");
    }

    #[test]
    fn a_registered_upcasters_failure_propagates_unwrapped() {
        let chain = UpcasterChain::new().with("AmountV1", ClosureUpcaster::new(parse_amount));
        let error = chain
            .upcast(raw("AmountV1", "not a number"))
            .expect_err("a bad payload is loud");
        assert_eq!(error.event_type, "AmountV1");
        assert_eq!(error.message, "payload is not a utf-8 number");
    }

    #[test]
    fn the_chain_is_itself_an_upcaster_so_chains_nest() {
        // Nesting merges chains: the inner chain handles the event
        // types it was built for, and the outer adds its own alongside.
        let amount_v1 = UpcasterChain::new().with("AmountV1", ClosureUpcaster::new(parse_amount));
        let chain = UpcasterChain::new()
            .with("AmountV1", amount_v1)
            .with("Other", ClosureUpcaster::new(|_| Ok(0)));
        assert_eq!(
            Upcaster::upcast(&chain, raw("AmountV1", "7")).expect("nested upcast"),
            7
        );
        assert_eq!(
            chain.upcast(raw("Other", "anything")).expect("own entry"),
            0
        );
    }

    #[test]
    fn a_version_ladder_composes_rung_by_rung() {
        // A1 -> A2 doubles; A2 -> A3 (the current shape) adds one.
        let chain = UpcasterChain::new()
            .with(
                "AmountV2",
                ClosureUpcaster::new(|raw: RawEvent| parse_amount(raw).map(|old| old + 1)),
            )
            .with(
                "AmountV1",
                ClosureUpcaster::new(|raw: RawEvent| parse_amount(raw).map(|a1| a1 * 2 + 1)),
            );
        assert_eq!(chain.upcast(raw("AmountV1", "3")).expect("ladder"), 7);
        assert_eq!(chain.upcast(raw("AmountV2", "3")).expect("ladder"), 4);
    }

    #[test]
    fn a_later_registration_shadows_an_earlier_one() {
        let chain = UpcasterChain::new()
            .with("AmountV1", ClosureUpcaster::new(|_| Ok(1)))
            .with("AmountV1", ClosureUpcaster::new(|_| Ok(2)));
        assert_eq!(chain.upcast(raw("AmountV1", "x")).expect("shadowed"), 2);
        assert_eq!(chain.len(), 2);
        assert!(!chain.is_empty());
        assert!(UpcasterChain::<u64>::new().is_empty());
    }
}

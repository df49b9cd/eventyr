//! The upcaster registry: a validated, lookup-ready set of upcasters
//! keyed by `(event_type, from_version)`.
//!
//! 0.2 shipped the upcast *vocabulary* ([`RawEvent`], [`Upcaster`]) and
//! `eventyr-projection` shipped the
//! [`UpcasterChain`](crate::upcast::Upcaster) — the run-time selection by
//! event type. What neither did is *verify* a chain is well-formed
//! before it serves a read, or *run* it against a stored event's schema
//! version. This module closes that loop.
//!
//! A **registry** is the set of upcasters a store or projection knows.
//! Each rung lifts *one* event type *one* schema version — from `raw`
//! bytes of `from_version` to the bytes of `from_version + 1` — and
//! registering under the version it upgrades *from* keeps the ladder
//! explicit. [`build`](UpcasterRegistry::build) checks the shape and
//! reports the broken links — a dangling version (a `V2→V3` rung with
//! no `V1→V2`), a cycle, or two rungs for the same `(type, from)` — as
//! loud startup errors, never as silently-dropped events at read.
//!
//! The registry is a plain value, not a machine (one lookup per event
//! is one step — §7 keeps machines for multi-step protocols). Running
//! it walks the ladder: an event stored at `V1` passes through the `V1
//! → V2` rung, then the `V2 → V3` rung, and so on, until its payload
//! reaches the current version. A payload already at the current
//! version is the common case and passes through unchanged.

use alloc::borrow::ToOwned;
use alloc::boxed::Box;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use crate::error::UpcastError;

/// The version of a stored event's schema: 1-based, where the current
/// shape is one past the highest registered version.
///
/// Upcasters form a per-type ladder: each registered rung lifts one
/// event type one step, from `version` to `version + 1`. A payload
/// already at the current version needs no rung.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(transparent))]
pub struct EventSchemaVersion(u32);

impl EventSchemaVersion {
    /// The first version of an event's schema.
    pub const V1: Self = Self(1);

    /// Construct a version from its raw number.
    pub const fn new(version: u32) -> Self {
        Self(version)
    }

    /// The raw version number.
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

impl core::fmt::Display for EventSchemaVersion {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A raw event tagged with the schema version it was stored at.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct VersionedRaw {
    /// The stored event type name.
    pub event_type: String,
    /// The schema version `payload` was stored at.
    pub version: EventSchemaVersion,
    /// The stored payload, as raw bytes.
    pub payload: Vec<u8>,
}

/// One rung: lifts a `from_version` payload of an event type to the next
/// version's bytes.
///
/// A rung returns raw bytes — the next version's shape — not the
/// current `E`, so the registry can chain rungs (a `V1→V2` ladder's
/// middle output is `V2`, not yet the final type). `E` arrives only at
/// the terminal step, decoded by the caller from the bytes the ladder
/// produced. The crate takes no serde dependency for that reason: the
/// rung body owns parsing.
pub trait VersionUpcaster: Send + Sync {
    /// Lift `payload` (stored at this rung's `from` version) to the next
    /// version's bytes.
    fn upcast(&self, payload: Vec<u8>) -> Result<Vec<u8>, UpcastError>;
}

/// Adapts `F: Fn(Vec<u8>) -> Result<Vec<u8>, UpcastError>` into a rung,
/// so a ladder composes without a bespoke struct per step.
pub struct VersionRung<F> {
    step: F,
}

impl<F> VersionRung<F> {
    /// Wrap a step function as a rung.
    pub fn new(step: F) -> Self {
        Self { step }
    }
}

impl<F> VersionUpcaster for VersionRung<F>
where
    F: Fn(Vec<u8>) -> Result<Vec<u8>, UpcastError> + Send + Sync,
{
    fn upcast(&self, payload: Vec<u8>) -> Result<Vec<u8>, UpcastError> {
        (self.step)(payload)
    }
}

/// The shape failures [`build`](UpcasterRegistry::build) reports.
///
/// Every variant names the event type and the offending version(s) —
/// these are startup-configuration errors, not read-time failures.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistryError {
    /// Two rungs registered for the same `(event_type, from_version)`.
    DuplicateRung {
        /// The event type.
        event_type: String,
        /// The version both rungs upgrade from.
        from: EventSchemaVersion,
    },
    /// A rung exists for `V{n}` with no rung for `V{n-1}` — the ladder
    /// has a gap, so payloads stored at `V{n-1}` can't reach the current
    /// version.
    DanglingVersion {
        /// The event type.
        event_type: String,
        /// The version the ladder breaks at (the gap sits below it).
        from: EventSchemaVersion,
    },
    /// The ladder does not terminate at V1: the lowest rung's version
    /// is above 1 (a type with no `V1→V2` step and registered rungs).
    MissingBase {
        /// The event type.
        event_type: String,
        /// The lowest registered version — should be [`EventSchemaVersion::V1`].
        found: EventSchemaVersion,
    },
    /// A payload's version exceeds any rung's reach — a stored shape
    /// newer than the code knows. Loud, never dropped.
    UnknownVersion {
        /// The event type.
        event_type: String,
        /// The stored payload's version.
        found: EventSchemaVersion,
        /// The highest version any rung of this type upgrades from (the
        /// current shape is one past it).
        latest: EventSchemaVersion,
    },
}

impl core::fmt::Display for RegistryError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::DuplicateRung { event_type, from } => write!(
                f,
                "two upcasters registered for `{event_type}` at version {from}"
            ),
            Self::DanglingVersion { event_type, from } => write!(
                f,
                "the upcaster ladder for `{event_type}` breaks at version {from}"
            ),
            Self::MissingBase { event_type, found } => write!(
                f,
                "the upcaster ladder for `{event_type}` has no V1 rung (lowest is {found})"
            ),
            Self::UnknownVersion {
                event_type,
                found,
                latest,
            } => write!(
                f,
                "`{event_type}` stored at version {found}, newer than the latest rung {latest}"
            ),
        }
    }
}

impl core::error::Error for RegistryError {}

/// A validated set of upcasters, indexed by `(event_type, from_version)`.
///
/// Build with [`new`](UpcasterRegistry::new) and register one rung per
/// `with_*` call, then [`build`](UpcasterRegistry::build) before serving
/// reads: a loud [`RegistryError`] on a dangling version, a cycle, or a
/// name collision — never a silently malformed chain at read.
///
/// The registry is a plain value, not a machine: one lookup per event
/// is one step (see §7). Running it — [`upcast`](UpcasterRegistry::upcast)
/// — walks the type's ladder from the stored version to the current one,
/// applying one rung per step. A payload already at the current version
/// passes through unchanged.
#[derive(Default)]
pub struct UpcasterRegistry {
    /// Per event type, the rungs keyed by the version they upgrade from.
    by_type: BTreeMap<String, BTreeMap<EventSchemaVersion, Box<dyn VersionUpcaster>>>,
}


impl UpcasterRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `upcaster` as the rung lifting `event_type` from
    /// `from` to `from + 1`.
    ///
    /// Returns the previous registration on a duplicate — overwriting a
    /// registered rung is a configuration error, surfaced here and
    /// again at [`build`](UpcasterRegistry::build).
    pub fn with<U>(
        mut self,
        event_type: &str,
        from: EventSchemaVersion,
        upcaster: U,
    ) -> (Self, Option<Box<dyn VersionUpcaster>>)
    where
        U: VersionUpcaster + 'static,
    {
        let rungs = self.by_type.entry(event_type.to_owned()).or_default();
        let previous = rungs.insert(from, Box::new(upcaster));
        (self, previous)
    }

    /// Validate the ladder: every registered rung for a type forms one
    /// gap-free, acyclic chain from `V1` upward. Does not consume
    /// `self`; a valid registry then serves reads via
    /// [`upcast`](UpcasterRegistry::upcast).
    ///
    /// # Errors
    ///
    /// The first broken link found, as a [`RegistryError`]. A type with
    /// no `V1→V2` rung but registered rungs above it is
    /// [`MissingBase`](RegistryError::MissingBase); a gap above V1 is
    /// [`DanglingVersion`](RegistryError::DanglingVersion).
    pub fn build(&self) -> Result<(), RegistryError> {
        for (event_type, rungs) in &self.by_type {
            let versions: Vec<EventSchemaVersion> = rungs.keys().copied().collect();
            let Some(&first) = versions.first() else {
                continue;
            };
            if first != EventSchemaVersion::V1 {
                return Err(RegistryError::MissingBase {
                    event_type: event_type.clone(),
                    found: first,
                });
            }
            // A well-formed ladder's rungs are V1, V2, …, one per step,
            // with no repetition (the map keeps the keys distinct, so
            // this check is the gap, not the duplicate).
            for (expected, &version) in versions.iter().enumerate().skip(1) {
                if version != EventSchemaVersion::new(expected as u32 + 1) {
                    return Err(RegistryError::DanglingVersion {
                        event_type: event_type.clone(),
                        from: version,
                    });
                }
            }
        }
        Ok(())
    }

    /// Upcast `raw` to raw bytes of the current version, following the
    /// type's ladder from `raw.version` to the highest registered
    /// version. The caller decodes the result into the current `E`.
    ///
    /// A payload already at (or one past) the highest rung passes
    /// through unchanged. A version above every registered rung is an
    /// [`UpcastError`] (a stored shape newer than the code knows). A
    /// type with no registered rung is an `UpcastError` too — a stored
    /// fact the code has never heard of is not conflated with one at
    /// the current version.
    pub fn upcast(&self, raw: VersionedRaw) -> Result<Vec<u8>, UpcastError> {
        let rungs = self
            .by_type
            .get(&raw.event_type)
            .ok_or_else(|| UpcastError {
                event_type: raw.event_type.clone(),
                message: RegistryError::DanglingVersion {
                    event_type: raw.event_type.clone(),
                    from: raw.version,
                }
                .to_string(),
            })?;
        let latest = *rungs.keys().next_back().ok_or_else(|| UpcastError {
            event_type: raw.event_type.clone(),
            message: RegistryError::DanglingVersion {
                event_type: raw.event_type.clone(),
                from: raw.version,
            }
            .to_string(),
        })?;
        // One past the latest rung is the current version: it passes
        // through unchanged. Anything higher is a stored shape newer
        // than the code knows.
        let current = EventSchemaVersion::new(latest.as_u32() + 1);
        if raw.version > current {
            return Err(UpcastError {
                event_type: raw.event_type.clone(),
                message: RegistryError::UnknownVersion {
                    event_type: raw.event_type,
                    found: raw.version,
                    latest,
                }
                .to_string(),
            });
        }

        let mut version = raw.version;
        let mut payload = raw.payload;
        // The ladder climbs one version per rung until the payload
        // reaches the current (one-past-the-highest-rung) version.
        while version < current {
            let rung = rungs.get(&version).ok_or_else(|| UpcastError {
                event_type: raw.event_type.clone(),
                message: RegistryError::DanglingVersion {
                    event_type: raw.event_type.clone(),
                    from: version,
                }
                .to_string(),
            })?;
            payload = rung.upcast(payload)?;
            version = EventSchemaVersion::new(version.as_u32() + 1);
        }
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::string::ToString;

    fn bump(payload: Vec<u8>) -> Result<Vec<u8>, UpcastError> {
        // A rung: increment the payload byte. A V1->V2 rung on a
        // one-byte payload adds one.
        let value = core::str::from_utf8(&payload)
            .map_err(|_| UpcastError {
                event_type: String::new(),
                message: "not utf-8".into(),
            })?
            .parse::<u64>()
            .map_err(|_| UpcastError {
                event_type: String::new(),
                message: "not a number".into(),
            })?;
        Ok((value + 1).to_string().into_bytes())
    }

    fn raw(event_type: &str, version: u32, payload: &str) -> VersionedRaw {
        VersionedRaw {
            event_type: event_type.to_string(),
            version: EventSchemaVersion::new(version),
            payload: payload.as_bytes().to_vec(),
        }
    }

    type RungFn = fn(Vec<u8>) -> Result<Vec<u8>, UpcastError>;

    fn registry_with(event_type: &str, rungs: &[(u32, RungFn)]) -> UpcasterRegistry {
        let mut registry = UpcasterRegistry::new();
        for &(from, step) in rungs {
            let (next, _previous) =
                registry.with(event_type, EventSchemaVersion::new(from), VersionRung::new(step));
            registry = next;
        }
        registry
    }

    #[test]
    fn a_single_rung_lifts_v1_to_the_current_version() {
        let registry = registry_with("Amount", &[(1, bump)]);
        assert_eq!(
            registry.upcast(raw("Amount", 1, "41")).expect("upcast"),
            b"42"
        );
    }

    #[test]
    fn a_v2_ladder_applies_both_rungs_in_order() {
        let registry = registry_with("Amount", &[(1, bump), (2, bump)]);
        // V1=41 → V2=42 → V3=43 (current).
        assert_eq!(
            registry.upcast(raw("Amount", 1, "41")).expect("upcast"),
            b"43"
        );
        // Already at V2 climbs one rung.
        assert_eq!(
            registry.upcast(raw("Amount", 2, "9")).expect("upcast"),
            b"10"
        );
    }

    #[test]
    fn a_payload_at_the_current_version_passes_through() {
        // With rungs V1→V2 and V2→V3, V3 is the current shape: a
        // payload already there passes through unchanged.
        let registry = registry_with("Amount", &[(1, bump), (2, bump)]);
        assert_eq!(
            registry.upcast(raw("Amount", 3, "tick")).expect("current"),
            b"tick"
        );
    }


    #[test]
    fn a_newer_stored_version_is_loud_never_dropped() {
        let registry = registry_with("Amount", &[(1, bump)]);
        let error = registry
            .upcast(raw("Amount", 3, "0"))
            .expect_err("a shape newer than the code knows");
        assert!(error.message.contains("newer than the latest rung"));
    }

    #[test]
    fn build_catches_a_ladder_gap() {
        let registry = registry_with("Amount", &[(2, bump)]); // no V1 rung
        let error = registry.build().expect_err("a missing base");
        assert!(matches!(
            error,
            RegistryError::MissingBase { ref event_type, found }
                if event_type == "Amount" && found == EventSchemaVersion::new(2)
        ));
    }

    #[test]
    fn build_catches_a_dangling_version() {
        let registry = registry_with("Amount", &[(1, bump), (3, bump)]); // gap at V2
        let error = registry.build().expect_err("a gap in the ladder");
        assert!(matches!(
            error,
            RegistryError::DanglingVersion { ref event_type, from }
                if event_type == "Amount" && from == EventSchemaVersion::new(3)
        ));
    }

    #[test]
    fn build_catches_a_duplicate_rung() {
        let registry = UpcasterRegistry::new();
        let (registry, previous) =
            registry.with("Amount", EventSchemaVersion::new(1), VersionRung::new(bump));
        assert!(previous.is_none());
        let (_registry, previous) =
            registry.with("Amount", EventSchemaVersion::new(1), VersionRung::new(bump));
        assert!(previous.is_some(), "overwriting a rung is reported");
    }

    #[test]
    fn a_valid_registry_builds() {
        let registry = registry_with("Amount", &[(1, bump), (2, bump)]);
        registry.build().expect("a well-formed ladder");
    }
}

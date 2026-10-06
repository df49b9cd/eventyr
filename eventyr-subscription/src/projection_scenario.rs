//! `ProjectionScenario`: the given/when/then test DSL for projections.
//!
//! The subscription-side counterpart of
//! [`eventyr_core::testing::Scenario`]
//! (§10): the write side asserts on `decide`/`apply`, the read side on
//! what a projection did with a stream of envelopes. A scenario folds
//! a history and lets a check assert on the projection afterwards — no
//! store, no subscription source, no `tokio`.

use core::future::Future;

use eventyr_core::envelope::EventEnvelope;

use crate::runner::Projection;

/// A test scenario for one projection: `given` the envelopes to fold,
/// `when` (or `when_driven` for a custom drive), then `then` the
/// assertion on the projection itself — the read-side counterpart of
/// core's `Scenario::when`.
pub struct ProjectionScenario<P: Projection> {
    name: String,
    projection: P,
    history: Vec<EventEnvelope<P::Event>>,
}

impl<P: Projection> ProjectionScenario<P> {
    /// An empty scenario over `projection`, named for its assertions.
    pub fn over(name: &str, projection: P) -> Self {
        Self {
            name: name.to_owned(),
            projection,
            history: Vec::new(),
        }
    }

    /// Seed the history the run folds through the projection, in order.
    /// Versions and sequence numbers come from the fixture's envelopes —
    /// a scenario is a projection test, not a store test.
    pub fn given(mut self, history: impl IntoIterator<Item = EventEnvelope<P::Event>>) -> Self {
        self.history = history.into_iter().collect();
        self
    }

    /// Fold the seeded history through the projection: every `apply` in
    /// order, panicking with the scenario's name on a rejection.
    pub async fn when(self) -> ProjectionOutcome<P>
    where
        P::Event: Send,
        P::Error: core::fmt::Debug,
    {
        let name = self.name;
        let mut projection = self.projection;
        for envelope in &self.history {
            projection
                .apply(envelope)
                .await
                .unwrap_or_else(|error| panic!("[{name}] applying: {error:?}"));
        }
        ProjectionOutcome { name, projection }
    }

    /// Drive the projection with a custom call — a catch-up replay, a
    /// direct `apply`, anything — then continue to `then`. The drive
    /// takes the projection and returns it, so arbitrary awaits may sit
    /// between its folds.
    pub async fn when_driven<F, Fut, E>(self, drive: F) -> ProjectionOutcome<P>
    where
        F: FnOnce(P) -> Fut,
        Fut: Future<Output = Result<P, E>>,
        E: core::fmt::Debug,
    {
        let projection = drive(self.projection)
            .await
            .unwrap_or_else(|error| panic!("[{}] driving: {error:?}", self.name));
        ProjectionOutcome {
            name: self.name,
            projection,
        }
    }
}

/// The second phase of a [`ProjectionScenario`]: the projection after
/// the drive. `then` chains so several assertions read as one scenario.
pub struct ProjectionOutcome<P: Projection> {
    name: String,
    projection: P,
}

impl<P: Projection> ProjectionOutcome<P> {
    /// Check the projection's state. The scenario's name is passed so
    /// an assertion failure names the scenario it broke. Returning
    /// `self` lets a test chain several assertions under one scenario.
    pub fn then(self, check: impl FnOnce(&str, &P)) -> Self {
        check(&self.name, &self.projection);
        self
    }

    /// The projection itself, for checks that need more than a closure.
    pub fn projection(&self) -> &P {
        &self.projection
    }
}

impl<P: Projection> core::fmt::Debug for ProjectionOutcome<P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("ProjectionOutcome")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

//! Shared test helpers.
//!
//! `tests/*/mod.rs`-adjacent: any file under `tests/<name>/` is not a
//! target of its own, so one copy serves every integration suite.

use std::sync::{Arc, Mutex};

/// The order stages ran in, shared by the handlers of one mediator.
pub(crate) struct Trace(Arc<Mutex<Vec<String>>>);

impl Trace {
    pub(crate) fn push(&self, step: impl Into<String>) {
        self.0.lock().unwrap().push(step.into());
    }

    pub(crate) fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

impl Clone for Trace {
    fn clone(&self) -> Self {
        Trace(self.0.clone())
    }
}

impl Default for Trace {
    fn default() -> Self {
        Trace(Arc::new(Mutex::new(Vec::new())))
    }
}

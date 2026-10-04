//! `decide` must be a function path, not an arbitrary expression.

use eventyr_macros::Aggregate;

#[derive(Aggregate)]
#[eventyr(decide = 1 + 1)]
struct Counter;

fn main() {}

//! A repeated `#[eventyr(...)]` key is rejected.

use eventyr_macros::Aggregate;

#[derive(Aggregate)]
#[eventyr(name = "a", name = "b")]
struct Counter;

fn main() {}

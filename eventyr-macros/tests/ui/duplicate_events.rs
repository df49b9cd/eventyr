//! A repeated `events(...)` list is rejected, like every other key.

use eventyr_macros::Aggregate;

#[derive(Aggregate)]
#[eventyr(events(Opened), events(Closed))]
struct Counter;

fn main() {}

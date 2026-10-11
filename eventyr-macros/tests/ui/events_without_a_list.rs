//! `events(...)` needs its parenthesized list, like events(Opened).

use eventyr_macros::Aggregate;

#[derive(Aggregate)]
#[eventyr(id = u64, events)]
struct Counter;

fn main() {}

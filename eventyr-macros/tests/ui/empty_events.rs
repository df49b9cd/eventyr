//! `events(...)` lists the payload types; an empty list is a typo.

use eventyr_macros::Aggregate;

#[derive(Aggregate)]
#[eventyr(events())]
struct Counter;

fn main() {}

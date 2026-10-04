//! `Aggregate` must be derived on a unit struct, not an enum.

use eventyr_macros::Aggregate;

#[derive(Aggregate)]
enum NotAnAggregate {
    A,
}

fn main() {}

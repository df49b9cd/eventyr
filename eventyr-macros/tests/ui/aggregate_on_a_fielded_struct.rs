//! `Aggregate` must be derived on a unit struct: the aggregate type is
//! a namespace, not a state holder.

use eventyr_macros::Aggregate;

#[derive(Aggregate)]
struct WithFields {
    state: u64,
}

fn main() {}

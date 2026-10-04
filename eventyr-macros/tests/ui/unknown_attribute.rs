//! An unknown `#[eventyr(...)]` key is a clear error, not a silent
//! ignore.

use eventyr_macros::Aggregate;

#[derive(Aggregate)]
#[eventyr(nme = "typo")]
struct Counter;

fn main() {}

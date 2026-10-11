//! `EventName` takes `name` and `crate` — nothing else.

use eventyr_macros::EventName;

#[derive(EventName)]
#[eventyr(nme = "x")]
enum Transfer {
    Sent,
}

fn main() {}

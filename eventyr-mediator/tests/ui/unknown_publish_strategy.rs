use eventyr_mediator::prelude::*;

struct Ping;
impl Notification for Ping {
    type Error = core::convert::Infallible;
}

eventyr_mediator::mediator! {
    struct App;
    event Ping;

    publish whenever;
}

fn main() {}

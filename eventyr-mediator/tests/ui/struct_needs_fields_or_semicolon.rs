use eventyr_mediator::prelude::*;

struct Ping;
impl Request for Ping {
    type Response = ();
}

eventyr_mediator::mediator! {
    struct App
    command Ping => Handler;
}

fn main() {}

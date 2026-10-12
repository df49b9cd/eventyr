use eventyr_mediator::prelude::*;

struct Ping;
impl Request for Ping {
    type Response = ();
}

#[derive(Default)]
struct Bystander;

eventyr_mediator::mediator! {
    struct App;
    command Ping => Bystander;
}

fn main() {}

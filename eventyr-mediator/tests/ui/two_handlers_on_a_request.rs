use eventyr_mediator::prelude::*;

struct Ping;
impl Request for Ping {
    type Response = ();
}

#[derive(Default)]
struct A;
#[derive(Default)]
struct B;

eventyr_mediator::mediator! {
    struct App;
    command Ping => A, B;
}

fn main() {}

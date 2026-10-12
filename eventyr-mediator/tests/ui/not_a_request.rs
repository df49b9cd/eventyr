struct Ping;

#[derive(Default)]
struct Handler;

eventyr_mediator::mediator! {
    struct App;
    command Ping => Handler;
}

fn main() {}

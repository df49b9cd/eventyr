use eventyr_mediator::prelude::*;

struct Routed;
impl Request for Routed {
    type Response = ();
}

struct Unrouted;
impl Request for Unrouted {
    type Response = ();
}

#[derive(Default)]
struct Handler;
impl<M: Sync> RequestHandler<Routed, M> for Handler {
    async fn handle(&self, _: &M, _: Routed) {}
}

eventyr_mediator::mediator! {
    struct App;
    command Routed => Handler;
}

fn main() {
    let app = App::new();
    let _ = app.send(Unrouted);
}

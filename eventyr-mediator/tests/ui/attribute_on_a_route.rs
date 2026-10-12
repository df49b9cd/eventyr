use eventyr_mediator::prelude::*;

struct Ping;
impl Request for Ping {
    type Response = ();
}

#[derive(Default)]
struct Handler;
impl<M: Sync> RequestHandler<Ping, M> for Handler {
    async fn handle(&self, _: &M, _: Ping) {}
}

eventyr_mediator::mediator! {
    struct App;
    // Attributes are accepted only on the `requests`, `streams`, and
    // `notifications` clauses.
    #[derive(Debug)]
    command Ping => Handler;
}

fn main() {}

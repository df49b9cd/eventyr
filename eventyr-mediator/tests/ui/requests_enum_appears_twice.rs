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
    command Ping => Handler;

    requests AnyRequest -> AnyResponse;
    requests OtherRequest -> OtherResponse;
}

fn main() {}

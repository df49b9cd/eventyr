use eventyr_mediator::prelude::*;

mod a {
    pub struct Ping;
}
mod b {
    pub struct Ping;
}
impl Request for a::Ping {
    type Response = ();
}
impl Request for b::Ping {
    type Response = ();
}

#[derive(Default)]
struct Handler;
impl<M: Sync> RequestHandler<a::Ping, M> for Handler {
    async fn handle(&self, _: &M, _: a::Ping) {}
}
impl<M: Sync> RequestHandler<b::Ping, M> for Handler {
    async fn handle(&self, _: &M, _: b::Ping) {}
}

eventyr_mediator::mediator! {
    struct App;
    command a::Ping => Handler;
    command b::Ping => Handler;
    requests AnyRequest -> AnyResponse;
}

fn main() {}

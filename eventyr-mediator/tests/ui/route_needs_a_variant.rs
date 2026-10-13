use eventyr_mediator::prelude::*;

struct Ping;
impl Request for Ping {
    type Response = ();
}

struct Other;
impl Request for Other {
    type Response = ();
}
impl Request for &'static Other {
    type Response = ();
}

#[derive(Default)]
struct Handler;
impl<M: Sync> RequestHandler<Ping, M> for Handler {
    async fn handle(&self, _: &M, _: Ping) {}
}
impl<M: Sync> RequestHandler<&'static Other, M> for Handler {
    async fn handle(&self, _: &M, _: &'static Other) {}
}

eventyr_mediator::mediator! {
    struct App;
    command Ping => Handler;
    // A reference is no path, so there is no last segment to take as the
    // variant: the route must name one with `as`.
    command &'static Other => Handler;

    requests AnyRequest -> AnyResponse;
}

fn main() {}

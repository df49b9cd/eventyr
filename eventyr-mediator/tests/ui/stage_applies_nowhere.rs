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

trait Audited {}

#[derive(Default)]
struct Audit;
impl<Req: Request + Audited, M: Sync> PipelineBehavior<Req, M> for Audit {
    async fn handle<N: Next<Req>>(&self, _: &M, request: Req, next: N) -> Req::Response {
        next.run(request).await
    }
}

eventyr_mediator::mediator! {
    struct App;
    command Ping => Handler;
    behaviors { Audit }
}

fn main() {}

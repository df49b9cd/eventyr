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

#[derive(Default)]
struct Audit;
impl<Req: Request + Sync, M: Sync> RequestPostProcessor<Req, M> for Audit
where
    Req::Response: Sync,
{
    async fn process(&self, _: &M, _: &Req, _: &Req::Response) {}
}

eventyr_mediator::mediator! {
    struct App;
    command Ping => Handler;
    post_processors { Audit }
}

fn main() {}

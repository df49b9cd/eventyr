use eventyr_mediator::prelude::*;

struct Ping;
impl Request for Ping {
    type Response = Result<u32, String>;
}

#[derive(Default)]
struct Handler;
impl<M: Sync> RequestHandler<Ping, M> for Handler {
    async fn handle(&self, _: &M, _: Ping) -> Result<u32, String> {
        Ok(1)
    }
}

#[derive(Clone, Default)]
struct Fallback;
impl<M: Sync> RequestErrorHandler<Ping, M> for Fallback {
    async fn handle(&self, _: &M, _: &Ping, _: String) -> Result<u32, String> {
        Ok(0)
    }
}
#[derive(Clone, Default)]
struct Fallback2;
impl<M: Sync> RequestErrorHandler<Ping, M> for Fallback2 {
    async fn handle(&self, _: &M, _: &Ping, _: String) -> Result<u32, String> {
        Ok(0)
    }
}

eventyr_mediator::mediator! {
    struct App;
    command Ping => Handler;

    error_handlers { Fallback }
    error_handlers { Fallback2 }
}

fn main() {}

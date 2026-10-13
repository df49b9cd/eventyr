use eventyr_mediator::prelude::*;

struct Get<T>(core::marker::PhantomData<T>);
impl<T: Send> Request for Get<T> {
    type Response = ();
}

#[derive(Default)]
struct Handler;
impl<T: Send, M: Sync> RequestHandler<Get<T>, M> for Handler {
    async fn handle(&self, _: &M, _: Get<T>) {}
}

eventyr_mediator::mediator! {
    struct App;
    query<T: Send> Get<T> as GetAny => Handler;
}

fn main() {}

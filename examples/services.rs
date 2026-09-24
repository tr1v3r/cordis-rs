//! Dependency-driven reload: a consumer tracks a service binding through
//! provider replacement (V20). Retiring the binding parks the consumer
//! in `Pending`; a fresh provider fiber re-activates it in a new
//! generation. Run with `cargo run --example services -p cordis-core`.

use cordis_core::{App, Context, FiberHandle, FiberState, ServiceKey, ShutdownOptions, define};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

struct Db {
    url: String,
}
struct Unit;

async fn await_state(handle: &FiberHandle<Unit>, want: FiberState) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while handle.status().await.expect("alive").state != want {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("reached the target state");
}

#[tokio::main]
async fn main() {
    let app = App::builder().name("services-demo").build().expect("app");
    let root = app.context();
    let key = ServiceKey::<Db>::new("db");
    let applies = Arc::new(AtomicUsize::new(0));

    // The provider publishes from its generation context.
    let provider = define("db", move |ctx: Context, _: Arc<Unit>| {
        let key = key.clone();
        async move {
            ctx.provide(
                key,
                Arc::new(Db {
                    url: "postgres://v1".into(),
                }),
            )
            .await?;
            Ok(())
        }
    });

    // The consumer declares the dependency at the definition level and
    // leases the binding of its generation on every activation.
    let consumer = {
        let applies = Arc::clone(&applies);
        define("api", move |ctx: Context, _: Arc<Unit>| {
            let applies = Arc::clone(&applies);
            let key = ServiceKey::<Db>::new("db");
            async move {
                applies.fetch_add(1, Ordering::SeqCst);
                let lease = ctx.get(key).await?;
                println!("consumer sees {}", lease.snapshot().expect("live").url);
                Ok(())
            }
        })
        .require(ServiceKey::<Db>::new("db"))
    };

    let first = root.load(&provider, Unit).await.expect("provider admitted");
    let consumer = root.load(&consumer, Unit).await.expect("consumer admitted");
    let deadline = Instant::now() + Duration::from_secs(15);
    let generation_1 = consumer.fiber.wait_active(deadline).await.expect("active");
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    // Retire the binding: the consumer converges to Pending (a waiting
    // state, never an error) without re-running its apply body.
    let dispose = first.fiber.dispose().await.expect("dispose admitted");
    dispose.wait().await.expect("provider disposed");
    await_state(&consumer.fiber, FiberState::Pending).await;
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    println!("binding retired -> consumer Pending (still 1 activation)");

    // Re-provide as a new fiber of the same definition: a fresh
    // BindingId invalidates the consumer's dependency epoch and reloads.
    let second = root.load(&provider, Unit).await.expect("re-provide");
    let generation_2 = consumer
        .fiber
        .wait_active(deadline)
        .await
        .expect("re-active");
    assert_ne!(generation_1, generation_2);
    assert_eq!(applies.load(Ordering::SeqCst), 2);
    println!("provider replaced -> consumer re-activated");
    drop(second);

    let report = app
        .shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
    println!("shutdown: {} fibers disposed", report.fibers_disposed);
}

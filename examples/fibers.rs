//! Minimal plugin lifecycle: define, load, update, dispose, shutdown.
//! Run with `cargo run --example fibers -p cordis-core`.

use cordis_core::{App, FiberState, OperationOutcome, ShutdownOptions, define};
use std::sync::Arc;
use std::time::Duration;

struct GreetingConfig {
    name: String,
}

#[tokio::main]
async fn main() {
    let app = App::builder().name("fibers-demo").build().expect("app");
    let root = app.context();

    // A plugin definition: `apply` runs once per activation on a
    // supervised worker; everything registered through the generation
    // context is owned by that generation.
    let plugin = define("greeting", |ctx, cfg: Arc<GreetingConfig>| async move {
        println!("activated for {}", cfg.name);
        let name = cfg.name.clone();
        ctx.on_dispose("log", move || async move {
            println!("goodbye, {name}");
            Ok(())
        })
        .await?;
        Ok(())
    });

    // Two loads of one definition share the runtime, not the fiber:
    // each fiber carries its own configuration (V01).
    let first = root
        .load(
            &plugin,
            GreetingConfig {
                name: "alpha".into(),
            },
        )
        .await
        .expect("first load admitted");
    let second = root
        .load(
            &plugin.clone(),
            GreetingConfig {
                name: "beta".into(),
            },
        )
        .await
        .expect("second load admitted");
    assert_eq!(first.fiber.runtime_id(), second.fiber.runtime_id());
    assert_ne!(first.fiber.fiber_id(), second.fiber.fiber_id());
    println!("one runtime, two fibers");

    // `load` resolves on admission; the operation receipt resolves when
    // this activation request settles.
    let outcome = first.operation.wait().await.expect("activation settles");
    assert!(matches!(&*outcome, OperationOutcome::Active { .. }));
    second.operation.wait().await.expect("second activates");
    println!("both fibers active");

    // An update submits a new desired revision on one fiber only.
    let update = first
        .fiber
        .update(GreetingConfig {
            name: "alpha-2".into(),
        })
        .await
        .expect("update admitted");
    match &*update.wait().await.expect("update settles") {
        OperationOutcome::Active { generation } => {
            println!("re-activated in generation {generation:?}");
        }
        other => panic!("unexpected update outcome: {other:?}"),
    }
    assert_eq!(
        second.fiber.status().await.expect("alive").state,
        FiberState::Active
    );

    // Disposal is terminal and awaited; the registered cleanup runs.
    let dispose = first.fiber.dispose().await.expect("dispose admitted");
    match &*dispose.wait().await.expect("dispose settles") {
        OperationOutcome::Disposed { cleanup } if cleanup.is_clean() => {}
        other => panic!("unexpected dispose outcome: {other:?}"),
    }
    println!("first fiber disposed; second keeps running");

    // Shutdown is explicit, awaited and observable.
    let options = ShutdownOptions {
        timeout: Some(Duration::from_secs(5)),
    };
    let report = app.shutdown(options).await.expect("shutdown completes");
    println!(
        "shutdown: {} disposed, {} runtimes dropped, {} quarantined",
        report.fibers_disposed, report.runtimes_dropped, report.quarantined
    );
}

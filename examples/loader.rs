//! Configuration-driven assembly: register plugins, compose JSON layers,
//! mount them onto an app, then reconcile a patched desired tree.
//! Run with `cargo run --example loader -p cordis-loader`.

use cordis_core::{App, define};
use cordis_loader::{
    ComposeOptions, DumpOptions, Layer, Registry, compose, dump, mount, plan, plan_report,
    reconcile, unmount,
};
use serde::Deserialize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const BASE: &str = r#"[{"id":"first","name":"greeter","config":{"name":"alpha"}}]"#;
const PATCH: &str = r#"[{"id":"first","config":{"name":"beta"}}]"#;

#[derive(Deserialize)]
struct GreeterConfig {
    name: String,
}

fn main() {
    // The registry maps names to typed constructors; `register_serde`
    // decodes JSON configs through serde (serde stays out of core).
    let mut registry = Registry::new();
    let activations = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&activations);
    registry
        .register_serde(
            "greeter",
            define("greeter", move |_, cfg: Arc<GreeterConfig>| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    println!("activated for {}", cfg.name);
                    Ok(())
                }
            }),
        )
        .expect("register greeter");

    // Pure data path: parse, compose, dump — no runtime, no loads.
    let base = Layer::parse("base", BASE).expect("base layer parses");
    let tree = compose(&[base], ComposeOptions::default()).expect("compose");
    print!("{}", dump(&tree, DumpOptions::redacted()));

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime builds");
    rt.block_on(async {
        let app = App::builder()
            .name("loader-demo")
            .build()
            .expect("app builds");
        let root = app.context();

        let mut mounted = mount(&tree, &registry, &root).await.expect("mount");
        let identity = mounted
            .find("first")
            .expect("entry")
            .fiber_id()
            .expect("fiber");
        println!(
            "mounted {:?} at revision {}",
            mounted.ids(),
            mounted.revision()
        );

        // Desired tree: a patch layer replaces first's config. The plan
        // is pure (dry-run renders it, touching nothing).
        let base = Layer::parse("base", BASE).expect("base layer parses");
        let patch = Layer::parse_patch("patch", PATCH).expect("patch layer parses");
        let desired = compose(&[base, patch], ComposeOptions::default()).expect("compose");
        let planned = plan(&tree, &desired, mounted.revision()).expect("plan");
        print!("{}", plan_report(&planned).render());

        let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
            .await
            .expect("apply");
        assert!(report.is_clean(), "{report:?}");
        assert_eq!(
            identity,
            mounted
                .find("first")
                .expect("entry")
                .fiber_id()
                .expect("fiber")
        );
        println!(
            "reconciled to revision {} in {} activations",
            report.revision,
            activations.load(Ordering::SeqCst)
        );

        let unmounted = unmount(&mounted).await;
        println!("unmounted {:?}", unmounted.disposed);
    });
}

//! P7 end-to-end: reconcile plans against real mounted fibers (V49–V53).
//!
//! - unchanged nodes keep their fiber ids (no restart);
//! - a config-only change updates in place;
//! - definition/inject/disabled changes recreate with a new fiber;
//! - a bad desired tree never changes the running tree;
//! - apply binds tree revisions: a stale plan is refused;
//! - runtime failures report per-node real states, no rollback claims;
//! - dry-run renders redacted plans without touching anything.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cordis_core::{App, define};
use cordis_loader::{
    Action, ComposeOptions, Layer, Registry, Tree, compose, mount, plan, plan_report, reconcile,
};

struct Cfg {
    #[allow(dead_code)]
    value: u64,
}

struct BigConfig {
    count: i64,
    umax: u64,
}

fn registry_with_counter() -> (Registry, Arc<AtomicUsize>) {
    let mut registry = Registry::new();
    let activations = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&activations);
    registry
        .register(
            "p",
            define("p", move |_ctx, _cfg: Arc<Cfg>| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }),
            |config| {
                Ok(Cfg {
                    value: config.get("value").and_then(|v| v.as_u64()).unwrap_or(0),
                })
            },
        )
        .expect("register p");
    let boom_counter = Arc::new(AtomicUsize::new(0));
    let boom = Arc::clone(&boom_counter);
    registry
        .register(
            "boom",
            define("boom", move |_ctx, _cfg: Arc<Cfg>| {
                let boom = Arc::clone(&boom);
                async move {
                    // Succeeds on the first activation, refuses afterwards:
                    // the recreate during a later apply fails at runtime.
                    if boom.fetch_add(1, Ordering::SeqCst) > 0 {
                        return Err(cordis_core::PluginError::from("recreate refused"));
                    }
                    Ok(())
                }
            }),
            |config| {
                Ok(Cfg {
                    value: config.get("value").and_then(|v| v.as_u64()).unwrap_or(0),
                })
            },
        )
        .expect("register boom");
    (registry, activations)
}

fn tree(text: &str) -> Tree {
    let layer = Layer::parse("t", text).expect("layer");
    compose(&[layer], ComposeOptions::default()).expect("compose")
}

#[tokio::test]
async fn v49_unchanged_nodes_keep_fiber_identity_config_changes_update() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (registry, activations) = registry_with_counter();

    let current = tree(
        r#"[{"id":"a","name":"p","config":{"value":1}},{"id":"b","name":"p","config":{"value":1}}]"#,
    );
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");
    let a_id = mounted.find("a").expect("a").fiber_id().expect("fiber");
    let b_id = mounted.find("b").expect("b").fiber_id().expect("fiber");
    assert_eq!(activations.load(Ordering::SeqCst), 2);

    // Only b's config changes: a keeps its fiber, b updates in place —
    // neither restarts (V49).
    let desired = tree(
        r#"[{"id":"a","name":"p","config":{"value":1}},{"id":"b","name":"p","config":{"value":2}}]"#,
    );
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "b" && e.action == Action::Update)
    );
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "a" && e.action == Action::Keep)
    );

    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply");
    assert!(report.is_clean(), "{report:?}");
    assert_eq!(report.revision, 2);
    // Update restarts the changed fiber's *generation* (one more apply
    // run) but never its fiber identity: b re-activates once, a never.
    assert_eq!(activations.load(Ordering::SeqCst), 3, "only b re-activates");
    assert_eq!(
        mounted.find("a").expect("a").fiber_id().expect("fiber"),
        a_id
    );
    assert_eq!(
        mounted.find("b").expect("b").fiber_id().expect("fiber"),
        b_id
    );

    app.shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v50_identity_changes_recreate_with_a_new_fiber() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (registry, activations) = registry_with_counter();

    let current = tree(r#"[{"id":"a","name":"p","config":{"value":1}}]"#);
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");
    let old_id = mounted.find("a").expect("a").fiber_id().expect("fiber");

    // Same id, different definition: recreate, never edit the live fiber
    // (V50). "p" activates once here — one restart in total.
    let desired = tree(r#"[{"id":"a","name":"p","config":{"value":2},"inject":["x"]}]"#);
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    assert!(planned
        .entries
        .iter()
        .any(|e| e.action == Action::Recreate && e.reasons.iter().any(|r| r.contains("inject"))));

    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply");
    assert!(report.is_clean(), "{report:?}");
    assert_ne!(
        mounted.find("a").expect("a").fiber_id().expect("fiber"),
        old_id,
        "recreate loads a new fiber"
    );
    assert_eq!(activations.load(Ordering::SeqCst), 2);

    app.shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v51_stale_plans_are_refused_by_tree_revision() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (registry, _) = registry_with_counter();

    let current = tree(r#"[{"id":"a","name":"p"}]"#);
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");

    // Plan against revision 1, apply, then replay the same plan: the
    // tree is now at revision 2 — the old plan is superseded (V51).
    let desired = tree(r#"[{"id":"a","name":"p","config":{"value":5}}]"#);
    let stale = plan(&current, &desired, 1).expect("plan");
    let report = reconcile(&mut mounted, stale.clone(), &desired, &registry, &root)
        .await
        .expect("first apply");
    assert!(report.is_clean());
    assert_eq!(mounted.revision(), 2);

    let refused = reconcile(&mut mounted, stale, &desired, &registry, &root)
        .await
        .expect_err("stale plan refused");
    assert!(
        refused.to_string().contains("revision 1") && refused.to_string().contains("revision 2"),
        "{refused}"
    );

    app.shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v46_bad_desired_tree_changes_nothing() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (registry, activations) = registry_with_counter();

    let current = tree(r#"[{"id":"a","name":"p","config":{"value":1}}]"#);
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");
    let a_id = mounted.find("a").expect("a").fiber_id().expect("fiber");

    // A desired tree with an unknown plugin: predecode refuses before the
    // running tree changes (V46 on the reconcile path).
    let bad = tree(r#"[{"id":"a","name":"p","config":{"value":1}},{"id":"x","name":"ghost"}]"#);
    let planned = plan(&current, &bad, mounted.revision()).expect("plan");
    let err = reconcile(&mut mounted, planned, &bad, &registry, &root)
        .await
        .expect_err("unknown plugin refuses");
    assert!(err.to_string().contains("ghost"), "{err}");

    assert_eq!(
        mounted.find("a").expect("a").fiber_id().expect("fiber"),
        a_id
    );
    assert_eq!(activations.load(Ordering::SeqCst), 1);
    assert_eq!(mounted.revision(), 1);
    assert_eq!(mounted.ids(), vec!["a"]);

    app.shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v52_runtime_failure_reports_real_state_no_rollback_claims() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (registry, _) = registry_with_counter();

    // "boom" succeeds on first activation; the recreate during apply
    // fails at runtime with "recreate refused".
    let current = tree(r#"[{"id":"a","name":"boom","config":{"value":1}}]"#);
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");

    // Definition change boom -> p forces a recreate that runs the new
    // plugin cleanly; instead keep the failing definition and change its
    // inject — the recreate reloads "boom", whose second activation
    // fails.
    let desired = tree(r#"[{"id":"a","name":"boom","config":{"value":1},"inject":["late"]}]"#);
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply completes with a per-node failure");
    assert!(!report.is_clean());
    let failure = report
        .outcomes
        .iter()
        .find(|outcome| outcome.result.is_err())
        .expect("the recreate failure is reported");
    assert_eq!(failure.id, "a");
    assert!(
        failure
            .result
            .as_ref()
            .is_err_and(|reason| reason.contains("activation")),
        "the report carries the real state: {:?}",
        failure.result
    );
    // No rollback claim: the revision moved, and the failed node has no
    // phantom fiber — the old fiber was disposed, the new one failed.
    assert_eq!(mounted.revision(), 2);
    assert!(
        mounted.find("a").is_none(),
        "the failed recreate left no phantom fiber"
    );

    app.shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v53_dry_run_renders_redacted_and_touches_nothing() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (registry, activations) = registry_with_counter();

    let current = tree(r#"[{"id":"a","name":"p","config":{"value":1}}]"#);
    let mounted = mount(&current, &registry, &root).await.expect("mount");
    let a_id = mounted.find("a").expect("a").fiber_id().expect("fiber");

    let desired = tree(
        r#"[{"id":"a","name":"p","config":{"value":2,"password":"hunter2"}},{"id":"new","name":"p"}]"#,
    );
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    let report = plan_report(&planned);
    let text = report.render();
    assert!(text.contains("update \"a\": config changed"), "{text}");
    assert!(text.contains("insert \"new\""), "{text}");
    assert!(!text.contains("hunter2"), "redaction failed: {text}");
    assert!(text.contains("\"password\":\"***\""), "{text}");

    // No runtime side effects: same fiber, same count, same revision.
    assert_eq!(
        mounted.find("a").expect("a").fiber_id().expect("fiber"),
        a_id
    );
    assert_eq!(activations.load(Ordering::SeqCst), 1);
    assert_eq!(mounted.revision(), 1);
    assert_eq!(mounted.ids(), vec!["a"]);

    app.shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v44_big_numbers_survive_into_typed_configs() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let mut registry = Registry::new();
    let seen = Arc::new(std::sync::Mutex::new(Vec::<(i64, u64)>::new()));
    let recorder = Arc::clone(&seen);
    registry
        .register(
            "big",
            define("big", move |_ctx, cfg: Arc<BigConfig>| {
                let recorder = Arc::clone(&recorder);
                async move {
                    recorder.lock().unwrap().push((cfg.count, cfg.umax));
                    Ok(())
                }
            }),
            |config| {
                // The decoder reads u64/i64 straight from serde_json's
                // exact integer representation: boundaries intact.
                Ok(BigConfig {
                    count: config.get("count").and_then(|v| v.as_i64()).unwrap_or(0),
                    umax: config.get("umax").and_then(|v| v.as_u64()).unwrap_or(0),
                })
            },
        )
        .expect("register big");

    let current = tree(
        r#"[{"id":"b","name":"big","config":{"count":-9007199254740993,"umax":18446744073709551615}}]"#,
    );
    let mounted = mount(&current, &registry, &root)
        .await
        .expect("mount with exact integers");
    assert_eq!(mounted.ids(), vec!["b"]);
    assert_eq!(
        *seen.lock().unwrap(),
        vec![(-9007199254740993_i64, u64::MAX)],
        "2^53+1 and u64::MAX arrive exactly"
    );

    app.shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v52_quarantine_during_reconcile_disposal_is_reported_honestly() {
    // A removed node whose cleanup refuses must surface as a quarantine
    // in the apply report — never as a clean removal (V52). This pins
    // the dispose_entry fix: the real per-node fiber state flows back.
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (mut registry, _) = registry_with_counter();
    registry
        .register(
            "dirty",
            define("dirty", |ctx, _cfg: Arc<Cfg>| async move {
                ctx.on_dispose("dirty-cleanup", || async {
                    Err(cordis_core::CleanupError::from("cleanup refused"))
                })
                .await?;
                Ok(())
            }),
            |_config| Ok(Cfg { value: 0 }),
        )
        .expect("register dirty");

    let current = tree(r#"[{"id":"d","name":"dirty"},{"id":"a","name":"p"}]"#);
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");

    // Desired tree drops "d": the disposal quarantines.
    let desired = tree(r#"[{"id":"a","name":"p"}]"#);
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply completes with a per-node failure");
    assert!(!report.is_clean(), "{report:?}");
    assert!(
        report.quarantined.iter().any(|id| id == "d"),
        "quarantine must be reported, not swallowed: {report:?}"
    );
    let failure = report
        .outcomes
        .iter()
        .find(|outcome| outcome.id == "d")
        .expect("d has an outcome");
    assert!(
        failure
            .result
            .as_ref()
            .is_err_and(|reason| reason.contains("quarantined")),
        "the removal must carry the real state: {:?}",
        failure.result
    );
    // The quarantined node keeps its bookkeeping entry (its real state
    // is quarantined, not vanished) but holds no live fiber, and the
    // survivor is untouched.
    assert!(mounted.find("d").is_some());
    assert!(
        mounted.find("d").expect("d").fiber_id().is_none(),
        "the quarantined node must have no live fiber"
    );
    assert!(mounted.find("a").is_some());
    assert_eq!(report.revision, 2);

    app.shutdown(cordis_core::ShutdownOptions::default())
        .await
        .expect("shutdown");
}

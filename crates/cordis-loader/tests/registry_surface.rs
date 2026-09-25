//! Surface coverage for the loader's registry and error surfaces:
//! registration refusals, serde registration, predecode structural
//! rejections, runtime failures mapped through the loader taxonomy, and
//! group-child reconcile paths that walk the mounted tree helpers.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cordis_core::{App, ShutdownOptions, define};
use cordis_loader::{
    Action, ComposeOptions, Layer, LoaderError, Registry, compose, mount, plan, predecode,
    reconcile,
};
use serde::Deserialize;

struct Cfg {
    #[expect(dead_code, reason = "decoder shape only; not read by these plugins")]
    value: u64,
}

#[derive(Deserialize)]
struct SerdeCfg {
    #[serde(default)]
    #[expect(dead_code, reason = "decoder shape only; the plugin never reads it")]
    label: String,
}

fn tree(source: &str) -> cordis_loader::Tree {
    let layer = Layer::parse("t", source).expect("layer parses");
    compose(&[layer], ComposeOptions::default()).expect("compose")
}

fn counter_registry() -> (Registry, Arc<AtomicUsize>) {
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
    (registry, activations)
}

#[test]
fn loader_error_display_covers_every_variant() {
    let cases: Vec<(LoaderError, &str)> = vec![
        (
            LoaderError::Parse {
                layer: "base".to_owned(),
                reason: "trailing data".to_owned(),
            },
            "layer \"base\": trailing data",
        ),
        (
            LoaderError::IntegerOutOfRange {
                layer: "base".to_owned(),
                literal: "18446744073709551616".to_owned(),
            },
            "exceeds the exact",
        ),
        (
            LoaderError::Compose {
                reason: "duplicate ids".to_owned(),
            },
            "compose: duplicate ids",
        ),
        (
            LoaderError::UnknownPlugin {
                entry: "a (p2)".to_owned(),
                plugin: "p2".to_owned(),
            },
            "references unknown plugin",
        ),
        (
            LoaderError::InvalidConfig {
                entry: "a".to_owned(),
                plugin: "p".to_owned(),
                reason: "bad".to_owned(),
            },
            "invalid config: bad",
        ),
        (
            LoaderError::DuplicateRegistration {
                plugin: "p".to_owned(),
            },
            "already registered",
        ),
        (
            LoaderError::PlanSuperseded {
                planned_for: 1,
                current: 2,
            },
            "revision 1 but the tree is at revision 2",
        ),
        (
            LoaderError::MissingNodeId {
                entry: "a".to_owned(),
            },
            "carries no id",
        ),
        (
            LoaderError::MountFailed {
                reason: "activation refused".to_owned(),
            },
            "mount failed: activation refused",
        ),
    ];
    for (error, needle) in &cases {
        let text = error.to_string();
        assert!(text.contains(needle), "missing {needle:?} in {text:?}");
        let _ = format!("{error:?}");
    }
}

#[tokio::test]
async fn registry_refuses_empty_and_duplicate_names() {
    let (mut registry, _) = counter_registry();
    assert!(registry.contains("p"));
    assert!(!registry.contains("nope"));

    let empty = registry
        .register(
            "",
            define("x", |_ctx, _cfg: Arc<Cfg>| async { Ok(()) }),
            |_| Ok(Cfg { value: 0 }),
        )
        .expect_err("empty name refused");
    assert!(matches!(empty, LoaderError::Parse { .. }), "{empty:?}");

    let duplicate = registry
        .register(
            "p",
            define("p2", |_ctx, _cfg: Arc<Cfg>| async { Ok(()) }),
            |_| Ok(Cfg { value: 0 }),
        )
        .expect_err("duplicate refused");
    assert!(
        matches!(duplicate, LoaderError::DuplicateRegistration { .. }),
        "{duplicate:?}"
    );
}

#[tokio::test]
async fn register_serde_decodes_through_serde_json() {
    let mut registry = Registry::new();
    registry
        .register_serde(
            "s",
            define("s", |_ctx, _cfg: Arc<SerdeCfg>| async { Ok(()) }),
        )
        .expect("register s");
    assert!(registry.contains("s"));

    // Well-formed config predecodes cleanly.
    let good = tree(r#"[{"id":"a","name":"s","config":{"label":"x"}}]"#);
    predecode(&good, &registry).expect("good config decodes");

    // Wrong shape fails predecode with the entry and plugin named.
    let bad = tree(r#"[{"id":"a","name":"s","config":{"label":7}}]"#);
    let err = predecode(&bad, &registry).expect_err("bad config refused");
    match &err {
        LoaderError::InvalidConfig { entry, plugin, .. } => {
            assert!(entry.contains('a'), "{entry}");
            assert_eq!(plugin, "s");
        }
        other => panic!("expected InvalidConfig, got {other:?}"),
    }
}

#[tokio::test]
async fn predecode_rejects_structural_shapes() {
    let (registry, _) = counter_registry();

    // A plain node carrying children is not a group: refused loudly.
    let layered =
        tree(r#"[{"id":"g","name":"p","config":{"value":1},"plugins":[{"id":"c","name":"p"}]}]"#);
    let err = predecode(&layered, &registry).expect_err("non-group with plugins refused");
    assert!(matches!(err, LoaderError::MountFailed { .. }), "{err:?}");

    // Unknown plugin names never reach the running tree.
    let unknown = tree(r#"[{"id":"a","name":"nope"}]"#);
    let err = predecode(&unknown, &registry).expect_err("unknown plugin refused");
    assert!(matches!(err, LoaderError::UnknownPlugin { .. }), "{err:?}");
}

#[tokio::test]
async fn mount_on_a_dead_app_maps_the_runtime_refusal() {
    let (registry, _) = counter_registry();
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");

    let desired = tree(r#"[{"id":"a","name":"p","config":{"value":1}}]"#);
    let err = mount(&desired, &registry, &root)
        .await
        .expect_err("dead host refused");
    match &err {
        LoaderError::MountFailed { reason } => {
            assert!(reason.contains("shut down"), "{reason}");
        }
        other => panic!("expected MountFailed, got {other:?}"),
    }
}

#[tokio::test]
async fn group_child_reconcile_walks_the_mounted_tree_paths() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let (registry, activations) = counter_registry();

    // A group with two children; the desired tree updates one child,
    // removes the other and inserts a third.
    let current = tree(
        r#"[{"id":"g","name":"cordis.group","group":true,"plugins":[
            {"id":"keep","name":"p","config":{"value":1}},
            {"id":"drop","name":"p","config":{"value":2}}]}]"#,
    );
    let mut mounted = mount(&current, &registry, &root).await.expect("mount");
    let keep_id = mounted
        .find("g")
        .expect("group")
        .fiber_id()
        .expect("group fiber");
    assert_eq!(activations.load(Ordering::SeqCst), 2);

    let desired = tree(
        r#"[{"id":"g","name":"cordis.group","group":true,"plugins":[
            {"id":"keep","name":"p","config":{"value":9}},
            {"id":"fresh","name":"p","config":{"value":3}}]}]"#,
    );
    let planned = plan(&current, &desired, mounted.revision()).expect("plan");
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "g/keep" && e.action == Action::Update)
    );
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "g/drop" && e.action == Action::Remove)
    );
    assert!(
        planned
            .entries
            .iter()
            .any(|e| e.id_path() == "g/fresh" && e.action == Action::Insert)
    );

    let report = reconcile(&mut mounted, planned, &desired, &registry, &root)
        .await
        .expect("apply");
    assert!(report.is_clean(), "{report:?}");

    // Group children follow the same Update semantics as top-level
    // nodes: the changed child restarts its generation (one more apply
    // run) while the group itself and the untouched children never
    // restart — drop is disposed, fresh activates once, keep re-applies.
    assert_eq!(activations.load(Ordering::SeqCst), 4);
    assert_eq!(
        mounted.find("g").expect("group").fiber_id().expect("fiber"),
        keep_id
    );

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

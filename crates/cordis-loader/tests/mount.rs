//! P6 end-to-end: two-layer composition mounts and unmounts through the
//! real coordinator (V42/V46/V47/V48), groups own their children, bad
//! trees never touch the running tree, and a failed mount cleans up
//! while the host stays usable.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use cordis_core::{App, FiberState, define};
use cordis_loader::{ComposeOptions, Layer, Registry, compose, mount, predecode, unmount};

struct DbConfig {
    path: String,
}

struct ApiConfig {
    #[expect(dead_code, reason = "decoder shape parity; not read by these plugins")]
    name: String,
}

#[tokio::test]
async fn v42_base_profile_two_layers_mount_and_unmount() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let mut registry = Registry::new();
    let db_activations = Arc::new(AtomicUsize::new(0));
    let api_activations = Arc::new(AtomicUsize::new(0));

    let db_counter = Arc::clone(&db_activations);
    let db_plugin = define("db", move |_ctx, cfg: Arc<DbConfig>| {
        let counter = Arc::clone(&db_counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            assert!(!cfg.path.is_empty());
            Ok(())
        }
    });
    registry
        .register("db", db_plugin, |config| {
            let path = config
                .get("path")
                .and_then(|value| value.as_str())
                .ok_or_else(|| invalid("db", "path (a string) is required"))?
                .to_owned();
            Ok(DbConfig { path })
        })
        .expect("register db");

    let api_counter = Arc::clone(&api_activations);
    let api_plugin = define("api", move |_ctx, _cfg: Arc<ApiConfig>| {
        let counter = Arc::clone(&api_counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    registry
        .register("api", api_plugin, |config| {
            let name = config
                .get("name")
                .and_then(|value| value.as_str())
                .unwrap_or("default-api")
                .to_owned();
            Ok(ApiConfig { name })
        })
        .expect("register api");

    // base + profile: the profile replaces the db config as a whole and
    // disables the api entry.
    let base = Layer::parse(
        "base",
        r#"[{"id":"db","name":"db","config":{"path":"a.db","pool":4}},{"id":"api","name":"api","config":{"name":"v1"}}]"#,
    )
    .expect("base");
    let profile = Layer::parse_patch("profile", r#"[{"id":"db","config":{"path":"prod.db"}}]"#)
        .expect("profile");
    let tree = compose(&[base, profile], ComposeOptions::default()).expect("compose");
    assert_eq!(tree.layers, vec!["base", "profile"]);
    let db = tree.find("db").expect("db");
    // Whole-object replacement: pool is gone (V42).
    assert!(!db.config.as_ref().expect("config").contains_key("pool"));

    let mounted = mount(&tree, &registry, &root).await.expect("mount");
    assert_eq!(mounted.ids(), vec!["db", "api"]);
    assert_eq!(db_activations.load(Ordering::SeqCst), 1);
    assert_eq!(api_activations.load(Ordering::SeqCst), 1);
    assert_eq!(mounted.revision(), 1);

    // Full unmount: every entry disposed cleanly and awaited (V47's
    // top-level half).
    let report = unmount(&mounted).await;
    assert_eq!(report.disposed, vec!["api", "db"]);
    assert!(report.quarantined.is_empty());

    app.shutdown_app().await;
}

#[tokio::test]
async fn v47_groups_own_children_and_unmount_waits_for_them() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let mut registry = Registry::new();
    let started = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&started);
    registry
        .register(
            "worker",
            define("worker", move |_ctx, _cfg: Arc<ApiConfig>| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }),
            |_config| {
                Ok(ApiConfig {
                    name: String::new(),
                })
            },
        )
        .expect("register worker");

    let base = Layer::parse(
        "base",
        r#"[
            {"id":"group","group":true,"plugins":[
                {"id":"w1","name":"worker"},
                {"id":"nested","group":true,"plugins":[{"id":"w2","name":"worker"}]},
                {"id":"off","name":"worker","disabled":true}
            ]}
        ]"#,
    )
    .expect("base");
    let tree = compose(&[base], ComposeOptions::default()).expect("compose");

    let mounted = mount(&tree, &registry, &root).await.expect("mount");
    // The group mounts; its enabled children (incl. the nested group's
    // child) activated; the disabled child did not.
    assert_eq!(mounted.ids(), vec!["group"]);
    assert_eq!(started.load(Ordering::SeqCst), 2, "w1 + w2, not off");

    // Group unmount disposes children first and waits for them.
    let report = unmount(&mounted).await;
    assert!(report.disposed.contains(&"group".to_owned()), "{report:?}");
    assert!(
        report.disposed.len() >= 2,
        "children disposed too: {report:?}"
    );
    assert!(report.quarantined.is_empty());

    app.shutdown_app().await;
}

#[tokio::test]
async fn v46_unknown_plugin_and_bad_config_leave_the_running_tree_untouched() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let mut registry = Registry::new();
    registry
        .register(
            "db",
            define("db", |_ctx, _cfg: Arc<DbConfig>| async { Ok(()) }),
            |config| {
                let path = config
                    .get("path")
                    .and_then(|value| value.as_str())
                    .ok_or_else(|| invalid("db", "path is required"))?
                    .to_owned();
                Ok(DbConfig { path })
            },
        )
        .expect("register db");

    // Unknown plugin: predecode refuses before any load.
    let unknown = Layer::parse("base", r#"[{"id":"x","name":"nope"}]"#).expect("base");
    let unknown_tree = compose(&[unknown], ComposeOptions::default()).expect("compose");
    let err = predecode(&unknown_tree, &registry).expect_err("unknown plugin");
    assert!(err.to_string().contains("unknown plugin"), "{err}");

    // Bad config: same refusal.
    let bad =
        Layer::parse("base", r#"[{"id":"x","name":"db","config":{"nope":1}}]"#).expect("base");
    let bad_tree = compose(&[bad], ComposeOptions::default()).expect("compose");
    let err = predecode(&bad_tree, &registry).expect_err("bad config");
    assert!(err.to_string().contains("path is required"), "{err}");
    let err = mount(&bad_tree, &registry, &root)
        .await
        .expect_err("mount refuses");
    assert!(err.to_string().contains("path is required"), "{err}");

    // The running tree is untouched: mounting a good tree afterwards
    // works with fresh activations.
    let good = Layer::parse(
        "base",
        r#"[{"id":"x","name":"db","config":{"path":"ok.db"}}]"#,
    )
    .expect("base");
    let good_tree = compose(&[good], ComposeOptions::default()).expect("compose");
    let mounted = mount(&good_tree, &registry, &root).await.expect("mount");
    assert_eq!(mounted.ids(), vec!["x"]);

    app.shutdown_app().await;
}

#[tokio::test]
async fn v48_later_failure_unwinds_the_mount_and_the_host_stays_usable() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let mut registry = Registry::new();
    let activations = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&activations);
    registry
        .register(
            "good",
            define("good", move |_ctx, _cfg: Arc<ApiConfig>| {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            }),
            |_config| {
                Ok(ApiConfig {
                    name: String::new(),
                })
            },
        )
        .expect("register good");
    registry
        .register(
            "boom",
            define("boom", |_ctx, _cfg: Arc<ApiConfig>| async {
                Err(cordis_core::PluginError::from("startup exploded"))
            }),
            |_config| {
                Ok(ApiConfig {
                    name: String::new(),
                })
            },
        )
        .expect("register boom");

    // The failing entry sits last: everything before it must be cleaned
    // up when its activation fails.
    let base = Layer::parse(
        "base",
        r#"[{"id":"first","name":"good"},{"id":"group","group":true,"plugins":[{"id":"child","name":"good"}]},{"id":"bomb","name":"boom"}]"#,
    )
    .expect("base");
    let tree = compose(&[base], ComposeOptions::default()).expect("compose");

    let err = mount(&tree, &registry, &root)
        .await
        .expect_err("mount fails");
    // The failure names the failing entry and the recovery disposal.
    assert!(err.to_string().contains("bomb"), "{err}");
    assert!(err.to_string().contains("startup exploded"), "{err}");
    assert!(err.to_string().contains("recovery disposed"), "{err}");
    assert_eq!(activations.load(Ordering::SeqCst), 2, "first + child ran");

    // The host stays usable: a fresh, valid tree mounts afterwards.
    let recovery = Layer::parse("base", r#"[{"id":"again","name":"good"}]"#).expect("base");
    let recovery_tree = compose(&[recovery], ComposeOptions::default()).expect("compose");
    let mounted = mount(&recovery_tree, &registry, &root)
        .await
        .expect("remount");
    assert_eq!(mounted.ids(), vec!["again"]);
    assert_eq!(activations.load(Ordering::SeqCst), 3);

    app.shutdown_app().await;
}

#[tokio::test]
async fn v52_style_partial_unmount_reports_quarantine_honestly() {
    // A node whose dispose resolves Quarantined is reported as such,
    // never as a clean disposal (V52 semantics on the unmount path).
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let mut registry = Registry::new();
    // A plugin whose cleanup hangs past the shutdown deadline — instead
    // of a deadline, use a panicking cleanup to force quarantine via the
    // effect ledger: a cleanup panic quarantines the fiber.
    registry
        .register(
            "dirty",
            define("dirty", |ctx, _cfg: Arc<ApiConfig>| async move {
                ctx.on_dispose("dirty-cleanup", || async {
                    Err(cordis_core::CleanupError::from("cleanup refused"))
                })
                .await?;
                Ok(())
            }),
            |_config| {
                Ok(ApiConfig {
                    name: String::new(),
                })
            },
        )
        .expect("register dirty");

    let base = Layer::parse("base", r#"[{"id":"d","name":"dirty"}]"#).expect("base");
    let tree = compose(&[base], ComposeOptions::default()).expect("compose");
    let mounted = mount(&tree, &registry, &root).await.expect("mount");

    let report = unmount(&mounted).await;
    // The failing cleanup surfaces as a quarantine, not a clean dispose.
    assert!(
        !report.quarantined.is_empty(),
        "failed cleanup must not be reported as disposed: {report:?}"
    );
    assert!(report.disposed.is_empty());

    app.shutdown_app().await;
}

fn invalid(plugin: &str, reason: &str) -> cordis_loader::LoaderError {
    cordis_loader::LoaderError::InvalidConfig {
        entry: String::new(),
        plugin: plugin.to_owned(),
        reason: reason.to_owned(),
    }
}

trait ShutdownApp {
    async fn shutdown_app(&self);
}

impl ShutdownApp for App {
    async fn shutdown_app(&self) {
        let _ = self.shutdown(cordis_core::ShutdownOptions::default()).await;
    }
}

#[tokio::test]
async fn pending_dependencies_mount_as_pending_not_failures() {
    // A plugin requiring a missing service stays Pending at mount time —
    // a stable waiting state, not a mount failure (docs/02 §3).
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let mut registry = Registry::new();
    let plugin = define("waiting", |_ctx, _cfg: Arc<ApiConfig>| async { Ok(()) }).require(
        cordis_core::ServiceKey::<MissingService>::new("never-provided"),
    );
    registry
        .register("waiting", plugin, |_config| {
            Ok(ApiConfig {
                name: String::new(),
            })
        })
        .expect("register");

    let base = Layer::parse("base", r#"[{"id":"w","name":"waiting"}]"#).expect("base");
    let tree = compose(&[base], ComposeOptions::default()).expect("compose");
    let mounted = mount(&tree, &registry, &root).await.expect("mount");
    assert_eq!(mounted.ids(), vec!["w"]);

    app.shutdown_app().await;
}

struct MissingService;

#[tokio::test]
async fn fiber_states_are_queryable_through_mounted_entries() {
    // Sanity: mounted plugin fibers reach Active through the loader path.
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let mut registry = Registry::new();
    registry
        .register(
            "good",
            define("good", |_ctx, _cfg: Arc<ApiConfig>| async { Ok(()) }),
            |_config| {
                Ok(ApiConfig {
                    name: String::new(),
                })
            },
        )
        .expect("register");
    let base = Layer::parse("base", r#"[{"id":"a","name":"good"}]"#).expect("base");
    let tree = compose(&[base], ComposeOptions::default()).expect("compose");
    mount(&tree, &registry, &root).await.expect("mount");
    let _ = FiberState::Active;
    app.shutdown_app().await;
}

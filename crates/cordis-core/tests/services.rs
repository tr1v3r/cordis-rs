//! P4 service system: dependency-driven availability convergence over an
//! A→B→C chain (V19), unbind/re-provide epoch reload with stale-context
//! rejection (V20), value `set` never reloading consumers (V21),
//! availability flips under an in-flight `Starting` ticket and dependency
//! cycles (V22/V26), namespace isolation with shared labels (V23), type
//! and permission errors (V24), managed start/stop lifecycle (V25) and
//! lease retirement semantics (V27).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use cordis_core::{
    App, Context, Error, FiberHandle, FiberState, OperationOutcome, Plugin, ServiceKey,
    ServiceLease, ShutdownOptions, define,
};
use tokio::sync::Notify;

struct Cfg;

struct Db {
    url: String,
}

struct Api {
    endpoint: String,
}

struct Cache;

struct Other;

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Db")
    }
}

impl std::fmt::Debug for Other {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Other")
    }
}

const KEY_DB: &str = "db";
const KEY_API: &str = "api";
const KEY_CACHE: &str = "cache";

fn db_key() -> ServiceKey<Db> {
    ServiceKey::new(KEY_DB)
}

fn api_key() -> ServiceKey<Api> {
    ServiceKey::new(KEY_API)
}

/// Slot through which activation bodies hand handles to the test.
type Slot<T> = Arc<Mutex<Option<T>>>;

fn slot<T>() -> Slot<T> {
    Arc::new(Mutex::new(None))
}

/// Waits until `handle` reaches `want`, failing the test on timeout.
async fn await_state<C>(handle: &FiberHandle<C>, want: FiberState) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if handle.status().await.expect("fiber alive").state == want {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("fiber did not reach {want}"));
}

/// A provider that publishes `key = value` from its apply body and
/// records its generation context for later provider-side operations.
fn provider<T>(name: &str, key: ServiceKey<T>, value: Arc<T>) -> (Plugin<Cfg>, Slot<Context>)
where
    T: Send + Sync + 'static,
{
    let ctx_slot = slot::<Context>();
    let plugin = {
        let ctx_slot = Arc::clone(&ctx_slot);
        define(name, move |ctx: Context, _cfg: Arc<Cfg>| {
            let ctx_slot = Arc::clone(&ctx_slot);
            let key = key.clone();
            let value = Arc::clone(&value);
            async move {
                *ctx_slot.lock().unwrap() = Some(ctx.clone());
                ctx.provide(key, value).await?;
                Ok(())
            }
        })
    };
    (plugin, ctx_slot)
}

/// A consumer that requires `key`, counts apply runs, snapshots the value
/// and registers a cleanup that tolerates the dependency disappearing
/// underneath it (V19: old-generation cleanup survives deactivation).
fn consumer(
    name: &str,
    key: ServiceKey<Db>,
    applies: Arc<AtomicUsize>,
    snapshots: Arc<Mutex<Vec<String>>>,
    cleanups: Arc<Mutex<Vec<String>>>,
    lease_slot: Slot<ServiceLease<Db>>,
) -> Plugin<Cfg> {
    let captured_key = key.clone();
    define(name, move |ctx: Context, _cfg: Arc<Cfg>| {
        let applies = Arc::clone(&applies);
        let snapshots = Arc::clone(&snapshots);
        let cleanups = Arc::clone(&cleanups);
        let lease_slot = Arc::clone(&lease_slot);
        let key = captured_key.clone();
        async move {
            let lease = ctx.get(key.clone()).await?;
            let db = lease.snapshot().expect("typed snapshot");
            snapshots.lock().unwrap().push(db.url.clone());
            applies.fetch_add(1, Ordering::SeqCst);
            *lease_slot.lock().unwrap() = Some(lease);
            let probe = ctx.clone();
            ctx.on_dispose("consumer-cleanup", move || {
                let probe = probe.clone();
                let cleanups = Arc::clone(&cleanups);
                async move {
                    // Deliberately tolerates the service being gone: the
                    // error is observed, not propagated.
                    if let Err(Error::ServiceMissing { .. }) = probe.lookup_dynamic(KEY_DB).await {
                        cleanups.lock().unwrap().push("missed".to_owned());
                    } else {
                        cleanups.lock().unwrap().push("present".to_owned());
                    }
                    Ok(())
                }
            })
            .await?;
            Ok(())
        }
    })
    .require(key)
}

/// Acceptance 1 — the A→B→C dependency chain converges through an
/// availability flip of the middle provider, and the discarded
/// generation's cleanup tolerates the loss.
async fn v19_chain_scenario() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let applies = Arc::new(AtomicUsize::new(0));
    let cleanups: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    // C provides db; B requires db and provides api; A requires api.
    // C's provide is parked on a watch the test controls, with an
    // explicit rendezvous: without it, a fast runtime can finish the
    // whole C -> B -> A activation chain between the load admissions
    // below and the "zero applies" assertion — a scheduling race, not a
    // kernel guarantee (docs/07 §8: tests control the exact ordering).
    let (c_release_tx, c_release_rx) = tokio::sync::watch::channel(0u64);
    let (c_parked_tx, mut c_parked_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (db_plugin, _db_ctx) = {
        let gate = c_release_rx.clone();
        let parked = c_parked_tx.clone();
        let ctx_slot = slot::<Context>();
        let plugin = {
            let ctx_slot = Arc::clone(&ctx_slot);
            define("db-provider", move |ctx: Context, _cfg: Arc<Cfg>| {
                let ctx_slot = Arc::clone(&ctx_slot);
                let parked = parked.clone();
                let mut gate = gate.clone();
                let key = db_key();
                async move {
                    // Sample the ordinal BEFORE reporting parked: the
                    // test only releases after seeing the report, so the
                    // release always satisfies this wait.
                    let target = *gate.borrow();
                    let _ = parked.send(());
                    gate.wait_for(|count| *count > target)
                        .await
                        .expect("gate lives");
                    *ctx_slot.lock().unwrap() = Some(ctx.clone());
                    ctx.provide(
                        key,
                        Arc::new(Db {
                            url: "postgres://c".to_owned(),
                        }),
                    )
                    .await?;
                    Ok(())
                }
            })
        };
        (plugin, ctx_slot)
    };
    let api_ctx_slot = slot::<Context>();
    let api_plugin = {
        let ctx_slot = Arc::clone(&api_ctx_slot);
        define("api-provider", move |ctx: Context, _cfg: Arc<Cfg>| {
            let ctx_slot = Arc::clone(&ctx_slot);
            let db_key = db_key();
            let api_key = api_key();
            async move {
                let _db = ctx.get(db_key).await?;
                *ctx_slot.lock().unwrap() = Some(ctx.clone());
                ctx.provide(
                    api_key,
                    Arc::new(Api {
                        endpoint: "https://api".to_owned(),
                    }),
                )
                .await?;
                Ok(())
            }
        })
        .require(db_key())
    };
    let a_ctx_slot = slot::<Context>();
    let a_cleanups = Arc::clone(&cleanups);
    let a_plugin = {
        let applies = Arc::clone(&applies);
        let cleanups = Arc::clone(&a_cleanups);
        let ctx_slot = Arc::clone(&a_ctx_slot);
        define("api-consumer", move |ctx: Context, _cfg: Arc<Cfg>| {
            let applies = Arc::clone(&applies);
            let cleanups = Arc::clone(&cleanups);
            let ctx_slot = Arc::clone(&ctx_slot);
            let key = api_key();
            async move {
                let lease = ctx.get(key).await?;
                let api = lease.snapshot().expect("api snapshot");
                assert_eq!(api.endpoint, "https://api");
                applies.fetch_add(1, Ordering::SeqCst);
                *ctx_slot.lock().unwrap() = Some(ctx.clone());
                let probe = ctx.clone();
                ctx.on_dispose("a-cleanup", move || {
                    let probe = probe.clone();
                    let cleanups = Arc::clone(&cleanups);
                    async move {
                        // The api binding is down while this cleanup runs;
                        // that must be tolerable, not a failure.
                        if let Err(Error::ServiceMissing { .. }) =
                            probe.lookup_dynamic(KEY_API).await
                        {
                            cleanups.lock().unwrap().push("missed".to_owned());
                        } else {
                            cleanups.lock().unwrap().push("present".to_owned());
                        }
                        Ok(())
                    }
                })
                .await?;
                Ok(())
            }
        })
        .require(api_key())
    };

    // Load A before its dependencies exist: Pending, zero apply runs.
    let a = root.load(&a_plugin, Cfg).await.expect("a admitted");
    let b = root.load(&api_plugin, Cfg).await.expect("b admitted");
    let c = root.load(&db_plugin, Cfg).await.expect("c admitted");
    assert_eq!(applies.load(Ordering::SeqCst), 0);

    // Release C's provide: the chain C -> B -> A converges from here.
    // C's apply is parked at the gate (rendezvous): only now release it,
    // and let the chain C -> B -> A converge.
    tokio::time::timeout(Duration::from_secs(15), c_parked_rx.recv())
        .await
        .expect("C parks at the gate")
        .expect("park channel lives");
    c_release_tx.send_modify(|count| *count += 1);
    let gen_a_1 = a
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("chain activates");
    b.fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("b active");
    c.fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("c active");
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    // B deactivates its api binding: A converges to Pending, B stays
    // Active (its own dependency is untouched).
    let b_ctx = api_ctx_slot.lock().unwrap().clone().expect("b ctx");
    b_ctx
        .set_available(api_key(), false)
        .await
        .expect("provider flips availability");
    await_state(&a.fiber, FiberState::Pending).await;
    assert_eq!(b.fiber.status().await.unwrap().state, FiberState::Active);

    // B recovers: A activates under a new generation.
    b_ctx.set_available(api_key(), true).await.expect("recover");
    let gen_a_2 = a
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("A reactivates");
    assert_ne!(gen_a_1, gen_a_2, "recovery is a new generation");
    assert_eq!(applies.load(Ordering::SeqCst), 2);

    // The old generation's cleanup ran while api was unavailable.
    assert_eq!(
        *cleanups.lock().unwrap(),
        vec!["missed".to_owned()],
        "old-generation cleanup executed and tolerated the loss"
    );
    let _ = a_ctx_slot;

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// Acceptance 3 + V20 — unbind/re-provide produces a fresh epoch; the
/// captured context of the disposed generation cannot register, set or
/// flip anything in the new epoch.
async fn v20_epoch_reload_scenario() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let applies = Arc::new(AtomicUsize::new(0));
    let snapshots: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let cleanups: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let provider_ctx_slot = slot::<Context>();

    let db_plugin = {
        let ctx_slot = Arc::clone(&provider_ctx_slot);
        define("db", move |ctx: Context, _cfg: Arc<Cfg>| {
            let ctx_slot = Arc::clone(&ctx_slot);
            let key = db_key();
            async move {
                *ctx_slot.lock().unwrap() = Some(ctx.clone());
                ctx.provide(
                    key,
                    Arc::new(Db {
                        url: "generation-1".to_owned(),
                    }),
                )
                .await?;
                Ok(())
            }
        })
    };
    let consumer_plugin = consumer(
        "db-consumer",
        db_key(),
        Arc::clone(&applies),
        Arc::clone(&snapshots),
        Arc::clone(&cleanups),
        slot(),
    );

    let first = root.load(&db_plugin, Cfg).await.expect("first provider");
    let consumer = root.load(&consumer_plugin, Cfg).await.expect("consumer");
    let generation_1 = consumer
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("consumer active on the first binding");
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    // Unbind: the provider's disposal retires the binding; the consumer
    // converges to Pending (a waiting state, never Failed).
    let dispose = first.fiber.dispose().await.expect("dispose op");
    dispose.wait().await.expect("provider disposed");
    await_state(&consumer.fiber, FiberState::Pending).await;
    assert_eq!(applies.load(Ordering::SeqCst), 1);

    // Capture the disposed provider's generation view *before* the new
    // provider overwrites the slot.
    let old_ctx = provider_ctx_slot.lock().unwrap().clone().expect("old ctx");

    // Re-provide: same definition, new fiber — a fresh BindingId (ABA
    // guard), so the consumer activates under a new generation.
    let second = root.load(&db_plugin, Cfg).await.expect("second provider");
    let generation_2 = consumer
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("consumer reactivates");
    assert_ne!(generation_1, generation_2);
    assert_eq!(applies.load(Ordering::SeqCst), 2);
    let _ = second;

    // The captured context belongs to the disposed provider generation.
    let stale_value = || {
        Arc::new(Db {
            url: "stale".to_owned(),
        })
    };
    let old_provide = old_ctx.provide(db_key(), stale_value()).await;
    assert!(
        matches!(old_provide, Err(Error::StaleGeneration { .. })),
        "old context cannot provide into the new epoch: {old_provide:?}"
    );
    let old_set = old_ctx.set(db_key(), stale_value()).await;
    assert!(
        matches!(old_set, Err(Error::StaleGeneration { .. })),
        "old context cannot set the new binding: {old_set:?}"
    );
    let old_flip = old_ctx.set_available(db_key(), false).await;
    assert!(
        matches!(old_flip, Err(Error::StaleGeneration { .. })),
        "old context cannot flip availability: {old_flip:?}"
    );
    // And its reads are refused too, never silently rerouted.
    let old_get = old_ctx.get(db_key()).await;
    assert!(matches!(old_get, Err(Error::StaleGeneration { .. })));

    // A live but foreign view (the host root) resolves the new binding
    // and is refused for ownership reasons, not staleness.
    let foreign_set = root.set(db_key(), stale_value()).await;
    assert!(matches!(foreign_set, Err(Error::InvalidOwner)));

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// Acceptance 2 + V21 — `set` replaces values without reloading the
/// consumer; leases see the new value, previously taken arcs keep theirs.
#[tokio::test]
async fn v21_set_replaces_values_without_reloading_consumers() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let applies = Arc::new(AtomicUsize::new(0));
    let snapshots: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let cleanups: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let lease_slot = slot::<ServiceLease<Db>>();
    let provider_ctx_slot = slot::<Context>();

    let db_plugin = {
        let ctx_slot = Arc::clone(&provider_ctx_slot);
        define("db", move |ctx: Context, _cfg: Arc<Cfg>| {
            let ctx_slot = Arc::clone(&ctx_slot);
            let key = db_key();
            let value = Arc::new(Db {
                url: "v1".to_owned(),
            });
            async move {
                *ctx_slot.lock().unwrap() = Some(ctx.clone());
                ctx.provide(key, value).await?;
                Ok(())
            }
        })
    };
    let consumer_plugin = consumer(
        "db-consumer",
        db_key(),
        Arc::clone(&applies),
        Arc::clone(&snapshots),
        Arc::clone(&cleanups),
        Arc::clone(&lease_slot),
    );

    let provider = root.load(&db_plugin, Cfg).await.expect("provider");
    let consumer = root.load(&consumer_plugin, Cfg).await.expect("consumer");
    let generation = consumer
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("consumer active");
    let lease = lease_slot.lock().unwrap().clone().expect("lease captured");
    let taken_v1 = lease.snapshot().expect("first snapshot");
    assert_eq!(taken_v1.url, "v1");
    assert_eq!(lease.revision(), Some(0));

    let provider_ctx = provider_ctx_slot.lock().unwrap().clone().expect("ctx");

    // Set a new value: leases observe it, the consumer does not reload.
    provider_ctx
        .set(
            db_key(),
            Arc::new(Db {
                url: "v2".to_owned(),
            }),
        )
        .await
        .expect("set new value");
    assert_eq!(
        consumer.fiber.status().await.unwrap().state,
        FiberState::Active
    );
    assert_eq!(
        consumer.fiber.status().await.unwrap().active_generation,
        Some(generation),
        "same generation after set"
    );
    assert_eq!(applies.load(Ordering::SeqCst), 1, "no consumer re-run");
    assert_eq!(
        lease.snapshot().unwrap().url,
        "v2",
        "lease sees the new value"
    );
    assert_eq!(taken_v1.url, "v1", "taken arcs keep the old value");
    assert_eq!(lease.revision(), Some(1));

    // Re-sending the same value also never reloads (same-value Set).
    provider_ctx
        .set(
            db_key(),
            Arc::new(Db {
                url: "v2".to_owned(),
            }),
        )
        .await
        .expect("same-value set");
    assert_eq!(
        consumer.fiber.status().await.unwrap().state,
        FiberState::Active
    );
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    assert_eq!(lease.revision(), Some(2));
    assert_eq!(lease.snapshot().unwrap().url, "v2");
    let _ = provider;

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// Acceptance 4 + V23 — isolated namespaces never fall back to outer
/// ones and diagnose by namespace; shared labels merge per service name.
#[tokio::test]
async fn v23_isolated_namespaces_do_not_fall_back_and_share_labels_per_service() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    // A provider in the default namespace.
    let (db_plugin, _ctx) = provider(
        "db",
        db_key(),
        Arc::new(Db {
            url: "default-db".to_owned(),
        }),
    );
    let default_provider = root.load(&db_plugin, Cfg).await.expect("load");
    default_provider
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("default provider active");

    // Root sees it; an isolated view does not — and the error names the
    // isolated namespace instead of silently falling back.
    assert!(root.lookup_dynamic(KEY_DB).await.is_ok());
    let isolated = root.isolate(KEY_DB);
    let err = isolated
        .lookup_dynamic(KEY_DB)
        .await
        .expect_err("no fallback across isolation");
    assert!(
        matches!(&err, Error::ServiceMissing { service, namespace }
            if service == KEY_DB && namespace.starts_with("unique(")),
        "namespace must name the isolated scope: {err}"
    );
    // The original view is untouched (P4.6: deriving never mutates).
    assert!(root.lookup_dynamic(KEY_DB).await.is_ok());

    // A consumer loaded through the isolated view stays Pending even
    // though the default namespace has a provider.
    let applies = Arc::new(AtomicUsize::new(0));
    let isolated_consumer = {
        let applies = Arc::clone(&applies);
        define("isolated-consumer", move |ctx: Context, _cfg: Arc<Cfg>| {
            let applies = Arc::clone(&applies);
            let key = db_key();
            async move {
                let _ = ctx.get(key).await?;
                applies.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .require(db_key())
    };
    let isolated_receipt = isolated
        .load(&isolated_consumer, Cfg)
        .await
        .expect("isolated consumer admitted");
    let outcome = Arc::clone(
        &isolated_receipt
            .operation
            .wait()
            .await
            .expect("settles as pending"),
    );
    assert!(
        matches!(&*outcome, OperationOutcome::Pending { missing }
            if missing.len() == 1 && missing[0].contains("unique(")),
        "pending reason must carry the namespace: {outcome:?}"
    );
    assert_eq!(
        isolated_receipt.fiber.status().await.unwrap().state,
        FiberState::Pending
    );
    assert_eq!(applies.load(Ordering::SeqCst), 0);

    // Shared labels merge exactly one service name: a provider bound in
    // shared("team") for `cache` is invisible from the default namespace
    // and from a shared view of a *different* service, but visible from
    // another shared("team") view of `cache`.
    let team_view = root.isolate_shared(KEY_CACHE, "team");
    let (cache_plugin, _cache_ctx) =
        provider("cache-team", ServiceKey::new(KEY_CACHE), Arc::new(Cache));
    let cache_provider = team_view.load(&cache_plugin, Cfg).await.expect("load");
    cache_provider
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("team cache active");

    assert!(root.lookup_dynamic(KEY_CACHE).await.is_err());
    assert!(team_view.lookup_dynamic(KEY_CACHE).await.is_ok());
    let other_team_view = root.isolate_shared(KEY_CACHE, "team");
    assert!(other_team_view.lookup_dynamic(KEY_CACHE).await.is_ok());
    // The same label for a different service stays a different namespace:
    // db resolves to shared("team") there, and only the default namespace
    // has a db binding.
    let db_via_team = root.isolate_shared(KEY_DB, "team");
    assert!(db_via_team.lookup_dynamic(KEY_DB).await.is_err());

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// V24 — type and permission errors are explicit, never panics or silent
/// cross-type reads.
#[tokio::test]
async fn v24_type_and_permission_errors_are_explicit() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let (db_plugin, _ctx) = provider(
        "db",
        db_key(),
        Arc::new(Db {
            url: "typed".to_owned(),
        }),
    );
    let provider_handle = root.load(&db_plugin, Cfg).await.expect("load");
    provider_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("provider active");

    // Same name, different expected type: an explicit type error.
    let wrong_type_err = root
        .lookup_dynamic(KEY_DB)
        .await
        .expect("dynamic lease")
        .snapshot_as::<Other>()
        .expect_err("type mismatch");
    assert!(matches!(wrong_type_err, Error::ServiceTypeMismatch { .. }));

    // Undeclared reads from the host root are refused.
    let undeclared = root.get(db_key()).await.expect_err("no declaration");
    assert!(matches!(undeclared, Error::UndeclaredDependency { service } if service == KEY_DB));
    // Setting someone else's binding is an owner error.
    let owner_err = root
        .set(
            db_key(),
            Arc::new(Db {
                url: "hijack".to_owned(),
            }),
        )
        .await
        .expect_err("not the owner");
    assert!(matches!(owner_err, Error::InvalidOwner));

    // Inside a declared consumer: wrong type is explicit, an undeclared
    // name is explicit, the declared read succeeds.
    let seen: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
    let consumer_plugin = {
        let seen = Arc::clone(&seen);
        define("typed-consumer", move |ctx: Context, _cfg: Arc<Cfg>| {
            let seen = Arc::clone(&seen);
            let declared = db_key();
            let wrong_type = ServiceKey::<Other>::new(KEY_DB);
            async move {
                match ctx.get(wrong_type).await {
                    Err(Error::ServiceTypeMismatch { .. }) => seen.lock().unwrap().push("type"),
                    other => panic!("expected type mismatch, got {other:?}"),
                }
                let undeclared = ServiceKey::<Cache>::new(KEY_CACHE);
                match ctx.get(undeclared).await {
                    Err(Error::UndeclaredDependency { .. }) => {
                        seen.lock().unwrap().push("undeclared");
                    }
                    other => panic!("expected undeclared, got {other:?}"),
                }
                let lease = ctx.get(declared).await.expect("declared read works");
                assert_eq!(lease.snapshot().unwrap().url, "typed");
                seen.lock().unwrap().push("ok");
                Ok(())
            }
        })
        .require(db_key())
    };
    let consumer_handle = root.load(&consumer_plugin, Cfg).await.expect("load");
    consumer_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("consumer active");
    assert_eq!(*seen.lock().unwrap(), vec!["type", "undeclared", "ok"]);

    // Conflicting duplicate declarations are refused at admission.
    let conflicting = define("conflicting", |_ctx, _cfg: Arc<Cfg>| async { Ok(()) })
        .require(db_key())
        .require(ServiceKey::<Other>::new(KEY_DB));
    let conflict_err = root.load(&conflicting, Cfg).await.expect_err("conflict");
    assert!(
        matches!(&conflict_err, Error::InvalidDependency { service, .. } if service == KEY_DB),
        "expected InvalidDependency, got {conflict_err:?}"
    );

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// V25 — managed services publish only after a successful start, and a
/// failed start frees the slot.
#[tokio::test]
async fn v25_managed_services_publish_after_start_and_fail_without_slot() {
    // --- Success path: invisible until start completes. ---
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let start_gate = Arc::new(Notify::new());

    let managed_plugin = {
        let start_gate = Arc::clone(&start_gate);
        define("managed-db", move |ctx: Context, _cfg: Arc<Cfg>| {
            let start_gate = Arc::clone(&start_gate);
            let key = db_key();
            let value = Arc::new(Db {
                url: "managed".to_owned(),
            });
            async move {
                let gate = Arc::clone(&start_gate);
                ctx.provide_managed(
                    key,
                    value,
                    move || async move {
                        gate.notified().await;
                        Ok(())
                    },
                    || async { Ok(()) },
                )
                .await?;
                Ok(())
            }
        })
    };
    let applies = Arc::new(AtomicUsize::new(0));
    let consumer_plugin = {
        let applies = Arc::clone(&applies);
        define("managed-consumer", move |ctx: Context, _cfg: Arc<Cfg>| {
            let applies = Arc::clone(&applies);
            let key = db_key();
            async move {
                let _ = ctx.get(key).await?;
                applies.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .require(db_key())
    };

    let managed = root.load(&managed_plugin, Cfg).await.expect("load");
    let consumer = root.load(&consumer_plugin, Cfg).await.expect("load");
    managed
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("provider fiber active while its managed start is gated");
    assert!(
        root.lookup_dynamic(KEY_DB).await.is_err(),
        "managed service is invisible before start succeeds"
    );
    assert_eq!(
        consumer.fiber.status().await.unwrap().state,
        FiberState::Pending
    );
    assert_eq!(applies.load(Ordering::SeqCst), 0);

    start_gate.notify_one();
    consumer
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("consumer activates after start");
    assert_eq!(applies.load(Ordering::SeqCst), 1);
    assert!(root.lookup_dynamic(KEY_DB).await.is_ok());

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");

    // --- Failure path: failed start fails the provider and leaves no
    // occupied slot behind. ---
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let failing_plugin = define("failing-managed", move |ctx: Context, _cfg: Arc<Cfg>| {
        let key = db_key();
        async move {
            ctx.provide_managed(
                key,
                Arc::new(Db {
                    url: "doomed".to_owned(),
                }),
                || async { Err(cordis_core::PluginError::from("connection refused")) },
                || async { Ok(()) },
            )
            .await?;
            Ok(())
        }
    });
    let failing = root.load(&failing_plugin, Cfg).await.expect("load");
    await_state(&failing.fiber, FiberState::Failed).await;

    // The slot is free: a plain provide of the same name succeeds.
    let (plain_plugin, _ctx) = provider(
        "db-plain",
        db_key(),
        Arc::new(Db {
            url: "plain".to_owned(),
        }),
    );
    let plain = root.load(&plain_plugin, Cfg).await.expect("slot was free");
    plain
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("plain provider takes the freed slot");
    let lease = root.lookup_dynamic(KEY_DB).await.expect("visible now");
    assert_eq!(lease.snapshot_as::<Db>().unwrap().url, "plain");

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// V22/V26 — availability flips under an in-flight `Starting` ticket
/// invalidate it, and dependency cycles settle Pending without busy
/// reconciliation.
async fn v26_flip_scenario() {
    // --- Flip under an in-flight Starting ticket. ---
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    // A latched gate: once opened it stays open, so the *replacement*
    // generation's apply does not block a second time (a one-shot notify
    // would be consumed by the discarded attempt).
    let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
    let reached_get = Arc::new(AtomicUsize::new(0));

    let (db_plugin, db_ctx_slot) = provider(
        "db",
        db_key(),
        Arc::new(Db {
            url: "flip".to_owned(),
        }),
    );
    let provider_handle = root.load(&db_plugin, Cfg).await.expect("load");
    provider_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("provider active");

    let applies = Arc::new(AtomicUsize::new(0));
    let first_generation: Arc<Mutex<Option<cordis_core::GenerationId>>> =
        Arc::new(Mutex::new(None));
    let slow_consumer = {
        let applies = Arc::clone(&applies);
        let gate_rx = gate_rx.clone();
        let reached_get = Arc::clone(&reached_get);
        let first_generation = Arc::clone(&first_generation);
        define("slow-consumer", move |ctx: Context, _cfg: Arc<Cfg>| {
            let applies = Arc::clone(&applies);
            let mut gate = gate_rx.clone();
            let reached_get = Arc::clone(&reached_get);
            let first_generation = Arc::clone(&first_generation);
            let key = db_key();
            async move {
                let lease = ctx.get(key).await?;
                let _ = lease.snapshot().expect("snapshot");
                *first_generation.lock().unwrap() = ctx.generation_id();
                reached_get.fetch_add(1, Ordering::SeqCst);
                applies.fetch_add(1, Ordering::SeqCst);
                while !*gate.borrow_and_update() {
                    gate.changed().await.expect("gate alive");
                }
                Ok(())
            }
        })
        .require(db_key())
    };
    let consumer_handle = root.load(&slow_consumer, Cfg).await.expect("admitted");
    let reached = tokio::time::timeout(Duration::from_secs(15), async {
        while reached_get.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "consumer apply reached the barrier");
    let generation_1 = first_generation.lock().unwrap().expect("captured");

    // Flip availability false then true while the ticket is in flight.
    let provider_ctx = db_ctx_slot.lock().unwrap().clone().expect("provider ctx");
    provider_ctx
        .set_available(db_key(), false)
        .await
        .expect("false");
    provider_ctx
        .set_available(db_key(), true)
        .await
        .expect("true");
    gate_tx.send(true).expect("open the gate");

    let generation_2 = consumer_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("consumer settles after the flip");
    assert_ne!(
        generation_1, generation_2,
        "the pre-flip Starting ticket must never publish"
    );
    assert_eq!(
        applies.load(Ordering::SeqCst),
        2,
        "one discarded attempt plus one fresh generation, never a loop"
    );

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");

    // --- Dependency cycle: both fibers Pending, zero apply runs, and
    // the kernel quiesces (no busy reconciliation). ---
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let applies = Arc::new(AtomicUsize::new(0));
    let left_key = ServiceKey::<Db>::new("left-svc");
    let right_key = ServiceKey::<Db>::new("right-svc");

    let side = |name: &'static str, provides: ServiceKey<Db>, requires: ServiceKey<Db>| {
        let applies = Arc::clone(&applies);
        define(name, move |ctx: Context, _cfg: Arc<Cfg>| {
            let applies = Arc::clone(&applies);
            let provides = provides.clone();
            async move {
                ctx.provide(
                    provides,
                    Arc::new(Db {
                        url: name.to_owned(),
                    }),
                )
                .await?;
                applies.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        })
        .require(requires)
    };
    let left = side("left", left_key.clone(), right_key.clone());
    let right = side("right", right_key.clone(), left_key.clone());
    let left_handle = root.load(&left, Cfg).await.expect("left admitted");
    let right_handle = root.load(&right, Cfg).await.expect("right admitted");

    let settled = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let left_state = left_handle.fiber.status().await.unwrap().state;
            let right_state = right_handle.fiber.status().await.unwrap().state;
            let stats = app.stats().await.expect("stats");
            if left_state == FiberState::Pending
                && right_state == FiberState::Pending
                && stats.dirty_queue_len == 0
                && stats.operations_pending == 0
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    assert!(
        settled.is_ok(),
        "cycle settles Pending with an empty dirty queue"
    );
    assert_eq!(
        applies.load(Ordering::SeqCst),
        0,
        "no apply runs on a cycle"
    );
    let left_status = left_handle.fiber.status().await.expect("alive");
    assert!(
        left_status
            .pending_reason
            .as_deref()
            .is_some_and(|reason| reason.contains("namespace default")),
        "cycle diagnosis names the missing namespace: {:?}",
        left_status.pending_reason
    );

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// V27 — lease retirement refuses new snapshots but never revokes arcs
/// already taken out.
#[tokio::test]
async fn v27_lease_retirement_refuses_snapshots_but_keeps_taken_arcs() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let applies = Arc::new(AtomicUsize::new(0));
    let snapshots: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let cleanups: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let lease_slot = slot::<ServiceLease<Db>>();
    let (db_plugin, _ctx) = provider(
        "db",
        db_key(),
        Arc::new(Db {
            url: "v27".to_owned(),
        }),
    );
    let consumer_plugin = consumer(
        "lease-holder",
        db_key(),
        Arc::clone(&applies),
        Arc::clone(&snapshots),
        Arc::clone(&cleanups),
        Arc::clone(&lease_slot),
    );

    let provider_handle = root.load(&db_plugin, Cfg).await.expect("load");
    let consumer_handle = root.load(&consumer_plugin, Cfg).await.expect("load");
    consumer_handle
        .fiber
        .wait_active(Instant::now() + Duration::from_secs(15))
        .await
        .expect("consumer active");

    let lease = lease_slot.lock().unwrap().clone().expect("lease captured");
    let taken = lease.snapshot().expect("live snapshot");

    // Retire the binding by disposing the provider fiber.
    let dispose = provider_handle.fiber.dispose().await.expect("dispose op");
    dispose.wait().await.expect("provider disposed");

    // New acquisitions are refused with an explicit namespace error …
    let missing = root.lookup_dynamic(KEY_DB).await.expect_err("retired");
    assert!(matches!(missing, Error::ServiceMissing { .. }));
    // … the lease refuses new snapshots …
    match lease.snapshot() {
        Err(Error::ServiceRetired { binding }) => {
            assert_eq!(binding, lease.binding_id());
        }
        other => panic!("expected ServiceRetired, got {other:?}"),
    }
    assert_eq!(lease.revision(), None);
    // … and arcs already taken out keep their value.
    assert_eq!(taken.url, "v27");

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

#[tokio::test]
async fn v19_dependency_chain_converges_through_availability_flips() {
    v19_chain_scenario().await;
}

#[tokio::test]
async fn v20_unbind_reprovide_reloads_and_rejects_stale_contexts() {
    v20_epoch_reload_scenario().await;
}

#[tokio::test]
async fn v26_availability_flip_under_inflight_start_and_dependency_cycles() {
    v26_flip_scenario().await;
}

// ---------------------------------------------------------------------------
// Scheduler-flavor coverage (docs/07-validation.md §8): the flagship
// dependency-epoch scenarios re-run on the multi-thread runtime, mirroring
// the convention of `concurrency.rs`.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v19_chain_convergence_multi_thread() {
    v19_chain_scenario().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v20_epoch_reload_multi_thread() {
    v20_epoch_reload_scenario().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v26_flip_under_starting_multi_thread() {
    v26_flip_scenario().await;
}

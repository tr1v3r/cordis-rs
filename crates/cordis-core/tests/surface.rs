//! t9 integration: typed-surface coverage (docs/07-validation.md §8).
//!
//! The behavioral suites drive the kernel through lifetimes, effects,
//! services and events; a slice of the typed public surface — `Debug`
//! renderings, registration accessors, listener-config builders,
//! scope-id renderings, and the lease-retirement read paths — only runs
//! when something formats or calls it directly. These tests exercise
//! that slice so the coverage baseline reflects reachable code, and pin
//! the promise that none of these outputs ever contain payloads.
//!
//! No timers: every step awaits an operation outcome or formats a value.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use cordis_core::{
    App, Cleanup, Context, Error, EventKey, ListenerConfig, QueryKey, Registration, ScopeId,
    ServiceKey, ServiceLease, ShutdownOptions, WaterfallKey, define,
};

struct Ping {
    #[expect(
        dead_code,
        reason = "payload shape; the tests format keys, not payloads"
    )]
    seq: u32,
}

struct Cfg;

struct Db {
    value: u64,
}

/// `Debug` of every typed key and handle names identities, never
/// configuration or payload content.
#[test]
fn debug_renderings_name_identities_not_payloads() {
    let event_key = EventKey::<Ping>::new("evt-secret-name");
    let query_key = QueryKey::<Ping, u32>::new("query-secret-name");
    let waterfall_key = WaterfallKey::<Ping, u32>::new("fall-secret-name");
    for text in [
        format!("{event_key:?}"),
        format!("{query_key:?}"),
        format!("{waterfall_key:?}"),
    ] {
        assert!(text.contains("Key"), "{text}");
    }
    assert!(format!("{event_key:?}").contains("evt-secret-name"));
    assert!(format!("{query_key:?}").contains("query-secret-name"));
    assert!(format!("{waterfall_key:?}").contains("fall-secret-name"));

    // Scope ids render deterministically for diagnostics and namespace
    // hashes.
    assert_eq!(ScopeId::Default.as_str(), "default");
    assert_eq!(ScopeId::Unique(7).to_string(), "unique(7)");
    assert_eq!(
        ScopeId::Shared("team".to_owned()).to_string(),
        "shared(\"team\")"
    );
    assert_eq!(format!("{:?}", ScopeId::Default), "default");

    // Listener-config builders compose.
    let config = ListenerConfig::default()
        .with_global(true)
        .with_once(true)
        .with_prepend(true);
    assert!(config.global);
    assert!(config.once);
    assert!(config.prepend);

    // Cleanup renders opaquely.
    let cleanup = Cleanup::noop();
    assert_eq!(format!("{cleanup:?}"), "Cleanup(..)");
}

/// Registration accessors and `Debug` surface identity fields. Effects
/// are owned by generation scopes (the root view refuses with
/// `InvalidOwner`), so the registrations come out of a fiber.
#[tokio::test]
async fn registration_accessors_expose_identity() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    // The root view is not an owner: direct registration is refused.
    let root_reg = root.on_dispose("root-cleanup", || async { Ok(()) }).await;
    assert!(
        matches!(root_reg, Err(Error::InvalidOwner)),
        "the root view cannot own effects: {root_reg:?}"
    );

    let slots: Arc<Mutex<Vec<Registration>>> = Arc::new(Mutex::new(Vec::new()));
    let plugin = {
        let slots = Arc::clone(&slots);
        define("surface-owner", move |ctx: Context, _cfg: Arc<Cfg>| {
            let slots = Arc::clone(&slots);
            async move {
                let cleanup_reg = ctx
                    .on_dispose("surface-cleanup", || async { Ok(()) })
                    .await?;
                let task_reg = ctx
                    .spawn_prepare("surface-task", || async { Ok(()) })
                    .await?;
                let mut slots = slots.lock().expect("slots");
                slots.push(cleanup_reg);
                slots.push(task_reg);
                Ok(())
            }
        })
    };

    let receipt = root.load(&plugin, Cfg).await.expect("load");
    match &*receipt.operation.wait().await.expect("settles") {
        cordis_core::OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    }

    let registrations = slots.lock().expect("slots").clone();
    assert_eq!(registrations.len(), 2);
    let [cleanup_reg, task_reg] = [&registrations[0], &registrations[1]];

    let cleanup_effect = cleanup_reg.effect_id();
    assert!(format!("{cleanup_effect:?}").contains("EffectId"));
    assert_eq!(cleanup_reg.task_id(), None, "on_dispose is not a task");
    let text = format!("{cleanup_reg:?}");
    assert!(text.contains("Registration"), "{text}");
    assert!(text.contains("effect"), "{text}");

    assert!(
        task_reg.task_id().is_some(),
        "spawn registrations carry a task id"
    );
    assert_ne!(
        cleanup_reg.effect_id(),
        task_reg.effect_id(),
        "distinct entries have distinct ids"
    );
    let _ = format!("{task_reg:?}");

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

/// Leases keep working as identity handles after their binding retired:
/// reads refuse with `ServiceRetired` instead of silently serving stale
/// values through the live-registry path (V27 read side).
#[tokio::test]
async fn lease_reads_refuse_after_binding_retirement() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    let db_key = ServiceKey::<Db>::new("surface-db");
    let lease_slot: Arc<Mutex<Option<ServiceLease<Db>>>> = Arc::new(Mutex::new(None));

    let provider = {
        let key = db_key.clone();
        define("surface-provider", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                ctx.provide(key, Arc::new(Db { value: 1 })).await?;
                Ok(())
            }
        })
    };
    let consumer = {
        let key = db_key.clone();
        let slot = Arc::clone(&lease_slot);
        define("surface-consumer", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            let slot = Arc::clone(&slot);
            async move {
                let lease = ctx.get(key).await?;
                *slot.lock().expect("lease slot") = Some(lease);
                Ok(())
            }
        })
        .require(db_key.clone())
    };

    let provider_receipt = root.load(&provider, Cfg).await.expect("provider load");
    match &*provider_receipt.operation.wait().await.expect("activates") {
        cordis_core::OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    }
    let consumer_receipt = root.load(&consumer, Cfg).await.expect("consumer load");
    match &*consumer_receipt.operation.wait().await.expect("activates") {
        cordis_core::OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    }

    // The lease is live: identity reads and value reads work.
    let lease = lease_slot
        .lock()
        .expect("lease slot")
        .clone()
        .expect("lease captured");
    let binding = lease.binding_id();
    assert_eq!(lease.binding_id(), binding);
    assert_eq!(lease.snapshot().expect("live snapshot").value, 1);
    let lease_debug = format!("{lease:?}");
    assert!(lease_debug.contains("ServiceLease"), "{lease_debug}");

    // Retire the binding: the provider's disposal retires the cell, and
    // the held lease refuses further snapshots.
    let dispose = provider_receipt.fiber.dispose().await.expect("dispose");
    match &*dispose.wait().await.expect("dispose settles") {
        cordis_core::OperationOutcome::Disposed { cleanup } => assert!(cleanup.is_clean()),
        other => panic!("expected Disposed, got {other:?}"),
    }

    match lease.snapshot() {
        Err(Error::ServiceRetired { .. }) => {}
        Err(other) => panic!("expected ServiceRetired, got {other:?}"),
        Ok(_) => panic!("snapshot must refuse after retirement"),
    }
    assert_eq!(
        lease.binding_id(),
        binding,
        "identity reads stay valid after retirement"
    );
    // The consumer lost its dependency and parked Pending, which is the
    // documented behavior; the shutdown deadline ends the scenario.
    let _ = consumer_receipt;
    let report = app
        .shutdown(ShutdownOptions {
            timeout: Some(Duration::from_secs(15)),
        })
        .await
        .expect("shutdown");
    assert_eq!(report.quarantined, 0, "{report:?}");
}

/// Mode-conflict diagnostics name the mode of the registration that won
/// the name: every mode word surfaces through the conflict error of an
/// attacker registering a foreign mode on a taken name.
#[tokio::test]
async fn mode_conflict_diagnostics_name_every_mode() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    async fn activate(root: &Context, plugin: &cordis_core::Plugin<Cfg>) {
        let receipt = root.load(plugin, Cfg).await.expect("holder load");
        match &*receipt.operation.wait().await.expect("holder settles") {
            cordis_core::OperationOutcome::Active { .. } => {}
            other => panic!("holder must activate, got {other:?}"),
        }
    }

    // Holders register one name per mode.
    let serial_holder = {
        let key = QueryKey::<Ping, u32>::new("conflict-serial");
        define("serial-holder", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                ctx.on_serial(
                    key,
                    |_req: Arc<Ping>| async { Ok(std::ops::ControlFlow::Continue(())) },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let parallel_holder = {
        let key = QueryKey::<Ping, u32>::new("conflict-parallel");
        define("parallel-holder", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                ctx.on_parallel(
                    key,
                    |_req: Arc<Ping>| async { Ok(0) },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let waterfall_holder = {
        let key = WaterfallKey::<Ping, u32>::new("conflict-waterfall");
        define("waterfall-holder", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                ctx.on_waterfall(
                    key,
                    |event: Ping, next: cordis_core::Next<Ping, u32>| async move {
                        next.run(event)
                            .await
                            .map_err(cordis_core::PluginError::from)
                    },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let emit_holder = {
        let key = EventKey::<Ping>::new("conflict-emit");
        define("emit-holder", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                ctx.on_emit(key, |_evt: &Ping| Ok(()), ListenerConfig::default())
                    .await?;
                Ok(())
            }
        })
    };

    activate(&root, &serial_holder).await;
    activate(&root, &parallel_holder).await;
    activate(&root, &waterfall_holder).await;
    activate(&root, &emit_holder).await;

    // Attackers: a foreign mode on each taken name. The failing
    // activation's error chain names the mode that won the name.
    let emit_attacker = |name: &'static str| {
        let key = EventKey::<Ping>::new(name);
        define(name, move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                ctx.on_emit(key, |_evt: &Ping| Ok(()), ListenerConfig::default())
                    .await?;
                Ok(())
            }
        })
    };
    let bail_on_emit = {
        let key = QueryKey::<Ping, u32>::new("conflict-emit");
        define("bail-attacker", move |ctx: Context, _cfg: Arc<Cfg>| {
            let key = key.clone();
            async move {
                ctx.on_bail(
                    key,
                    |_evt: &Ping| Ok(std::ops::ControlFlow::Continue(())),
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };

    for (plugin, expected_word) in [
        (emit_attacker("conflict-serial"), "serial"),
        (emit_attacker("conflict-parallel"), "parallel"),
        (emit_attacker("conflict-waterfall"), "waterfall"),
        (bail_on_emit, "emit"),
    ] {
        let receipt = root.load(&plugin, Cfg).await.expect("attacker load");
        let chain = match &*receipt.operation.wait().await.expect("settles") {
            cordis_core::OperationOutcome::Failed { error } => {
                let mut chain = vec![error.to_string()];
                let mut source = std::error::Error::source(error);
                while let Some(next) = source {
                    chain.push(next.to_string());
                    source = next.source();
                }
                chain.join(" | ")
            }
            other => panic!("expected Failed for {expected_word}, got {other:?}"),
        };
        assert!(
            chain.contains(expected_word),
            "conflict diagnostics must name the {expected_word} mode: {chain}"
        );
    }

    app.shutdown(ShutdownOptions::default())
        .await
        .expect("shutdown");
}

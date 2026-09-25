//! Surface coverage for the public error taxonomy: every `Error` variant
//! renders a stable English message, source chaining is only claimed by
//! the four wrapping variants, and the `PluginError`/`CleanupError`
//! wrappers convert as documented. Variants carrying runtime ids are
//! exercised through real lifecycle triggers (their messages name the id).

use std::error::Error as StdError;
use std::io;
use std::sync::Arc;

use cordis_core::{App, CleanupError, Error, ListenerConfig, PluginError, QueryKey, define};

struct Cfg {
    #[expect(dead_code, reason = "shape only; the plugin never reads it")]
    value: u64,
}

fn cfg() -> Cfg {
    Cfg { value: 1 }
}

#[test]
fn every_constructible_variant_renders_a_specific_message() {
    let cases: Vec<(Error, &str)> = vec![
        (Error::HostClosed, "shut down or dropped"),
        (Error::InactiveScope, "no longer accepts registrations"),
        (
            Error::InvalidConfig {
                reason: "path is required".to_owned(),
            },
            "invalid configuration: path is required",
        ),
        (
            Error::UndeclaredDependency {
                service: "db".to_owned(),
            },
            "not declared as a dependency",
        ),
        (
            Error::ServiceMissing {
                service: "db".to_owned(),
                namespace: "unique(9)".to_owned(),
            },
            "not currently provided in namespace unique(9)",
        ),
        (
            Error::ServiceTypeMismatch {
                service: "db".to_owned(),
                namespace: "default".to_owned(),
            },
            "bound with a different type",
        ),
        (
            Error::ServiceExists {
                service: "db".to_owned(),
                namespace: "shared".to_owned(),
            },
            "already has a provider",
        ),
        (
            Error::InvalidDependency {
                service: "db".to_owned(),
                reason: "conflicting types".to_owned(),
            },
            "invalid dependency declaration",
        ),
        (
            Error::EventUnknown {
                event: "tick".to_owned(),
            },
            "no listener is registered",
        ),
        (
            Error::EventConflict {
                event: "tick".to_owned(),
                reason: "mode mismatch".to_owned(),
            },
            "identity conflict",
        ),
        (Error::ReentrantDispatchLimit, "maximum reentrancy depth"),
        (
            Error::HandlerFailed {
                listener: None,
                source: PluginError::from("anon boom"),
            },
            "a dispatched handler failed",
        ),
        (Error::InvalidOwner, "not owned by this scope"),
        (Error::WouldDeadlock, "would deadlock"),
        (
            Error::ActivationFailed {
                source: PluginError::from("apply boom"),
            },
            "plugin activation failed",
        ),
        (
            Error::TaskFailed {
                source: Box::new(io::Error::other("task boom")),
            },
            "a supervised task failed",
        ),
        (
            Error::CleanupFailed {
                source: CleanupError::from("cleanup boom"),
            },
            "a cleanup step failed",
        ),
        (
            Error::Quarantined {
                reason: "uncooperative task".to_owned(),
            },
            "quarantined: uncooperative task",
        ),
        (
            Error::CapacityExceeded {
                reason: "mailbox full".to_owned(),
            },
            "an admission limit was reached: mailbox full",
        ),
        (
            Error::WorkerPanicked {
                context: "factory".to_owned(),
                message: "boom".to_owned(),
            },
            "panicked in factory: boom",
        ),
        (
            Error::DeadlineExceeded {
                reason: "shutdown drain".to_owned(),
            },
            "a deadline passed before shutdown drain",
        ),
    ];
    for (error, needle) in &cases {
        let text = error.to_string();
        assert!(text.contains(needle), "missing {needle:?} in {text:?}");
        // Debug never fails and stays cheap (ids, not payloads).
        let _ = format!("{error:?}");
    }
}

#[tokio::test]
async fn stale_generation_display_comes_from_a_disposed_fiber() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let plugin: cordis_core::Plugin<Cfg> =
        define("stale-source", |_ctx, _cfg: Arc<Cfg>| async { Ok(()) });
    let receipt = root.load(&plugin, cfg()).await.expect("load");
    receipt.operation.wait().await.expect("active");

    let dispose = receipt.fiber.dispose().await.expect("dispose");
    let _ = dispose.wait().await.expect("dispose resolves");

    let err = receipt
        .fiber
        .update(cfg())
        .await
        .expect_err("terminal fiber refuses updates");
    match &err {
        Error::StaleGeneration { .. } => {
            assert!(
                err.to_string()
                    .contains("no longer exists in its app (fiber FiberId("),
                "{}",
                err
            );
        }
        other => panic!("expected StaleGeneration, got {other:?}"),
    }
}

#[tokio::test]
async fn handler_failure_display_names_the_listener() {
    struct Ping {
        #[expect(dead_code, reason = "payload shape only")]
        seq: u32,
    }

    let app = App::builder().build().expect("app builds");
    let root = app.context();
    let key = QueryKey::<Ping, u32>::new("err-surface");
    let listener_key = key.clone();
    let plugin = define("failing-voter", move |ctx, _cfg: Arc<Cfg>| {
        let key = listener_key.clone();
        async move {
            ctx.on_bail(
                key,
                |_: &Ping| Err(PluginError::from("vote failed")),
                ListenerConfig::default(),
            )
            .await?;
            Ok(())
        }
    });
    let receipt = root.load(&plugin, cfg()).await.expect("load");
    receipt.operation.wait().await.expect("active");

    let err = root
        .bail(key, Ping { seq: 1 })
        .await
        .expect_err("failing handler surfaces");
    match &err {
        Error::HandlerFailed {
            listener: Some(_),
            source,
        } => {
            assert_eq!(source.to_string(), "vote failed");
            assert!(
                err.to_string()
                    .contains("a dispatched handler failed (listener EffectId("),
                "{err}"
            );
        }
        other => panic!("expected HandlerFailed, got {other:?}"),
    }
}

#[test]
fn only_wrapping_variants_chain_a_source() {
    let wrapped = [
        Error::ActivationFailed {
            source: PluginError::from("a"),
        },
        Error::TaskFailed {
            source: Box::new(io::Error::other("t")),
        },
        Error::CleanupFailed {
            source: CleanupError::from("c"),
        },
        Error::HandlerFailed {
            listener: None,
            source: PluginError::from("h"),
        },
    ];
    for error in &wrapped {
        assert!(StdError::source(error).is_some(), "{error} chains");
    }

    let bare = [
        Error::HostClosed,
        Error::InactiveScope,
        Error::InvalidOwner,
        Error::WouldDeadlock,
        Error::ReentrantDispatchLimit,
        Error::Quarantined {
            reason: "r".to_owned(),
        },
    ];
    for error in &bare {
        assert!(StdError::source(error).is_none(), "{error} is bare");
    }
}

#[test]
fn plugin_error_conversions_preserve_the_cause() {
    let from_str: PluginError = "boom".into();
    assert_eq!(from_str.to_string(), "boom");
    let from_string: PluginError = String::from("bang").into();
    assert_eq!(from_string.to_string(), "bang");
    let from_error: PluginError = Error::WouldDeadlock.into();
    assert_eq!(from_error.to_string(), Error::WouldDeadlock.to_string());
    // The wrapped framework error is the leaf of the chain: `source`
    // stays empty instead of pointing at itself.
    assert!(StdError::source(&from_error).is_none());
    let constructed = PluginError::new(io::Error::other("io"));
    assert_eq!(constructed.to_string(), "io");
    assert!(StdError::source(&constructed).is_none());
}

#[test]
fn cleanup_error_conversions_preserve_the_cause() {
    let from_str: CleanupError = "slow".into();
    assert_eq!(from_str.to_string(), "slow");
    let from_string: CleanupError = String::from("stuck").into();
    assert_eq!(from_string.to_string(), "stuck");
    let constructed = CleanupError::new(io::Error::other("io"));
    assert_eq!(constructed.to_string(), "io");
    assert!(StdError::source(&constructed).is_none());
}

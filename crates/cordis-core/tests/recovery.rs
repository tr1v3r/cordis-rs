//! t9 integration: recovery paths (docs/07-validation.md §5).
//!
//! - One composition injects panics at every supervised boundary —
//!   factory, emit handler, parallel handler, cleanup — on a single app
//!   and proves isolation: each failure lands on the failing item only,
//!   the host keeps serving, and a failed activation recovers through an
//!   explicit restart.
//! - A subprocess scenario runs a supervised task that blocks a native
//!   thread forever (the "uncooperative native code" slice of V38): the
//!   shutdown deadline must report quarantine honestly, never fake
//!   completion, and the child process still exits on its own terms.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use cordis_core::{
    App, EventKey, ListenerConfig, OperationOutcome, QueryKey, ShutdownOptions, define,
};

struct Cfg {
    #[expect(dead_code, reason = "shape parity across plugins; not read")]
    value: u64,
}

struct Ping {
    seq: u64,
}

fn cfg() -> Cfg {
    Cfg { value: 0 }
}

/// Renders the full error chain (message + sources) for assertions.
fn error_chain(error: &cordis_core::Error) -> Vec<String> {
    let mut chain = vec![error.to_string()];
    let mut source = std::error::Error::source(error);
    while let Some(next) = source {
        chain.push(next.to_string());
        source = next.source();
    }
    chain
}

async fn panics_across_boundaries_isolate_and_recover_scenario() {
    let app = App::builder().build().expect("app builds");
    let root = app.context();

    // ---- boundary 1: the activation factory panics on first run.
    let flaky_fired = Arc::new(AtomicBool::new(false));
    let flaky = {
        let fired = Arc::clone(&flaky_fired);
        define("flaky", move |_ctx, _cfg: Arc<Cfg>| {
            let fired = Arc::clone(&fired);
            async move {
                if !fired.swap(true, Ordering::SeqCst) {
                    panic!("flaky factory boom");
                }
                Ok(())
            }
        })
    };
    let flaky_receipt = root.load(&flaky, cfg()).await.expect("load admitted");
    match &*flaky_receipt.operation.wait().await.expect("settles") {
        OperationOutcome::Failed { error } => {
            let chain = error_chain(error);
            assert!(
                chain
                    .iter()
                    .any(|entry| entry.contains("flaky factory boom")),
                "the panic is rendered into the failure: {chain:?}"
            );
        }
        other => panic!("expected Failed, got {other:?}"),
    }
    let status = flaky_receipt.fiber.status().await.expect("status");
    assert_eq!(status.state, cordis_core::FiberState::Failed);

    // ---- boundary 2/3: emit and parallel handler panics are isolated
    // failures; healthy siblings in the same dispatch still run.
    let healthy_seen = Arc::new(AtomicUsize::new(0));
    let listener_host = {
        let seen = Arc::clone(&healthy_seen);
        let emit_key = EventKey::<Ping>::new("panic-emit");
        let fan_key = QueryKey::<Ping, u32>::new("panic-fan");
        define("listener-host", move |ctx, _cfg: Arc<Cfg>| {
            let seen = Arc::clone(&seen);
            let emit_key = emit_key.clone();
            let fan_key = fan_key.clone();
            async move {
                let recorder_seen = Arc::clone(&seen);
                ctx.on_emit(
                    emit_key.clone(),
                    move |_ping: &Ping| {
                        recorder_seen.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    },
                    ListenerConfig::default(),
                )
                .await?;
                ctx.on_emit(
                    emit_key,
                    move |_ping: &Ping| panic!("emit handler boom"),
                    ListenerConfig::default(),
                )
                .await?;
                let fan_seen = Arc::clone(&seen);
                ctx.on_parallel(
                    fan_key.clone(),
                    move |ping: Arc<Ping>| {
                        let fan_seen = Arc::clone(&fan_seen);
                        async move {
                            fan_seen.fetch_add(1, Ordering::SeqCst);
                            Ok(u32::try_from(ping.seq).expect("seq fits u32"))
                        }
                    },
                    ListenerConfig::default(),
                )
                .await?;
                ctx.on_parallel(
                    fan_key,
                    move |_ping: Arc<Ping>| async { panic!("parallel handler boom") },
                    ListenerConfig::default(),
                )
                .await?;
                Ok(())
            }
        })
    };
    let listener_receipt = root
        .load(&listener_host, cfg())
        .await
        .expect("listener host admitted");
    match &*listener_receipt.operation.wait().await.expect("settles") {
        OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    }

    // The emit dispatch delivers to the healthy listener, and the
    // panicking listener surfaces as exactly one collected failure.
    let report = root
        .emit(EventKey::<Ping>::new("panic-emit"), Ping { seq: 1 })
        .await
        .expect("emit dispatch resolves");
    assert_eq!(report.delivered, 1, "{report:?}");
    assert_eq!(report.failures.len(), 1, "{report:?}");
    assert!(
        report.failures[0]
            .error
            .to_string()
            .contains("emit handler boom"),
        "{report:?}"
    );

    // The parallel dispatch aggregates both handlers in registration
    // order: the healthy result survives the sibling panic.
    let fan = root
        .parallel(QueryKey::<Ping, u32>::new("panic-fan"), Ping { seq: 7 })
        .await
        .expect("parallel dispatch resolves");
    assert_eq!(fan.results.len(), 2, "{fan:?}");
    assert_eq!(fan.results[0].as_ref().expect("healthy handler"), &7);
    let failure = fan.results[1].as_ref().expect_err("panicking handler");
    assert!(
        failure.to_string().contains("parallel handler boom"),
        "{failure}"
    );
    assert_eq!(healthy_seen.load(Ordering::SeqCst), 2);

    // ---- boundary 4: a cleanup panic quarantines that fiber only.
    let dirty = define("dirty", |ctx, _cfg: Arc<Cfg>| async move {
        ctx.on_dispose("dirty-cleanup", || async {
            panic!("cleanup boom");
        })
        .await?;
        Ok(())
    });
    let dirty_receipt = root.load(&dirty, cfg()).await.expect("dirty admitted");
    match &*dirty_receipt.operation.wait().await.expect("settles") {
        OperationOutcome::Active { .. } => {}
        other => panic!("expected Active, got {other:?}"),
    }
    let dirty_dispose = dirty_receipt
        .fiber
        .dispose()
        .await
        .expect("dispose admitted");
    match &*dirty_dispose.wait().await.expect("dispose settles") {
        OperationOutcome::Quarantined { cleanup } => {
            assert!(
                cleanup
                    .failures
                    .iter()
                    .any(|failure| failure.error.to_string().contains("cleanup boom")),
                "the panic is rendered into the quarantine: {cleanup:?}"
            );
            assert!(cleanup.quarantined >= 1, "{cleanup:?}");
        }
        other => panic!("a panicking cleanup must quarantine, got {other:?}"),
    }

    // ---- the host is still fully alive: the failed fiber restarts onto
    // a healthy generation, and a fresh load works after every failure.
    let restart = flaky_receipt
        .fiber
        .restart()
        .await
        .expect("restart admitted");
    match &*restart.wait().await.expect("restart settles") {
        OperationOutcome::Active { .. } => {}
        other => panic!("expected Active after restart, got {other:?}"),
    }
    let healthy_dispose = flaky_receipt.fiber.dispose().await.expect("dispose");
    match &*healthy_dispose.wait().await.expect("settles") {
        OperationOutcome::Disposed { cleanup } => assert!(cleanup.is_clean()),
        other => panic!("expected Disposed, got {other:?}"),
    }
    let stats = app
        .stats()
        .await
        .expect("stats respond after every failure");
    assert_eq!(stats.workers_live, 0, "{stats:?}");

    let report = app
        .shutdown(ShutdownOptions {
            timeout: Some(Duration::from_secs(15)),
        })
        .await
        .expect("shutdown completes");
    // Only the listener host remains to dispose: the dirty fiber already
    // reported its quarantine through its own dispose operation above,
    // and the flaky fiber was disposed explicitly — shutdown never
    // double-counts either.
    assert_eq!(report.fibers_disposed, 1, "{report:?}");
    assert_eq!(report.quarantined, 0, "{report:?}");
    assert!(app.is_closed());
}

#[tokio::test]
async fn panics_across_boundaries_isolate_and_recover_current_thread() {
    panics_across_boundaries_isolate_and_recover_scenario().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn panics_across_boundaries_isolate_and_recover_multi_thread() {
    panics_across_boundaries_isolate_and_recover_scenario().await;
}

// ---------------------------------------------------------------------------
// Subprocess: an uncooperative supervised task blocks a native thread
// forever; the shutdown deadline quarantines honestly and the child still
// exits (docs/07 V38, "native不yield" slice; complement of
// blocking_drop.rs, which covers a blocking user `Drop`).

const CHILD_ENV: &str = "CORDIS_TASK_CHILD";
const MARKER: &str = "cordis-task-child-ok";

#[test]
fn uncooperative_task_native_block_runs_in_subprocess() {
    if std::env::var(CHILD_ENV).is_ok() {
        child_scenario();
        return;
    }
    let exe = std::env::current_exe().expect("test binary path");
    let mut child = Command::new(exe)
        .env(CHILD_ENV, "1")
        .args([
            "uncooperative_task_native_block_runs_in_subprocess",
            "--exact",
            "--nocapture",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn child");

    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        match child.try_wait().expect("child is alive") {
            Some(status) => break status,
            None => {
                assert!(Instant::now() < deadline, "child did not exit in time");
                std::thread::sleep(Duration::from_millis(25));
            }
        }
    };
    assert!(status.success(), "child scenario failed: {status}");

    let output = child.wait_with_output().expect("collect output");
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains(MARKER),
        "child did not report the marker; output: {text}"
    );
}

fn child_scenario() {
    let started = Arc::new(AtomicBool::new(false));

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("child runtime");
    rt.block_on(async {
        let task_started = Arc::clone(&started);
        let plugin = define("parked", move |ctx, _cfg: Arc<Cfg>| {
            let started = Arc::clone(&task_started);
            async move {
                ctx.spawn_prepare("parked-forever", move || {
                    started.store(true, Ordering::SeqCst);
                    // Uncooperative native code: park the worker thread
                    // forever — no yielding, no cancellation observed.
                    let (_tx, rx) = std::sync::mpsc::channel::<()>();
                    let _ = rx.recv();
                    async { Ok(()) }
                })
                .await?;
                Ok(())
            }
        });

        let app = App::builder().build().expect("app builds");
        let receipt = app
            .context()
            .load(&plugin, cfg())
            .await
            .expect("load admitted");
        match &*receipt.operation.wait().await.expect("activates") {
            OperationOutcome::Active { .. } => {}
            other => panic!("expected Active, got {other:?}"),
        }

        // Wait until the task truly parked (it flips the flag right
        // before blocking the thread).
        let parked = tokio::time::timeout(Duration::from_secs(15), async {
            while !started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await;
        parked.expect("task parked before the deadline fires");

        // The deadline cannot join the parked thread: the report must
        // quarantine it instead of faking a disposal.
        let report = app
            .shutdown(ShutdownOptions {
                timeout: Some(Duration::from_millis(300)),
            })
            .await
            .expect("shutdown returns despite the blocked task");
        assert!(
            report.quarantined >= 1,
            "an unjoinable task must be quarantined: {report:?}"
        );
        assert_eq!(
            report.fibers_disposed, 0,
            "quarantine must not be reported as disposal: {report:?}"
        );

        println!("{MARKER}");
    });

    // The parked worker thread can never finish: exit on our own terms
    // instead of hanging in runtime teardown.
    std::process::exit(0);
}

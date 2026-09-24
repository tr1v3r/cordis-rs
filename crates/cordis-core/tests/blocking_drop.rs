//! V38 (subprocess slice): a user `Drop` that blocks a native thread must
//! not block the coordinator, and retirement must not fake completion.
//!
//! The blocking scenario runs in a child process (`std::process`): the
//! parent test spawns this very binary with `CORDIS_BLOCKING_CHILD=1`,
//! the child runs the scenario and prints a marker line; the parent
//! asserts exit status and marker, bounded by a watchdog.

use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use cordis_core::{App, OperationOutcome, Plugin, ShutdownOptions, define};

const CHILD_ENV: &str = "CORDIS_BLOCKING_CHILD";
const MARKER: &str = "cordis-blocking-child-ok";

#[test]
fn v38_blocking_drop_runs_in_subprocess() {
    if std::env::var(CHILD_ENV).is_ok() {
        child_scenario();
        return;
    }
    let exe = std::env::current_exe().expect("test binary path");
    let mut child = Command::new(exe)
        .env(CHILD_ENV, "1")
        .args([
            "v38_blocking_drop_runs_in_subprocess",
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

/// The child: a configuration whose `Drop` blocks a native thread for
/// longer than the whole scenario. The coordinator must stay responsive,
/// the shutdown deadline must hold, and retirement must count the value
/// as *pending*, not completed.
fn child_scenario() {
    struct BlockingDrop;
    impl Drop for BlockingDrop {
        fn drop(&mut self) {
            // Native blocking: no yielding, no cancellation.
            std::thread::sleep(Duration::from_millis(3_000));
        }
    }

    // Current-thread runtime: the lane drops user values on Tokio's
    // dedicated blocking pool, so even a native-blocking Drop leaves the
    // coordinator schedulable — the strongest form of the isolation.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("child runtime");
    rt.block_on(async {
        let started = Instant::now();
        let app = App::builder().build().expect("app builds");
        let plugin: Plugin<BlockingDrop> =
            define("blocking-drop", |_ctx, _cfg: Arc<BlockingDrop>| async {
                Ok(())
            });

        let receipt = app
            .context()
            .load(&plugin, BlockingDrop)
            .await
            .expect("load");
        receipt.operation.wait().await.expect("active");

        // Dispose: the config retires onto the lane, whose Drop blocks a
        // thread. The teardown itself does not wait for the lane.
        let dispose = receipt.fiber.dispose().await.expect("dispose");
        assert!(matches!(
            &*dispose.wait().await.expect("dispose resolves"),
            OperationOutcome::Disposed { .. }
        ));

        let t2 = Instant::now();
        eprintln!(
            "CHILD submitting stats at {} ms",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        let stats = app.stats().await.expect("stats answer while lane blocked");
        eprintln!(
            "CHILD stats returned at {} ms (delta {:?})",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            t2.elapsed()
        );
        assert!(
            stats.retirement_pending >= 1,
            "retirement must not fake completion: {stats:?}"
        );

        let report = app
            .shutdown(ShutdownOptions {
                timeout: Some(Duration::from_millis(300)),
            })
            .await
            .expect("shutdown completes despite the blocked lane");
        assert_eq!(report.quarantined, 0);

        // The whole scenario must finish well under the 3s block: the
        // coordinator never ran user Drop code on its actor.
        assert!(
            started.elapsed() < Duration::from_millis(2_500),
            "actor waited for a blocking user Drop: {:?}",
            started.elapsed()
        );
        // Post-shutdown reads refuse honestly.
        assert!(matches!(
            receipt.fiber.status().await,
            Err(cordis_core::Error::HostClosed)
        ));
        println!("{MARKER}");
    });
}

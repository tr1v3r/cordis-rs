//! Compile-fail coverage (V31/V40, docs/04 §3.1, docs/07 §5): patterns
//! that must be rejected by the type system instead of panicking at
//! runtime —
//!
//! - calling the move-only [`Next`] twice (use-after-move),
//! - a `!Send + !Sync` (`Rc`) configuration crossing the plugin boundary,
//! - an apply future that is not `Send`.
//!
//! Instead of pulling a compile-fail harness dependency, each fixture is
//! compiled with `rustc` directly against this crate's rlib (located
//! next to the running test binary), and the test asserts the expected
//! error code. This keeps the suite hermetic: no network, no extra
//! dependencies, and it runs under plain `cargo test`.

use std::path::PathBuf;
use std::process::Command;

/// Locates the directory holding the test binary (target/debug/deps),
/// which also holds the crate rlib and its dependency rlibs.
fn deps_dir() -> PathBuf {
    let exe = std::env::current_exe().expect("test binary path");
    exe.parent()
        .expect("test binary lives in a directory")
        .to_path_buf()
}

/// All `libcordis_core-*.rlib` candidates next to the test binary, newest
/// first. Several variants coexist (unit-test, doctest and dependency
/// profiles rebuild the crate under different metadata hashes), and a
/// variant whose sibling dependencies were since collected can no longer
/// load — callers must skip those instead of asserting on them.
fn core_rlib_candidates() -> Vec<PathBuf> {
    let dir = deps_dir();
    let mut candidates: Vec<(std::time::SystemTime, PathBuf)> = std::fs::read_dir(&dir)
        .expect("deps dir readable")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("libcordis_core-") && name.ends_with(".rlib"))
        })
        .filter_map(|path| {
            let modified = std::fs::metadata(&path)
                .and_then(|meta| meta.modified())
                .ok()?;
            Some((modified, path))
        })
        .collect();
    assert!(!candidates.is_empty(), "no cordis-core rlib in {dir:?}");
    candidates.sort_by_key(|(modified, _)| std::cmp::Reverse(*modified)); // newest first
    candidates.into_iter().map(|(_, path)| path).collect()
}

/// Environment errors that say "this rlib candidate cannot be loaded or
/// predates the current API" — never a verdict about the fixture itself.
fn is_environment_error(stderr: &str) -> bool {
    // E0463: crate not found (sibling rlibs collected).
    // E0460/E0514: metadata version mismatch between artifacts.
    // E0432/E0433: unresolved import — a stale crate predating the API
    // the fixture exercises.
    stderr.contains("error[E0463]")
        || stderr.contains("error[E0460]")
        || stderr.contains("error[E0514]")
        || stderr.contains("error[E0432]")
        || stderr.contains("error[E0433]")
}

/// Compiles `fixture` (a complete crate source) against cordis-core and
/// returns the collected stderr; the compilation must fail for a
/// type-system reason, not for an unloadable-environment reason.
fn compile_fail(fixture: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let run = NEXT.fetch_add(1, Ordering::Relaxed);
    // Unique scratch dir per invocation: the fixtures run in parallel.
    let out_dir = deps_dir()
        .join("compilefail")
        .join(format!("{}_{}", fixture_id(fixture), run));
    std::fs::create_dir_all(&out_dir).expect("create scratch dir");
    let source = out_dir.join("fixture.rs");
    std::fs::write(&source, fixture).expect("write fixture");

    // Newest-first over the rlib variants: the newest *loadable* one is
    // deterministic; stale variants are skipped by their environment
    // errors instead of failing the assertion.
    let mut last_environment_error = String::new();
    for rlib in core_rlib_candidates() {
        let output = Command::new("rustc")
            .arg("--edition=2024")
            .arg("--crate-type=lib")
            .arg("--emit=metadata")
            .arg("-o")
            .arg(out_dir.join("fixture.rmeta"))
            .arg("--extern")
            .arg(format!("cordis_core={}", rlib.display()))
            .arg("-L")
            .arg(format!("dependency={}", deps_dir().display()))
            .arg(&source)
            .output()
            .expect("rustc runs");
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        if is_environment_error(&stderr) {
            last_environment_error = stderr;
            continue;
        }
        assert!(
            !output.status.success(),
            "fixture must fail to compile:\n--- source ---\n{fixture}\n--- it compiled, but must not ---"
        );
        return stderr;
    }
    panic!(
        "no loadable cordis-core rlib variant for the fixture; \
         last environment error:\n{last_environment_error}"
    );
}

/// A short stable id for a fixture (first type name found, else a hash).
fn fixture_id(fixture: &str) -> String {
    let word = fixture
        .lines()
        .find(|line| line.contains("struct ") || line.contains("fn main"))
        .unwrap_or("fixture")
        .trim();
    let sanitized: String = word
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect();
    sanitized.chars().take(40).collect()
}

const NEXT_TWICE: &str = r#"
use cordis_core::{Context, WaterfallKey, define};
use std::sync::Arc;

struct Cfg;
struct Ping;
struct Reply;

// V31: `Next` is move-only. Forwarding twice must not compile — the Go
// baseline returned a settled result on the second call; Rust rejects the
// program instead (docs/04 §3.1).
pub fn fixture(ctx: Context) {
    let plugin = define("m", move |ctx: Context, _cfg: Arc<Cfg>| {
        let key = WaterfallKey::<Ping, Reply>::new("pipeline");
        async move {
            ctx.on_waterfall(
                key,
                |event: Ping, next: cordis_core::Next<Ping, Reply>| async move {
                    let first = next.run(event).await;
                    let second = next.run(event).await; // ERROR: use after move
                    let _ = (first, second);
                    Ok(Reply)
                },
                cordis_core::ListenerConfig::default(),
            )
            .await?;
            Ok(())
        }
    });
    let _ = plugin;
}
"#;

const RC_CONFIG: &str = r#"
use cordis_core::{App, Plugin, define};
use std::rc::Rc;

// V40: a !Send + !Sync configuration must not cross the plugin boundary.
pub fn fixture(app: &App) -> Plugin<Rc<String>> {
    define("rc", |_ctx, _cfg: std::sync::Arc<Rc<String>>| async { Ok(()) })
}
"#;

const UNSYNC_PAYLOAD: &str = r#"
use cordis_core::{Context, EventKey, define};
use std::sync::Arc;

struct Cfg;
struct NotSync(std::rc::Rc<u8>);

// V40: event payloads must be Send + Sync — they cross worker tasks.
pub fn fixture(ctx: Context) {
    let key = EventKey::<NotSync>::new("evt");
    let _ = key;
    let _ = ctx;
}
"#;

const UNSEND_FUTURE: &str = r#"
use cordis_core::{Context, define};
use std::sync::Arc;

struct Cfg;

// V40: an apply future that is not Send must not cross the erased plugin
// boundary — the future below captures an Rc across an await.
pub fn fixture() {
    let plugin = define("unsend", |_ctx: Context, _cfg: Arc<Cfg>| async {
        let marker = std::rc::Rc::new(0u8);
        some_send_boundary().await;
        let _ = marker;
        Ok(())
    });
    let _ = plugin;
}

async fn some_send_boundary() {}
"#;

#[test]
fn v40_compile_fail_unsend_future() {
    let stderr = compile_fail(UNSEND_FUTURE);
    assert!(
        stderr.contains("error[E0277]")
            || stderr.contains("future cannot be sent between threads safely"),
        "expected a Send-bound rejection, got:\n{stderr}"
    );
}

#[test]
fn v31_compile_fail_next_run_twice() {
    let stderr = compile_fail(NEXT_TWICE);
    assert!(
        stderr.contains("error[E0382]") && stderr.contains("use of moved value"),
        "expected a use-after-move error, got:\n{stderr}"
    );
}

#[test]
fn v40_compile_fail_rc_config() {
    let stderr = compile_fail(RC_CONFIG);
    assert!(
        stderr.contains("error[E0277]"),
        "expected a trait-bound rejection, got:\\n{stderr}"
    );
}

#[test]
fn v40_compile_fail_unsync_payload() {
    let stderr = compile_fail(UNSYNC_PAYLOAD);
    assert!(
        stderr.contains("error[E0277]") || stderr.contains("error[E0599]"),
        "expected a trait-bound rejection (the constructor exists only for Send + Sync payloads), got:\\n{stderr}"
    );
    assert!(
        stderr.contains("trait bounds were not satisfied"),
        "the rejection must name the unsatisfied bound:\\n{stderr}"
    );
}

//! Golden fixtures (V42–V45): the Rust compose/dump pipeline against
//! dumps produced by the **Go baseline** (`../cordis-go` loader), pinned
//! in `fixtures/golden_dump.txt`.
//!
//! How the goldens were produced: a generator program under /tmp ran the
//! baseline's `ParseLayer`/`ParsePatchLayer`/`Compose`/`DumpString` over
//! the same fixture files (Go 1.27.1, workspace-local GOCACHE/GOPATH —
//! see the task log; the sandbox denies the default cache paths). The
//! baseline IS runnable, so the fixtures are a real differential
//! comparison, not hand-written expectations.
//!
//! Intentional differences (docs/07 §6 notes "列有意差异"):
//! - the header line names the implementation (`# cordis-rs config
//!   dump`); the test normalizes it before comparing;
//! - sensitive-value redaction exists only in the Rust dump (default
//!   on); the fixtures avoid sensitive-looking keys so both render the
//!   same bytes, and a dedicated test below pins the redaction itself.

use cordis_loader::{ComposeOptions, DumpOptions, Layer, compose, dump};

fn fixture(name: &str) -> String {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {name}: {error}"))
}

fn golden() -> String {
    let path = format!(
        "{}/tests/fixtures/golden_dump.txt",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read_to_string(path)
        .expect("golden dump exists")
        .replace("\r\n", "\n")
}

/// Loads one scenario's layers and returns its dump with the header
/// line normalized for the differential comparison.
fn rust_dump(specs: &[(&str, &str, bool)]) -> String {
    let layers: Vec<Layer> = specs
        .iter()
        .map(|(label, file, patch)| {
            let data = fixture(file);
            if *patch {
                Layer::parse_patch(*label, &data).expect("patch layer parses")
            } else {
                Layer::parse(*label, &data).expect("base layer parses")
            }
        })
        .collect();
    let tree = compose(&layers, ComposeOptions::default()).expect("compose");
    let text = dump(&tree, DumpOptions::redacted());
    text.replace("# cordis-rs config dump", "# cordis-go config dump")
}

#[test]
fn golden_two_layer_config_replace_matches_the_go_baseline() {
    let ours = rust_dump(&[
        ("base", "two_layer_base.json", false),
        ("profile", "two_layer_profile.json", true),
    ]);
    let theirs = scenario(&golden(), "two_layer_config_replace");
    assert_eq!(ours, theirs);
}

#[test]
fn golden_group_children_match_the_go_baseline() {
    let ours = rust_dump(&[
        ("base", "group_base.json", false),
        ("profile", "group_profile.json", true),
    ]);
    let theirs = scenario(&golden(), "group_children");
    assert_eq!(ours, theirs);
}

#[test]
fn golden_insert_and_patch_order_matches_the_go_baseline() {
    let ours = rust_dump(&[
        ("base", "insert_base.json", false),
        ("insert", "insert_layer.json", true),
        ("patch", "insert_patch.json", true),
    ]);
    let theirs = scenario(&golden(), "insert_and_patch_order");
    assert_eq!(ours, theirs);
}

#[test]
fn golden_big_numbers_match_the_go_baseline_exactly() {
    // V44: 2^53+1 twice, i64 bounds and u64::MAX render identically to
    // the baseline's json.Number pipeline — no precision loss anywhere.
    let ours = rust_dump(&[("base", "big_numbers.json", false)]);
    let theirs = scenario(&golden(), "big_numbers");
    assert_eq!(ours, theirs);
    assert!(theirs.contains("9007199254740993"));
    assert!(theirs.contains("18446744073709551615"));
    assert!(theirs.contains("-9223372036854775808"));
}

#[test]
fn intentional_difference_redaction_exists_only_in_rust() {
    // The Rust dump masks sensitive keys by default and can show them
    // explicitly; the Go baseline has no redaction at all. Pin the
    // difference so it stays intentional.
    let base = Layer::parse(
        "base",
        r#"[{"id":"s","name":"s","config":{"password":"hunter2","v":1}}]"#,
    )
    .expect("layer");
    let tree = compose(&[base], ComposeOptions::default()).expect("compose");
    let redacted = dump(&tree, DumpOptions::redacted());
    assert!(redacted.contains("\"password\": \"***\""));
    let full = dump(&tree, DumpOptions::full());
    assert!(full.contains("hunter2"));
}

#[test]
fn provenance_survives_source_container_mutation() {
    // V45's aliasing half: mutating the parsed layer after composing must
    // not change the composed tree (no accidental aliasing).
    let mut layer =
        Layer::parse("base", r#"[{"id":"a","name":"p","config":{"v":1}}]"#).expect("layer");
    let tree = compose(&[layer.clone()], ComposeOptions::default()).expect("compose");
    // Mutate the source container.
    if let Some(config) = &mut layer.entries[0].config {
        config.insert("v".to_owned(), 99.into());
    }
    // The composed tree keeps the value it was built with.
    let node = tree.find("a").expect("a");
    assert_eq!(
        node.config.as_ref().expect("config")["v"],
        serde_json_ext::number(1)
    );
}

/// Extracts one `===== name =====` section from the golden file.
fn scenario(golden: &str, name: &str) -> String {
    let header = format!("===== {name} =====\n");
    let start = golden
        .find(&header)
        .unwrap_or_else(|| panic!("scenario {name} missing from goldens"));
    let body = &golden[start + header.len()..];
    let end = body.find("===== ").unwrap_or(body.len());
    body[..end].to_owned()
}

mod serde_json_ext {
    pub fn number(value: u64) -> serde_json::Value {
        serde_json::Value::Number(value.into())
    }
}

#[test]
fn allow_empty_organizations_accepts_true_from_environment() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_people-index"))
        .args([
            "run",
            "--store-url",
            "invalid://store",
            "--metrics-addr",
            "off",
        ])
        .env("PI_PUBLISH_ALLOW_EMPTY_ORGANIZATIONS", "true")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("invalid value"), "{stderr}");
    assert!(stderr.contains("store"), "{stderr}");
}

#[test]
fn schema_and_mapping_files_come_from_the_environment_and_empty_means_built_in() {
    let run = |schema: &str| {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_people-index"))
            .args(["run", "--store-url", "memory://", "--metrics-addr", "off"])
            .env("PI_SCHEMA_FILE", schema)
            .env("PI_MAPPING_FILE", "")
            .output()
            .unwrap();
        assert!(!output.status.success());
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
    };
    let missing = run("no-such-schema.json");
    assert!(missing.contains("PI_SCHEMA_FILE"), "{missing}");
    assert!(missing.contains("no-such-schema.json"), "{missing}");
    // Empty variables (compose's default) are the built-in schema and mapping: the run gets
    // as far as planning an empty store.
    let empty = run("");
    assert!(!empty.contains("PI_SCHEMA_FILE"), "{empty}");
    assert!(empty.contains("no person files"), "{empty}");
}

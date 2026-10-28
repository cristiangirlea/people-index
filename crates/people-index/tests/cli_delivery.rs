#[test]
fn environment_marker_and_delivery_flag_are_parsed() {
    let dir = tempfile::tempdir().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_people-index"))
        .args([
            "run",
            "--store-url",
            dir.path().to_str().unwrap(),
            "--delivery",
            "a/b",
            "--metrics-addr",
            "off",
        ])
        .env("PI_REQUIRE_COMPLETE_MARKER", "true")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(text.contains("raw/a/b/_COMPLETE"), "{text}");
}

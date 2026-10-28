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

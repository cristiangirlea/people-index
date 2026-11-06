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

/// `people-index run` with `PI_DELIVERY=latest` on a local store, with an Elasticsearch whose
/// alias serves run `r0`, which read delivery `b1`, the newest complete one.
async fn run_latest(
    dir: &std::path::Path,
    run_id: &str,
    republish: &str,
) -> (std::process::ExitStatus, String) {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/_alias/persons"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"persons-r0": {"aliases": {"persons": {}}}})),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/persons-r0/_mapping"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(
            {"persons-r0": {"mappings": {"_meta": {"people_index": {"run_id": "r0"}}}}}
        )))
        .mount(&server)
        .await;
    let (store, es, run_id, republish) = (
        dir.to_str().unwrap().to_owned(),
        server.uri(),
        run_id.to_owned(),
        republish.to_owned(),
    );
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new(env!("CARGO_BIN_EXE_people-index"))
            .args([
                "run",
                "--store-url",
                &store,
                "--es-url",
                &es,
                "--run-id",
                &run_id,
                "--task-attempts",
                "1",
                "--metrics-addr",
                "off",
            ])
            .env("PI_DELIVERY", "latest")
            .env("PI_REPUBLISH_UNCHANGED_DELIVERY", republish)
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    (
        output.status,
        String::from_utf8_lossy(&output.stdout).into_owned(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_skipped_run_exits_zero_and_the_republish_setting_comes_from_the_environment() {
    let dir = tempfile::tempdir().unwrap();
    let write = |key: &str, body: &[u8]| {
        let file = dir.path().join(key);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, body).unwrap();
    };
    // Not gzip: a run that plans it fails in extract, before Elasticsearch is written to.
    write("raw/b1/persons/a.ndjson.gz", &[1]);
    write("raw/b1/_COMPLETE", b"");
    write(
        "runs/r0/plan.json",
        serde_json::json!({"run_id": "r0", "delivery": "b1", "partitions": 16,
            "alias": "persons", "index": "persons-r0", "organizations": [], "persons": [],
            "total_bytes": 1, "created_at": "2026-11-01T02:00:00Z"})
        .to_string()
        .as_bytes(),
    );

    let (status, stdout) = run_latest(dir.path(), "r1", "false").await;
    assert_eq!(status.code(), Some(0), "{stdout}");
    let printed: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert_eq!(printed["status"], "skipped");
    assert_eq!(printed["delivery"], "b1");
    assert_eq!(printed["serving_run"], "r0");
    assert!(dir.path().join("runs/r1/skipped.json").exists());
    assert!(!dir.path().join("runs/r1/plan.json").exists());

    let (status, _) = run_latest(dir.path(), "r2", "true").await;
    assert_eq!(status.code(), Some(1));
    let plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("runs/r2/plan.json")).unwrap())
            .unwrap();
    assert_eq!(plan["delivery"], "b1");
    assert!(!dir.path().join("runs/r2/skipped.json").exists());
}

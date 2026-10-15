//! End-to-end checks: generate, read the files back as the pipeline would, and recompute the
//! expected counts independently of the generator's own simulation.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;

use flate2::read::MultiGzDecoder;
use pi_datagen::{EXPECTED_FILE, Expected, GenConfig, LocalDirSink, MemorySink, generate};
use serde_json::Value;

fn small(seed: u64) -> GenConfig {
    GenConfig {
        persons: 3_000,
        organizations: 600,
        files_per_feed: 3,
        seed,
        malformed_rate: 0.01,
        duplicate_org_rate: 0.05,
        duplicate_person_rate: 0.03,
        unresolved_rate: 0.1,
        extra_field_rate: 0.2,
        string_id_rate: 0.1,
        ..GenConfig::default()
    }
}

fn run(cfg: &GenConfig) -> (Expected, BTreeMap<String, Vec<u8>>) {
    let mut sink = MemorySink::new();
    let expected = generate(cfg, &mut sink).expect("generate");
    (expected, sink.files())
}

/// The lines of every file of `feed`, in feed order (file name, then line).
fn feed_lines(files: &BTreeMap<String, Vec<u8>>, feed: &str) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    for (path, bytes) in files.range(format!("{feed}/")..format!("{feed}0")) {
        assert!(path.ends_with(".ndjson.gz"), "{path}");
        let mut text = Vec::new();
        MultiGzDecoder::new(&bytes[..])
            .read_to_end(&mut text)
            .expect("valid gzip");
        assert!(text.is_empty() || text.ends_with(b"\n"), "{path}");
        lines.extend(
            text.split(|&b| b == b'\n')
                .filter(|l| !l.is_empty())
                .map(<[u8]>::to_vec),
        );
    }
    lines
}

/// The contract's envelope rule, restated here: `(id, record)` or `None` when malformed.
fn parse_envelope(line: &[u8]) -> Option<(u64, serde_json::Map<String, Value>)> {
    let Ok(Value::Object(mut env)) = serde_json::from_slice::<Value>(line) else {
        return None;
    };
    let id = match env.get("id")? {
        Value::Number(n) => n.as_u64()?,
        Value::String(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) => {
            s.parse().ok()?
        }
        _ => return None,
    };
    match env.remove("record")? {
        Value::Object(record) => Some((id, record)),
        _ => None,
    }
}

struct Recomputed {
    expected: Expected,
    org_versions: HashMap<u64, Vec<String>>,
    person_copies: HashMap<u64, usize>,
    role_counts: Vec<usize>,
    roles_total: usize,
    roles_null: usize,
    string_ids: usize,
    extra_fields: usize,
}

/// Recomputes `expected.json` from the files alone, using the generator's choice of titles and
/// names as the questions.
fn recompute(files: &BTreeMap<String, Vec<u8>>, questions: &Expected) -> Recomputed {
    let mut out = Recomputed {
        expected: Expected::default(),
        org_versions: HashMap::new(),
        person_copies: HashMap::new(),
        role_counts: Vec::new(),
        roles_total: 0,
        roles_null: 0,
        string_ids: 0,
        extra_fields: 0,
    };
    let mut orgs: HashMap<u64, String> = HashMap::new();
    for line in feed_lines(files, "organizations") {
        let Some((id, record)) = parse_envelope(&line) else {
            out.expected.malformed.organizations += 1;
            continue;
        };
        let name = record["name"].as_str().expect("org name").to_owned();
        out.org_versions.entry(id).or_default().push(name.clone());
        orgs.insert(id, name); // last wins
        out.string_ids += usize::from(line.starts_with(b"{\"id\":\""));
        out.extra_fields += usize::from(record.keys().any(|k| k.starts_with("x_")));
    }
    let mut persons: HashMap<u64, serde_json::Map<String, Value>> = HashMap::new();
    for line in feed_lines(files, "persons") {
        let Some((id, record)) = parse_envelope(&line) else {
            out.expected.malformed.persons += 1;
            continue;
        };
        *out.person_copies.entry(id).or_default() += 1;
        out.string_ids += usize::from(line.starts_with(b"{\"id\":\""));
        persons.insert(id, record); // last wins
    }

    out.expected.persons = persons.len() as u64;
    out.expected.organizations = orgs.len() as u64;
    let mut title_counts: BTreeMap<String, u64> = questions
        .role_title_counts
        .keys()
        .map(|k| (k.clone(), 0))
        .collect();
    let mut name_counts: BTreeMap<String, u64> = questions
        .organization_name_counts
        .keys()
        .map(|k| (k.clone(), 0))
        .collect();
    for record in persons.values() {
        let roles = record["roles"].as_array().expect("roles array");
        out.role_counts.push(roles.len());
        let mut titles = HashSet::new();
        let mut names = HashSet::new();
        let mut unresolved = false;
        for role in roles {
            out.roles_total += 1;
            titles.insert(role["title"].as_str().expect("title").to_owned());
            match &role["organization_id"] {
                Value::Null => out.roles_null += 1,
                Value::Number(n) => {
                    let org = n.as_u64().expect("u64 org id");
                    match orgs.get(&org) {
                        Some(name) => {
                            names.insert(name.clone());
                        }
                        None => unresolved = true,
                    }
                }
                other => panic!("unexpected organization_id {other}"),
            }
        }
        out.expected.persons_with_unresolved += u64::from(unresolved);
        for (title, n) in &mut title_counts {
            *n += u64::from(titles.contains(title));
        }
        for (name, n) in &mut name_counts {
            *n += u64::from(names.contains(name));
        }
    }
    out.expected.role_title_counts = title_counts;
    out.expected.organization_name_counts = name_counts;
    out
}

#[test]
fn expected_counts_match_an_independent_recount() {
    let cfg = small(7);
    let (expected, files) = run(&cfg);
    let re = recompute(&files, &expected);
    assert_eq!(re.expected, expected);

    assert_eq!(expected.persons, cfg.persons);
    assert_eq!(expected.organizations, cfg.organizations);
    assert_eq!(expected.role_title_counts.len(), 20);
    assert!(expected.organization_name_counts.len() >= 15);
    assert!(expected.persons_with_unresolved > 0);
    assert!(expected.malformed.persons > 0 && expected.malformed.organizations > 0);

    // Last-wins must be observable: renamed organizations, and persons sent twice.
    let renamed = re
        .org_versions
        .values()
        .filter(|v| v.len() == 2 && v[0] != v[1])
        .count();
    assert!(renamed > 10, "{renamed} renamed organizations");
    assert!(re.org_versions.values().all(|v| v.len() <= 2));
    assert!(re.person_copies.values().filter(|&&n| n == 2).count() > 30);

    // The headline title counts persons; the rarest titles may be zero but not all are.
    let counts: Vec<u64> = expected.role_title_counts.values().copied().collect();
    assert!(counts.iter().any(|&c| c > 300));
    assert!(counts.iter().filter(|&&c| c > 0).count() >= 15);
    let name_counts: Vec<u64> = expected
        .organization_name_counts
        .values()
        .copied()
        .collect();
    assert!(name_counts.iter().any(|&c| c > 100), "{name_counts:?}");

    // Realism knobs show up in the data.
    assert!(re.role_counts.iter().all(|&n| n <= 8));
    assert!(re.role_counts.contains(&0) && re.role_counts.contains(&8));
    #[allow(clippy::cast_precision_loss)]
    let null_share = re.roles_null as f64 / re.roles_total as f64;
    assert!((0.29..0.37).contains(&null_share), "{null_share}");
    assert!(re.string_ids > 100);
    assert!(re.extra_fields > 50);
}

#[test]
fn same_seed_same_bytes_and_parallelism_does_not_matter() {
    let cfg = small(11);
    let (e1, f1) = run(&cfg);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .build()
        .expect("pool");
    let (e2, f2) = pool.install(|| run(&cfg));
    assert_eq!(e1, e2);
    assert_eq!(f1, f2);
    assert_eq!(f1.len(), 2 * 3 + 1);
    assert!(f1.contains_key(EXPECTED_FILE));
    assert!(f1.contains_key("organizations/part-00002.ndjson.gz"));
    assert!(f1.contains_key("persons/part-00000.ndjson.gz"));

    let (_, other) = run(&small(12));
    assert_ne!(
        f1["persons/part-00000.ndjson.gz"],
        other["persons/part-00000.ndjson.gz"]
    );
}

#[test]
fn malformed_rate_is_respected() {
    let cfg = GenConfig {
        persons: 20_000,
        organizations: 2_000,
        files_per_feed: 4,
        malformed_rate: 0.01,
        gzip_level: 1,
        ..GenConfig::default()
    };
    let (expected, files) = run(&cfg);
    let re = recompute(&files, &expected);
    assert_eq!(re.expected.malformed, expected.malformed);
    // About 1% of the ~20k person records: a malformed line is drawn before each record.
    let p = expected.malformed.persons;
    assert!((150..=260).contains(&p), "{p} malformed person lines");

    let clean = GenConfig {
        malformed_rate: 0.0,
        ..small(3)
    };
    let (expected, _) = run(&clean);
    assert_eq!(expected.malformed.persons, 0);
    assert_eq!(expected.malformed.organizations, 0);
}

#[test]
fn local_dir_sink_writes_the_layout() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut sink = LocalDirSink::new(dir.path()).expect("sink");
    let cfg = GenConfig {
        persons: 200,
        organizations: 50,
        files_per_feed: 2,
        ..GenConfig::default()
    };
    let expected = generate(&cfg, &mut sink).expect("generate");
    for feed in ["organizations", "persons"] {
        let mut names: Vec<String> = std::fs::read_dir(dir.path().join(feed))
            .expect("feed dir")
            .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(names, ["part-00000.ndjson.gz", "part-00001.ndjson.gz"]);
    }
    let text = std::fs::read(dir.path().join(EXPECTED_FILE)).expect("expected.json");
    let back: Expected = serde_json::from_slice(&text).expect("parse");
    assert_eq!(back, expected);
}

#[test]
fn edge_sizes_generate_valid_feeds() {
    // No organizations: every non-null role is unresolved.
    let cfg = GenConfig {
        persons: 300,
        organizations: 0,
        files_per_feed: 2,
        ..GenConfig::default()
    };
    let (expected, files) = run(&cfg);
    assert!(expected.organization_name_counts.is_empty());
    assert_eq!(recompute(&files, &expected).expected, expected);
    assert!(expected.persons_with_unresolved > 150);

    // More files than records: empty files are still valid gzip.
    let cfg = GenConfig {
        persons: 3,
        organizations: 2,
        files_per_feed: 8,
        duplicate_person_rate: 1.0,
        duplicate_org_rate: 1.0,
        ..GenConfig::default()
    };
    let (expected, files) = run(&cfg);
    assert_eq!(files.len(), 17);
    assert_eq!(recompute(&files, &expected).expected, expected);
    assert_eq!(expected.persons, 3);

    // Nothing at all.
    let cfg = GenConfig {
        persons: 0,
        organizations: 0,
        files_per_feed: 1,
        ..GenConfig::default()
    };
    let (expected, _) = run(&cfg);
    assert_eq!(expected.persons, 0);
    assert!(expected.role_title_counts.values().all(|&c| c == 0));
}

#[test]
fn invalid_config_is_refused_before_writing() {
    let cfg = GenConfig {
        files_per_feed: 0,
        ..GenConfig::default()
    };
    let mut sink = MemorySink::new();
    assert!(generate(&cfg, &mut sink).is_err());
    assert!(sink.files().is_empty());
}

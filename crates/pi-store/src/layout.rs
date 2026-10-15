//! [`RunLayout`]: every key of the storage layout in docs/ARCHITECTURE.md, built in one place.
//!
//! Nothing else in the workspace formats a store key by hand, so the layout can change in one
//! file and a typo cannot send one stage's outputs where the next stage does not look.

use std::fmt;

use crate::error::StoreError;

/// The top-level directory that holds every run.
pub const RUNS_ROOT: &str = "runs";

/// The longest id accepted for a run, task or attempt: far below S3's 1024-byte key limit
/// once the rest of the path is added.
const MAX_ID_LEN: usize = 128;

/// An id that cannot be used as one path segment.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid {kind} {value:?}: {reason}")]
pub struct LayoutError {
    /// What the id names (`run id`, `task id`, ...).
    pub kind: &'static str,
    /// The rejected value.
    pub value: String,
    /// Why it was rejected.
    pub reason: &'static str,
}

impl From<LayoutError> for StoreError {
    fn from(e: LayoutError) -> Self {
        StoreError::InvalidKey {
            key: e.value.clone(),
            reason: e.to_string(),
        }
    }
}

/// Checks that an id is one safe path segment: `[A-Za-z0-9._=-]`, not `.` or `..`, bounded.
///
/// The character set keeps ids portable across S3 key rules, Windows file names and shell
/// quoting, and leaves `/` impossible so an id can never escape its directory.
pub fn validate_id(kind: &'static str, value: &str) -> Result<(), LayoutError> {
    let err = |reason| LayoutError {
        kind,
        value: value.to_owned(),
        reason,
    };
    if value.is_empty() {
        return Err(err("empty"));
    }
    if value.len() > MAX_ID_LEN {
        return Err(err("longer than 128 bytes"));
    }
    if value == "." || value == ".." {
        return Err(err("a relative path segment"));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'=' | b'-'))
    {
        return Err(err("only ASCII letters, digits and . _ = - are allowed"));
    }
    Ok(())
}

/// The kinds of segment data the stages exchange, each partitioned by one key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Dataset {
    /// `OrgRow`s, partitioned by organization (`p`).
    Orgs,
    /// `PersonRow`s, partitioned by person (`q`).
    Persons,
    /// `EdgeRow`s, partitioned by organization (`p`).
    Edges,
    /// `JoinedRow`s, partitioned by person (`q`).
    Joined,
    /// `DocRow`s, partitioned by person (`q`).
    Docs,
}

impl Dataset {
    /// Every dataset, in pipeline order.
    pub const ALL: [Dataset; 5] = [
        Dataset::Orgs,
        Dataset::Persons,
        Dataset::Edges,
        Dataset::Joined,
        Dataset::Docs,
    ];

    /// The directory name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Orgs => "orgs",
            Self::Persons => "persons",
            Self::Edges => "edges",
            Self::Joined => "joined",
            Self::Docs => "docs",
        }
    }

    /// The partition key letter: `p` for organization-keyed data, `q` for person-keyed data.
    #[must_use]
    pub fn partition_key(self) -> char {
        match self {
            Self::Orgs | Self::Edges => 'p',
            Self::Persons | Self::Joined | Self::Docs => 'q',
        }
    }
}

impl fmt::Display for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Builds the keys of one run (and of the shared inputs) relative to the store root.
///
/// Methods that take a task or attempt id validate it and return `Result`; stage names come
/// from the coordinator's own enum and are validated too, since the type here is a `&str`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunLayout {
    run_id: String,
    input_root: String,
}

impl RunLayout {
    /// The layout of run `run_id`, with inputs under `raw/` in the same store.
    pub fn new(run_id: impl Into<String>) -> Result<Self, LayoutError> {
        let run_id = run_id.into();
        validate_id("run id", &run_id)?;
        Ok(Self {
            run_id,
            input_root: "raw".to_owned(),
        })
    }

    /// Moves the inputs to another root: `""` when they live at the top of their own store
    /// (`PI_INPUT_URL`), so `organizations/` and `persons/` are read from there.
    #[must_use]
    pub fn with_input_root(mut self, root: &str) -> Self {
        root.trim_matches('/').clone_into(&mut self.input_root);
        self
    }

    /// The run id.
    #[must_use]
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// The directory of every run: `runs`.
    #[must_use]
    pub fn runs_root() -> &'static str {
        RUNS_ROOT
    }

    /// Extracts the run id from a listed run directory (`runs/<id>`), if it is one.
    #[must_use]
    pub fn run_id_from_dir(dir: &str) -> Option<&str> {
        let id = dir
            .trim_end_matches('/')
            .strip_prefix(RUNS_ROOT)?
            .strip_prefix('/')?;
        validate_id("run id", id).ok().map(|()| id)
    }

    /// `raw/organizations`: the organization feed's directory.
    #[must_use]
    pub fn raw_organizations(&self) -> String {
        self.input_dir("organizations")
    }

    /// `raw/persons`: the person feed's directory.
    #[must_use]
    pub fn raw_persons(&self) -> String {
        self.input_dir("persons")
    }

    fn input_dir(&self, feed: &str) -> String {
        if self.input_root.is_empty() {
            feed.to_owned()
        } else {
            format!("{}/{feed}", self.input_root)
        }
    }

    /// `runs/<run_id>`.
    #[must_use]
    pub fn run_root(&self) -> String {
        format!("{RUNS_ROOT}/{}", self.run_id)
    }

    /// `runs/<run_id>/plan.json`.
    #[must_use]
    pub fn plan(&self) -> String {
        format!("{}/plan.json", self.run_root())
    }

    /// `runs/<run_id>/state.json`: the coordinator's view of the run (stage, status).
    #[must_use]
    pub fn state(&self) -> String {
        format!("{}/state.json", self.run_root())
    }

    /// `runs/<run_id>/failed.json`: created once by whoever fails the run first, so every
    /// worker and the coordinator stop.
    #[must_use]
    pub fn failure_marker(&self) -> String {
        format!("{}/failed.json", self.run_root())
    }

    /// `runs/<run_id>/summary.json`.
    #[must_use]
    pub fn summary(&self) -> String {
        format!("{}/summary.json", self.run_root())
    }

    /// `runs/<run_id>/stages`.
    #[must_use]
    pub fn stages_dir(&self) -> String {
        format!("{}/stages", self.run_root())
    }

    /// `runs/<run_id>/stages/<stage>.json`: the stage's task list.
    pub fn stage_spec(&self, stage: &str) -> Result<String, LayoutError> {
        validate_id("stage name", stage)?;
        Ok(format!("{}/{stage}.json", self.stages_dir()))
    }

    /// `runs/<run_id>/<stage>`.
    pub fn stage_root(&self, stage: &str) -> Result<String, LayoutError> {
        validate_id("stage name", stage)?;
        Ok(format!("{}/{stage}", self.run_root()))
    }

    /// `runs/<run_id>/<stage>/_commit`.
    pub fn commits_dir(&self, stage: &str) -> Result<String, LayoutError> {
        Ok(format!("{}/_commit", self.stage_root(stage)?))
    }

    /// `runs/<run_id>/<stage>/_commit/<task>.json`.
    pub fn commit(&self, stage: &str, task: &str) -> Result<String, LayoutError> {
        validate_id("task id", task)?;
        Ok(format!("{}/{task}.json", self.commits_dir(stage)?))
    }

    /// `runs/<run_id>/<stage>/_lease`.
    pub fn leases_dir(&self, stage: &str) -> Result<String, LayoutError> {
        Ok(format!("{}/_lease", self.stage_root(stage)?))
    }

    /// `runs/<run_id>/<stage>/_lease/<task>.json`.
    pub fn lease(&self, stage: &str, task: &str) -> Result<String, LayoutError> {
        validate_id("task id", task)?;
        Ok(format!("{}/{task}.json", self.leases_dir(stage)?))
    }

    /// The task id of a key under `_commit/` or `_lease/` (`.../_commit/p=3.json` gives `p=3`).
    #[must_use]
    pub fn task_id_from_key(key: &str) -> Option<&str> {
        let file = key.rsplit('/').next()?;
        let task = file.strip_suffix(".json")?;
        validate_id("task id", task).ok().map(|()| task)
    }

    /// `runs/<run_id>/<stage>/data`: every output of the stage, committed or not.
    pub fn data_dir(&self, stage: &str) -> Result<String, LayoutError> {
        Ok(format!("{}/data", self.stage_root(stage)?))
    }

    /// `runs/<run_id>/<stage>/data/<dataset>`.
    pub fn dataset_dir(&self, stage: &str, dataset: Dataset) -> Result<String, LayoutError> {
        Ok(format!("{}/{dataset}", self.data_dir(stage)?))
    }

    /// `runs/<run_id>/<stage>/data/<dataset>/<p|q>=<partition>`.
    pub fn partition_dir(
        &self,
        stage: &str,
        dataset: Dataset,
        partition: u32,
    ) -> Result<String, LayoutError> {
        Ok(format!(
            "{}/{}={partition}",
            self.dataset_dir(stage, dataset)?,
            dataset.partition_key()
        ))
    }

    /// One segment file a task attempt writes:
    /// `runs/<run_id>/<stage>/data/<dataset>[/<p|q>=<partition>]/<task>-<attempt>-<n>.seg`.
    ///
    /// The attempt id in the name keeps a retried or duplicate attempt from overwriting the
    /// files a committed attempt listed.
    pub fn segment(
        &self,
        stage: &str,
        dataset: Dataset,
        partition: Option<u32>,
        task: &str,
        attempt: &str,
        n: u32,
    ) -> Result<String, LayoutError> {
        validate_id("task id", task)?;
        validate_id("attempt id", attempt)?;
        let dir = match partition {
            Some(p) => self.partition_dir(stage, dataset, p)?,
            None => self.dataset_dir(stage, dataset)?,
        };
        Ok(format!("{dir}/{task}-{attempt}-{n}.seg"))
    }

    /// `runs/<run_id>/quarantine`.
    #[must_use]
    pub fn quarantine_dir(&self) -> String {
        format!("{}/quarantine", self.run_root())
    }

    /// `runs/<run_id>/quarantine/<task>-<attempt>.ndjson`: malformed lines with their origin.
    pub fn quarantine(&self, task: &str, attempt: &str) -> Result<String, LayoutError> {
        validate_id("task id", task)?;
        validate_id("attempt id", attempt)?;
        Ok(format!("{}/{task}-{attempt}.ndjson", self.quarantine_dir()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout() -> RunLayout {
        RunLayout::new("20261015T120000Z-ab12").unwrap()
    }

    #[test]
    fn every_path_matches_the_contract() {
        let l = layout();
        let r = "runs/20261015T120000Z-ab12";
        assert_eq!(l.raw_organizations(), "raw/organizations");
        assert_eq!(l.raw_persons(), "raw/persons");
        assert_eq!(l.run_root(), r);
        assert_eq!(l.plan(), format!("{r}/plan.json"));
        assert_eq!(l.state(), format!("{r}/state.json"));
        assert_eq!(l.failure_marker(), format!("{r}/failed.json"));
        assert_eq!(l.summary(), format!("{r}/summary.json"));
        assert_eq!(
            l.stage_spec("join").unwrap(),
            format!("{r}/stages/join.json")
        );
        assert_eq!(
            l.commit("join", "p=7").unwrap(),
            format!("{r}/join/_commit/p=7.json")
        );
        assert_eq!(
            l.lease("join", "p=7").unwrap(),
            format!("{r}/join/_lease/p=7.json")
        );
        assert_eq!(l.data_dir("join").unwrap(), format!("{r}/join/data"));
        assert_eq!(
            l.segment("join", Dataset::Joined, Some(3), "p=7", "a1", 0)
                .unwrap(),
            format!("{r}/join/data/joined/q=3/p=7-a1-0.seg")
        );
        assert_eq!(
            l.segment("extract-orgs", Dataset::Orgs, Some(12), "f00001", "a1", 2)
                .unwrap(),
            format!("{r}/extract-orgs/data/orgs/p=12/f00001-a1-2.seg")
        );
        assert_eq!(
            l.segment("assemble", Dataset::Docs, None, "q=1", "a1", 0)
                .unwrap(),
            format!("{r}/assemble/data/docs/q=1-a1-0.seg")
        );
        assert_eq!(
            l.quarantine("f00001", "a1").unwrap(),
            format!("{r}/quarantine/f00001-a1.ndjson")
        );
    }

    #[test]
    fn input_root_can_move() {
        let l = layout().with_input_root("/");
        assert_eq!(l.raw_persons(), "persons");
        let l = layout().with_input_root("feeds/2026/");
        assert_eq!(l.raw_organizations(), "feeds/2026/organizations");
    }

    #[test]
    fn ids_cannot_escape_their_directory() {
        assert!(RunLayout::new("").is_err());
        assert!(RunLayout::new("..").is_err());
        assert!(RunLayout::new("a/b").is_err());
        assert!(RunLayout::new("a b").is_err());
        assert!(RunLayout::new("x".repeat(129)).is_err());
        let l = layout();
        assert!(l.commit("join", "../x").is_err());
        assert!(l.commit("join/../x", "t").is_err());
        assert!(
            l.segment("join", Dataset::Docs, None, "t", "a/b", 0)
                .is_err()
        );
    }

    #[test]
    fn ids_parse_back_from_keys() {
        let l = layout();
        let key = l.commit("join", "p=7").unwrap();
        assert_eq!(RunLayout::task_id_from_key(&key), Some("p=7"));
        assert_eq!(
            RunLayout::task_id_from_key("runs/x/join/_commit/x.txt"),
            None
        );
        assert_eq!(
            RunLayout::run_id_from_dir("runs/20261015T120000Z-ab12/"),
            Some("20261015T120000Z-ab12")
        );
        assert_eq!(RunLayout::run_id_from_dir("raw/persons"), None);
    }

    #[test]
    fn dataset_partition_keys() {
        let keys: Vec<char> = Dataset::ALL.iter().map(|d| d.partition_key()).collect();
        assert_eq!(keys, vec!['p', 'q', 'p', 'q', 'q']);
    }
}

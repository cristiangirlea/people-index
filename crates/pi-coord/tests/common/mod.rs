//! Fake stages and a fault-injecting store for the executor tests.

#![allow(
    dead_code,
    reason = "each test binary uses a different part of this module"
)]

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use pi_coord::{
    AttemptId, LeaseConfig, RetryPolicy, RunContext, Stage, StageName, StoreCoordination,
    TaskReport, TaskSpec, permanent,
};
use pi_store::{Backend, Dataset, Store};

/// What a fake task does on one attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Behavior {
    Succeed,
    Fail,
    FailPermanently,
    /// Waits until its context is cancelled, then reports `Cancelled`-free failure.
    Hang,
    Panic,
}

type Script = dyn Fn(&TaskSpec, u32) -> Behavior + Send + Sync;

/// A stage with `n` tasks (or one `run` task when `n == 0`) whose behavior is scripted per
/// task and attempt number, and which records every run.
pub struct FakeStage {
    pub name: StageName,
    pub n: u32,
    pub delay: Duration,
    script: Box<Script>,
    pub runs: Arc<Mutex<HashMap<String, u32>>>,
    pub in_flight: Arc<AtomicUsize>,
    pub max_in_flight: Arc<AtomicUsize>,
    pub saw_cancel: Arc<AtomicBool>,
}

impl fmt::Debug for FakeStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FakeStage")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl FakeStage {
    pub fn new(name: StageName, n: u32) -> Self {
        Self {
            name,
            n,
            delay: Duration::ZERO,
            script: Box::new(|_, _| Behavior::Succeed),
            runs: Arc::default(),
            in_flight: Arc::default(),
            max_in_flight: Arc::default(),
            saw_cancel: Arc::default(),
        }
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    pub fn with_script(
        mut self,
        script: impl Fn(&TaskSpec, u32) -> Behavior + Send + Sync + 'static,
    ) -> Self {
        self.script = Box::new(script);
        self
    }

    pub fn runs_of(&self, task: &str) -> u32 {
        self.runs.lock().unwrap().get(task).copied().unwrap_or(0)
    }

    pub fn total_runs(&self) -> u32 {
        self.runs.lock().unwrap().values().sum()
    }

    pub fn task_ids(&self) -> Vec<String> {
        if self.n == 0 {
            vec!["run".to_owned()]
        } else {
            (0..self.n)
                .map(|p| TaskSpec::for_partition(self.name, p).task_id)
                .collect()
        }
    }
}

#[async_trait]
impl Stage for FakeStage {
    fn name(&self) -> StageName {
        self.name
    }

    async fn tasks(&self, _ctx: &RunContext) -> anyhow::Result<Vec<TaskSpec>> {
        Ok(if self.n == 0 {
            vec![TaskSpec::single(self.name)]
        } else {
            (0..self.n)
                .map(|p| TaskSpec::for_partition(self.name, p))
                .collect()
        })
    }

    async fn run(
        &self,
        ctx: &RunContext,
        task: &TaskSpec,
        attempt: &AttemptId,
    ) -> anyhow::Result<TaskReport> {
        let attempt_no = {
            let mut runs = self.runs.lock().unwrap();
            let n = runs.entry(task.task_id.clone()).or_default();
            *n += 1;
            *n
        };
        let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(now, Ordering::SeqCst);
        let _guard = Decrement(Arc::clone(&self.in_flight));

        // Outputs first, as a real stage would: a failing attempt leaves orphans behind.
        let key = ctx.layout.segment(
            self.name.as_str(),
            Dataset::Docs,
            task.partition,
            &task.task_id,
            &attempt.to_string(),
            0,
        )?;
        ctx.store
            .put(&key, format!("{}:{attempt}", task.task_id))
            .await?;

        if !self.delay.is_zero() {
            tokio::select! {
                () = ctx.cancel.cancelled() => {
                    self.saw_cancel.store(true, Ordering::SeqCst);
                    anyhow::bail!("cancelled while working");
                }
                () = tokio::time::sleep(self.delay) => {}
            }
        }
        match (self.script)(task, attempt_no) {
            Behavior::Succeed => {
                let mut report = TaskReport {
                    outputs: vec![key],
                    bytes_out: 10,
                    ..TaskReport::default()
                };
                report.add("records", 1);
                Ok(report)
            }
            Behavior::Fail => anyhow::bail!("{} failed on attempt {attempt_no}", task.task_id),
            Behavior::FailPermanently => Err(permanent(anyhow::anyhow!(
                "{} is broken for good",
                task.task_id
            ))),
            Behavior::Hang => {
                ctx.cancel.cancelled().await;
                self.saw_cancel.store(true, Ordering::SeqCst);
                anyhow::bail!("{} stopped by cancellation", task.task_id)
            }
            Behavior::Panic => panic!("{} panicked on purpose", task.task_id),
        }
    }
}

struct Decrement(Arc<AtomicUsize>);

impl Drop for Decrement {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

pub fn fast_retry(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        base_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(20),
    }
}

pub fn short_leases(ttl_ms: u64) -> LeaseConfig {
    LeaseConfig {
        ttl: Duration::from_millis(ttl_ms),
        clock_skew: Duration::ZERO,
    }
}

pub fn coordination(worker: &str, ttl_ms: u64) -> Arc<StoreCoordination> {
    Arc::new(StoreCoordination::new(worker, short_leases(ttl_ms)))
}

pub fn ctx(store: &Store, run_id: &str) -> RunContext {
    RunContext::new(run_id, store.clone()).unwrap()
}

/// Polls `cond` every 10 ms until it holds or `timeout` passes.
pub async fn eventually<F, Fut>(timeout: Duration, mut cond: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        if cond().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond().await
}

/// Faults a [`FlakyStore`] injects, switched on from the test.
#[derive(Debug, Clone, Default)]
pub struct Faults {
    /// While set, puts fail without reaching the store: an unreachable S3.
    pub failing: Arc<AtomicBool>,
    /// The next this many puts land on the store but report an error: a lost response.
    pub lost_responses: Arc<AtomicUsize>,
    /// The next this many gets fail.
    pub failed_gets: Arc<AtomicUsize>,
    /// While set, every update-if-match is refused and the object left alone: a store whose
    /// conditional check never accepts the version it handed out.
    pub refused_updates: Arc<AtomicBool>,
    /// Puts seen so far, faulted or not.
    pub puts: Arc<AtomicUsize>,
}

/// An in-memory store that injects the [`Faults`] it was built with.
#[derive(Debug)]
pub struct FlakyStore {
    inner: InMemory,
    faults: Faults,
}

impl FlakyStore {
    /// A store whose puts fail while the returned flag is set.
    pub fn store() -> (Store, Arc<AtomicBool>) {
        let (store, faults) = Self::with_faults();
        (store, faults.failing)
    }

    /// A store with every fault off, and the switches.
    pub fn with_faults() -> (Store, Faults) {
        let faults = Faults::default();
        let flaky = FlakyStore {
            inner: InMemory::new(),
            faults: faults.clone(),
        };
        (
            Store::from_object_store(Arc::new(flaky), Backend::Memory, "flaky://"),
            faults,
        )
    }

    fn check(&self) -> object_store::Result<()> {
        if self.faults.failing.load(Ordering::SeqCst) {
            Err(injected("injected outage"))
        } else {
            Ok(())
        }
    }
}

/// Takes one from `counter` when it is above zero.
// `fetch_update` is deprecated since Rust 1.99 in favour of `try_update`, which needs 1.95;
// `rust-version` is 1.90, so the old name stays.
#[allow(deprecated)]
fn take_one(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
        .is_ok()
}

fn injected(what: &'static str) -> object_store::Error {
    object_store::Error::Generic {
        store: "flaky",
        source: what.into(),
    }
}

impl fmt::Display for FlakyStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("FlakyStore")
    }
}

#[async_trait]
impl ObjectStore for FlakyStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.faults.puts.fetch_add(1, Ordering::SeqCst);
        self.check()?;
        if matches!(opts.mode, PutMode::Update(_))
            && self.faults.refused_updates.load(Ordering::SeqCst)
        {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: "injected refusal".into(),
            });
        }
        let lose_response = take_one(&self.faults.lost_responses);
        let result = self.inner.put_opts(location, payload, opts).await;
        if lose_response && result.is_ok() {
            return Err(injected("injected lost response"));
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.check()?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if take_one(&self.faults.failed_gets) {
            return Err(injected("injected read failure"));
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// One request a [`CountingStore`] saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    List,
    Get,
    /// A create-if-absent put.
    Create,
    /// An update-if-match put.
    Update,
    /// An unconditional put.
    Put,
}

/// The requests a [`CountingStore`] saw, with their key or prefix.
#[derive(Debug, Clone, Default)]
pub struct Ops(Arc<Mutex<Vec<(Op, String)>>>);

impl Ops {
    fn record(&self, op: Op, path: String) {
        self.0.lock().unwrap().push((op, path));
    }

    pub fn clear(&self) {
        self.0.lock().unwrap().clear();
    }

    /// Requests of kind `op` whose key or prefix satisfies `matches`.
    pub fn count(&self, op: Op, matches: impl Fn(&str) -> bool) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(o, path)| *o == op && matches(path))
            .count()
    }
}

/// An in-memory store that records every list, get and put: what a worker costs S3.
#[derive(Debug)]
pub struct CountingStore {
    inner: InMemory,
    ops: Ops,
}

impl CountingStore {
    pub fn store() -> (Store, Ops) {
        let ops = Ops::default();
        let counting = CountingStore {
            inner: InMemory::new(),
            ops: ops.clone(),
        };
        (
            Store::from_object_store(Arc::new(counting), Backend::Memory, "counting://"),
            ops,
        )
    }
}

impl fmt::Display for CountingStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CountingStore")
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let op = match opts.mode {
            PutMode::Create => Op::Create,
            PutMode::Update(_) => Op::Update,
            PutMode::Overwrite => Op::Put,
        };
        self.ops.record(op, location.to_string());
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.ops.record(Op::Put, location.to_string());
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.ops.record(Op::Get, location.to_string());
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.ops.record(
            Op::List,
            prefix.map(ToString::to_string).unwrap_or_default(),
        );
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.ops.record(
            Op::List,
            prefix.map(ToString::to_string).unwrap_or_default(),
        );
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

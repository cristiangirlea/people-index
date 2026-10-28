# people-index: guide for agents

A staged, multi-node ETL in Rust: two gzipped NDJSON feeds (persons, organizations) are
partitioned, joined with a partitioned hash join and loaded into an Elasticsearch cluster behind
an alias. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is the contract: the data, the stages,
the storage layout, the commit and lease protocol, the crates and the configuration. Read it
before changing anything; when code and it disagree, fix one of them in the same change.

## Layout

- `crates/pi-model`: schema, envelope parsing, partitioner, rows, document builder (no I/O).
- `crates/pi-segment`: the segment file format.
- `crates/pi-store`: object store wrapper and `RunLayout` (every path).
- `crates/pi-coord`: `Stage` / `Executor` traits, commits, leases, local and distributed executors.
- `crates/pi-es`: Elasticsearch client, bulk indexer, index admin, publisher, verifier, mapping.
- `crates/pi-stages`: one type per stage.
- `crates/pi-datagen`: seeded synthetic feeds and their `expected.json`.
- `crates/people-index`: the binary.
- `deploy/k3d`, `deploy/k8s` (Kustomize, ECK), `deploy/compose`, `Dockerfile`, `Makefile`.
- `docs/`: architecture, operations, decisions in `docs/adr/`.

## Commands CI runs

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
scripts/check-deploy.sh        # kustomize build | kubeconform, compose config
scripts/scrub-check.sh
scripts/test-scrub-check.sh   # the scrub check and hooks, in a throwaway repo and worktree
scripts/test-release-notes.sh  # release tag and changelog extraction
scripts/test-check-republish.sh  # the compose republish check, against stub curl and aws
scripts/test-check-pins-resolve.sh  # the pinned-image resolve check, against a stub docker
scripts/test-check-modes.sh   # the mode check, in a throwaway repo
scripts/check-modes.sh        # hooks and *.sh are stored as 100755
scripts/test-check-s3.sh      # the S3 check, against stub docker and cargo
scripts/check-s3.sh           # store conformance and leases on the compose SeaweedFS (Docker)
cargo deny check               # audit job (also weekly)
```

CI also checks the MSRV (`cargo check` at `rust-version`), lints the workflow, shell scripts and
Dockerfile (actionlint, shellcheck, hadolint), builds the image, and on pushes to `main` runs
the compose end-to-end (`make compose-e2e`: a run that fails in publish, then two runs, so
the second publish replaces the first and the failed run's index is deleted). `make check` runs the list above (cargo-deny when installed); `/check`
(`.claude/skills/check`) runs all but cargo-deny.

## Rules

- One component per file, small types behind traits; I/O at the edges, pure logic in
  `pi-model`. No `unwrap`/`expect` outside tests and provably infallible spots (say why).
- CPU work (gzip, JSON, zstd) runs off the async reactor: rayon or `spawn_blocking`.
- Every task is idempotent and commits with create-if-absent; readers list commits, not data.
- Memory is bounded by configuration (partition size, segment size, bulk bytes), never by the
  size of the input.
- Each crate's public behaviour has tests next to it; stages are tested on a local store,
  Elasticsearch calls against a mock server.
- No forbidden words: `scripts/scrub-check.sh` (pre-commit and commit-msg hooks after
  `git config core.hooksPath .githooks`) reads the list from outside the repository, beside the
  main checkout, from every worktree.
- Hooks and scripts are stored as 100755: `git update-index --chmod=+x <path>` for a new one
  (Git for Windows does not set it, `scripts/check-modes.sh` fails without it).
- No AI attribution lines in commits or pull requests.

## Working rules

- One purpose per branch; larger work is a feature branch with branches merged into it.
  Something found on the way is fixed there only if the branch caused it or it blocks it;
  otherwise it becomes an issue and a branch of its own.
- Blocking review findings are fixed with the smallest change; non-blocking ones become issues.
  Merge the default branch in only for a conflict or once before merging.
- Reviews are text, commits are changes: findings and the reason for a fix go into the review on
  the pull request. Before a commit exists, everything found goes into that one commit; after
  it, a new finding is one review entry and one more commit for all of it.
- Never push to `main`, never force-push; merges go through the pull request once CI and the
  review pass.

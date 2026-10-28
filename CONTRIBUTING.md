# Contributing

## Build and check

CI uses Rust 1.99.1; the minimum supported Rust version in `Cargo.toml` is 1.90. Build with `cargo build --workspace` and run the checks with:

```bash
make check
```

Besides Rust, `make check` needs Docker, `kubectl` (or `kustomize`) for `scripts/check-deploy.sh`, and network access. The deployment check downloads kubeconform v0.8.0 if it is not installed, plus the Kubernetes, k3d and CRD schemas and the ECK files. `cargo-deny` is optional; `make check` runs it when installed. See [AGENTS.md](AGENTS.md#commands-ci-runs) for the full list.

Enable the repository hooks with:

```bash
git config core.hooksPath .githooks
```

Hooks and scripts must be stored as 100755. Git for Windows does not set this mode; for a new hook or script, run `git update-index --chmod=+x <path>`. `scripts/check-modes.sh` fails without it.

The forbidden-word list lives outside the repository. The scrub check skips when the list is absent, as it is for outside contributors and CI.

## Changes and review

Use one purpose per branch. For larger work, use a feature branch with branches merged into it. Unrelated findings should become separate issues and branches unless the branch caused them or they block the work. Open a pull request; never push to `main` or force-push. Merge through the pull request once CI and review pass.

Before pushing, fix every defect, security issue, broken house rule and missing test in code the branch touches, whether blocking or not. Drop nits (wording, naming, style and "could also" suggestions), or mention them in at most one line in the pull request. Only real work outside the branch or something needing the owner's decision becomes an issue. Record findings and reasons in the pull request review. Before a commit exists, include all findings in that commit; after it, use one review entry and one further commit for the findings. Merge the default branch only for a conflict or once before merging. See [AGENTS.md](AGENTS.md#working-rules) for the full rules.

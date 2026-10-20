---
name: check
description: Run what CI runs for people-index (fmt, clippy, tests, deploy manifests, scrub, file modes) and report each result.
---

Run each from the repository root and report pass or fail, with the first error of any failure:

1. `cargo fmt --all -- --check`
2. `cargo clippy --workspace --all-targets --locked -- -D warnings`
3. `cargo test --workspace --locked`
4. `scripts/check-deploy.sh`
5. `scripts/scrub-check.sh`
6. `scripts/test-scrub-check.sh`
7. `scripts/test-check-republish.sh`
8. `scripts/test-check-modes.sh`
9. `scripts/check-modes.sh`

Do not fix anything unless asked; say what failed and where.

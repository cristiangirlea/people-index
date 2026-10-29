# syntax=docker/dockerfile:1.27.1@sha256:4edf897a3ffa55b89f906fc8cc78afdb3f1834cc9c7083565e611a8a7d5fe99e
#
# The people-index image: one static-ish binary on a distroless base, run as non-root.
#
# Stages:
#   chef     the Rust toolchain plus cargo-chef, shared by the next two stages
#   planner  reduces the workspace to recipe.json (manifests and lockfile only), so the
#            dependency layer below is rebuilt only when a Cargo.toml or Cargo.lock changes
#   builder  compiles the dependencies from the recipe (cached layer), then the workspace
#   runtime  gcr.io/distroless/cc-debian13:nonroot: glibc, libgcc, CA certificates and nothing
#            else (no shell, no package manager), uid 65532
#
# The builder is Debian 13 (trixie) on purpose: the binary links against the builder's glibc,
# and the runtime is Debian 13 too, so the two always agree.
#
# Build: docker build -t people-index:local .   (or `make image`)
#
# Base images are pinned as name:tag@digest, so a rebuild gets the same bytes; Dependabot
# (.github/dependabot.yml) bumps tag and digest together. The Rust tag is written out rather
# than taken from an ARG because Dependabot cannot follow an ARG in FROM, and it must equal
# RUST_TOOLCHAIN in .github/workflows/ci.yml (scripts/check-deploy.sh fails otherwise).

ARG CARGO_CHEF_VERSION=0.1.78

FROM rust:1.99.1-slim-trixie@sha256:b8e062b1c3a80183df83d9ba187cadd2b5ab84bc78d453dbfeb2fccd5f982247 AS chef
ARG CARGO_CHEF_VERSION
RUN cargo install cargo-chef --version "${CARGO_CHEF_VERSION}" --locked
WORKDIR /src

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /src/recipe.json recipe.json
# The registry and git caches are BuildKit cache mounts: they survive between builds on the same
# host without becoming part of a layer. target/ is deliberately not a cache mount, because
# cargo-chef's whole point is that the compiled dependencies live in this cached layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo chef cook --release --locked --recipe-path recipe.json --package people-index
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    cargo build --release --locked --package people-index --bin people-index \
    && install -D -m 0755 target/release/people-index /out/people-index

FROM gcr.io/distroless/cc-debian13:nonroot@sha256:3ee00291cf386ec52a552e6a858d555debd74776eb4bfa53fa3df2cd6677515e AS runtime
LABEL org.opencontainers.image.title="people-index" \
      org.opencontainers.image.description="Staged, multi-node person and organization join loaded into Elasticsearch" \
      org.opencontainers.image.source="https://github.com/cristiangirlea/people-index" \
      org.opencontainers.image.licenses="MIT"
COPY --from=builder /out/people-index /usr/local/bin/people-index
USER 65532:65532
# PI_METRICS_ADDR defaults to 0.0.0.0:9464 (Prometheus text format).
EXPOSE 9464
ENTRYPOINT ["/usr/local/bin/people-index"]

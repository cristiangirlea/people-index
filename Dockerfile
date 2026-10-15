# syntax=docker/dockerfile:1
#
# The people-index image: one static-ish binary on a distroless base, run as non-root.
#
# Stages:
#   chef     the Rust toolchain plus cargo-chef, shared by the next two stages
#   planner  reduces the workspace to recipe.json (manifests and lockfile only), so the
#            dependency layer below is rebuilt only when a Cargo.toml or Cargo.lock changes
#   builder  compiles the dependencies from the recipe (cached layer), then the workspace
#   runtime  gcr.io/distroless/cc-debian12:nonroot: glibc, libgcc, CA certificates and nothing
#            else (no shell, no package manager), uid 65532
#
# The builder is Debian 12 (bookworm) on purpose: the binary links against the builder's glibc,
# and the runtime is Debian 12 too, so the two always agree.
#
# Build: docker build -t people-index:local .   (or `make image`)

ARG RUST_VERSION=1.98
ARG CARGO_CHEF_VERSION=0.1.78

FROM rust:${RUST_VERSION}-slim-bookworm AS chef
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

FROM gcr.io/distroless/cc-debian12:nonroot AS runtime
LABEL org.opencontainers.image.title="people-index" \
      org.opencontainers.image.description="Staged, multi-node person and organization join loaded into Elasticsearch" \
      org.opencontainers.image.source="https://github.com/cristiangirlea/people-index" \
      org.opencontainers.image.licenses="MIT"
COPY --from=builder /out/people-index /usr/local/bin/people-index
USER 65532:65532
# PI_METRICS_ADDR defaults to 0.0.0.0:9464 (Prometheus text format).
EXPOSE 9464
ENTRYPOINT ["/usr/local/bin/people-index"]

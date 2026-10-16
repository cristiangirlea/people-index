# people-index: local workflows. `make help` lists the targets; docs/OPERATIONS.md explains them.
#
# Two ways to run the whole system on one machine:
#   compose  make image compose-run                     (Docker Compose, deploy/compose)
#   k3d      make k3d-up eck image image-import deploy-local datagen run verify
#
# Every kubectl call names the context explicitly (KUBE_CONTEXT): this Makefile never acts on
# whatever context happens to be current.

SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help
MAKEFLAGS += --no-builtin-rules --warn-undefined-variables

IMAGE        ?= people-index:local
CLUSTER      ?= people-index
KUBE_CONTEXT ?= k3d-people-index
NAMESPACE    ?= people-index
OVERLAY      ?= local
RUN_TIMEOUT  ?= 7200

# One run id per make invocation, evaluated once (a recursive `?=` would call date again on
# every expansion and could straddle a second). Pass RUN_ID=<id> to resume or inspect a run.
# Lowercase digits and dashes: it ends up in an index name and in Kubernetes object names.
ifeq ($(origin RUN_ID),undefined)
RUN_ID := $(shell date -u +%Y%m%d-%H%M%S)
RUN_ID_GIVEN := no
else
RUN_ID_GIVEN := yes
endif

# Sizes are not defaulted here, so each deployment keeps its own defaults (compose.yaml for
# compose, deploy/k8s/overlays/<overlay>/jobs.vars for k8s). Set one to override both, e.g.
# `make compose-run WORKERS=6` or `WORKERS=6 make run`.
ifeq ($(origin WORKERS),undefined)
COMPOSE_WORKERS := 3
else
COMPOSE_WORKERS := $(WORKERS)
export WORKERS
endif

# The datagen sizes have compose names (DATAGEN_*); render.sh reads the same values as
# PERSONS, ORGANIZATIONS, FILES and SEED. Map only what was given.
datagen-env = $(if $(filter-out undefined,$(origin DATAGEN_$(1))),$(1)=$(DATAGEN_$(1)))
K8S_DATAGEN_ENV := $(strip $(call datagen-env,PERSONS) $(call datagen-env,ORGANIZATIONS) \
  $(call datagen-env,FILES) $(call datagen-env,SEED))

COMPOSE := docker compose -f deploy/compose/compose.yaml
COMPOSE_ALL := $(COMPOSE) --profile datagen --profile verify --profile metrics
KUBECTL := kubectl --context $(KUBE_CONTEXT)
RENDER := bash deploy/k8s/jobs/render.sh
JOBS_OUT := target/jobs
ECK_DIR := target/eck
SECRETS_DIR := deploy/k8s/overlays/$(OVERLAY)/secrets

# wait-job <job name>: block until the Job is Complete (exit 0) or Failed (exit 1), or until
# RUN_TIMEOUT seconds pass. kubectl wait can only wait for one condition, hence the loop.
define wait-job
deadline=$$((SECONDS + $(RUN_TIMEOUT))); \
while :; do \
  state="$$($(KUBECTL) -n $(NAMESPACE) get job $(1) -o jsonpath='{range .status.conditions[?(@.status=="True")]}{.type}{" "}{end}')"; \
  case " $$state " in \
    *" Complete "*) echo "job $(1): complete"; break ;; \
    *" Failed "*) echo "job $(1): failed" >&2; $(KUBECTL) -n $(NAMESPACE) logs job/$(1) --tail=50 >&2 || true; exit 1 ;; \
  esac; \
  if (( SECONDS >= deadline )); then echo "job $(1): timed out after $(RUN_TIMEOUT)s" >&2; exit 1; fi; \
  sleep 5; \
done
endef

# wait-oneshot <service...>: compose's `up --wait` treats a finished one-shot as a failure and
# `compose wait` ignores stopped containers, so read each container's exit code with docker wait.
define wait-oneshot
for svc in $(1); do \
  id="$$($(COMPOSE) ps -aq $$svc)"; \
  [[ -n "$$id" ]] || { echo "$$svc: no container" >&2; exit 1; }; \
  code="$$(docker wait $$id)"; \
  if [[ "$$code" != 0 ]]; then echo "$$svc exited $$code" >&2; $(COMPOSE) logs --no-log-prefix $$svc >&2; exit 1; fi; \
  echo "$$svc: done"; \
done
endef

define require-run-id
[[ "$(RUN_ID_GIVEN)" == yes ]] || { echo "set RUN_ID=<id> ($(1))" >&2; exit 1; }
endef

.PHONY: help
help: ## List the targets
	@awk 'BEGIN {FS = ":.*## "} \
	  /^##@ / {printf "\n%s\n", substr($$0, 5)} \
	  /^[a-zA-Z0-9_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

##@ Checks

.PHONY: check
check: ## Run what CI runs: fmt, clippy, tests, deploy manifests, scrub (and cargo-deny if installed)
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets --locked -- -D warnings
	cargo test --workspace --locked
	bash scripts/check-deploy.sh
	bash scripts/scrub-check.sh
	bash scripts/test-scrub-check.sh
	@if command -v cargo-deny >/dev/null; then cargo deny check; else echo "cargo-deny not installed; skipped (CI runs it)"; fi

.PHONY: scrub
scrub: ## Fail on a forbidden word in tracked files or commit messages
	bash scripts/scrub-check.sh

.PHONY: image
image: ## Build the people-index:local image
	docker build -t $(IMAGE) .

##@ Docker Compose (deploy/compose)

.PHONY: compose-up
compose-up: ## Start Elasticsearch (3 nodes) and the S3 store, create the users and the bucket
	$(COMPOSE) up -d es-users s3-init
	@$(call wait-oneshot,certs es-users s3-init)
	@echo "Elasticsearch: https://localhost:$${ES_PORT:-9200} (CA: make compose-ca)  S3: http://localhost:$${S3_PORT:-8333}"

.PHONY: compose-datagen
compose-datagen: compose-up ## Generate the feeds into s3://people-index/raw/
	$(COMPOSE) --profile datagen run --rm datagen

.PHONY: compose-pipeline
compose-pipeline: compose-up ## Run the coordinator and the workers for RUN_ID; exit with the run's status
	@echo "run $(RUN_ID) with $(COMPOSE_WORKERS) workers"
	@# --no-deps: compose-up already settled the dependencies, and compose would otherwise
	@# rerun them. A new RUN_ID changes the containers' config, so compose recreates them; the
	@# same RUN_ID (a resume) restarts the stopped containers as they were.
	PI_RUN_ID=$(RUN_ID) $(COMPOSE) up -d --no-deps --scale worker=$(COMPOSE_WORKERS) coordinator worker
	@code="$$(docker wait "$$($(COMPOSE) ps -aq coordinator)")"; \
	if [[ "$$code" != 0 ]]; then \
	  echo "run $(RUN_ID) failed (coordinator exit $$code); resume with: make compose-resume RUN_ID=$(RUN_ID)" >&2; \
	  $(COMPOSE) logs --no-log-prefix --tail=50 coordinator >&2; exit 1; \
	fi; \
	echo "run $(RUN_ID) published"

.PHONY: compose-resume
compose-resume: ## Resume a failed run from its commits: make compose-resume RUN_ID=<id>
	@$(call require-run-id,the failed run; make compose-runs lists them)
	$(MAKE) compose-pipeline RUN_ID=$(RUN_ID)

.PHONY: compose-verify
compose-verify: ## Acceptance queries and content digest against the alias
	$(COMPOSE) --profile verify run --rm verify

.PHONY: compose-run
compose-run: ## Datagen, a full distributed run, then verify (what CI's e2e job does)
	$(MAKE) compose-datagen
	$(MAKE) compose-pipeline RUN_ID=$(RUN_ID)
	$(MAKE) compose-verify

.PHONY: compose-scale
compose-scale: ## Change the worker count of a running run: make compose-scale RUN_ID=<id> WORKERS=6
	@$(call require-run-id,the running run)
	PI_RUN_ID=$(RUN_ID) $(COMPOSE) up -d --no-deps --no-recreate --scale worker=$(COMPOSE_WORKERS) worker

.PHONY: compose-runs
compose-runs: ## List the runs in the store, and print one run's summary with RUN_ID=<id>
	@if [[ "$(RUN_ID_GIVEN)" == yes ]]; then \
	  $(COMPOSE) run --rm --no-deps --entrypoint aws s3-init --endpoint-url http://s3:8333 s3 cp s3://people-index/runs/$(RUN_ID)/summary.json -; \
	else \
	  $(COMPOSE) run --rm --no-deps --entrypoint aws s3-init --endpoint-url http://s3:8333 s3 ls s3://people-index/runs/; \
	fi

.PHONY: compose-metrics
compose-metrics: ## Start Prometheus on http://localhost:9090 (coordinator, workers, store)
	$(COMPOSE) --profile metrics up -d prometheus

.PHONY: compose-ca
compose-ca: ## Copy the cluster's CA certificate to target/es-ca.crt (for curl from the host)
	@mkdir -p target
	docker cp "$$($(COMPOSE) ps -aq es01)":/usr/share/elasticsearch/config/certs/ca/ca.crt target/es-ca.crt

.PHONY: compose-logs
compose-logs: ## Print the logs of every compose service
	$(COMPOSE_ALL) logs --no-color --timestamps

.PHONY: compose-down
compose-down: ## Stop and remove the containers; keep the data volumes
	$(COMPOSE_ALL) down --remove-orphans

.PHONY: compose-clean
compose-clean: ## Remove the containers and every volume (Elasticsearch data, store, certificates)
	$(COMPOSE_ALL) down --volumes --remove-orphans

##@ Kubernetes on k3d (deploy/k3d, deploy/k8s)

.PHONY: k3d-up
k3d-up: ## Create the k3d cluster: 1 server, 3 agents in zones a/b/c
	@if k3d cluster get $(CLUSTER) >/dev/null 2>&1; then echo "cluster $(CLUSTER) exists"; \
	else k3d cluster create --config deploy/k3d/cluster.yaml; fi
	$(KUBECTL) get nodes -L topology.kubernetes.io/zone

.PHONY: k3d-down
k3d-down: ## Delete the k3d cluster and everything in it
	k3d cluster delete $(CLUSTER)

.PHONY: eck
eck: ## Download the ECK operator and CRDs, check them against deploy/k8s/eck/SHA256SUMS, apply them
	@# fetch.sh leaves only files that match SHA256SUMS in $(ECK_DIR) and fails otherwise, so
	@# nothing unverified is applied. Server-side: the CRDs are too large for client-side apply.
	bash deploy/k8s/eck/fetch.sh $(ECK_DIR)
	$(KUBECTL) apply --server-side -f $(ECK_DIR)/crds.yaml -f $(ECK_DIR)/operator.yaml
	$(KUBECTL) -n elastic-system rollout status statefulset/elastic-operator --timeout=5m

.PHONY: image-import
image-import: ## Load people-index:local into the k3d nodes
	k3d image import $(IMAGE) -c $(CLUSTER)

.PHONY: secrets
secrets: ## Create the overlay's git-ignored credential files with random values, if missing
	@if [[ -f $(SECRETS_DIR)/pipeline-es.env && -f $(SECRETS_DIR)/s3.env ]]; then \
	  echo "$(SECRETS_DIR): credentials exist"; \
	else bash deploy/k8s/gen-secrets.sh $(OVERLAY); fi

.PHONY: deploy-local
deploy-local: secrets ## Apply the overlay (Elasticsearch, S3 store, RBAC, config) and wait until ready
	$(KUBECTL) apply -k deploy/k8s/overlays/$(OVERLAY)
	$(KUBECTL) -n $(NAMESPACE) rollout status statefulset/seaweedfs --timeout=5m
	$(KUBECTL) -n $(NAMESPACE) wait job/s3-bucket-init --for=condition=complete --timeout=5m
	$(KUBECTL) -n $(NAMESPACE) wait elasticsearch/es --for=jsonpath='{.status.health}'=green --timeout=15m

.PHONY: datagen
datagen: ## Generate the feeds into s3://people-index/raw/ with a Job (DATAGEN_PERSONS=... to size)
	@mkdir -p $(JOBS_OUT)
	$(K8S_DATAGEN_ENV) JOB_ID=$(RUN_ID) $(RENDER) datagen --overlay $(OVERLAY) > $(JOBS_OUT)/datagen-$(RUN_ID).yaml
	$(KUBECTL) create -f $(JOBS_OUT)/datagen-$(RUN_ID).yaml
	@$(call wait-job,pi-datagen-$(RUN_ID))

.PHONY: run
run: ## Start run RUN_ID (coordinator Job + worker Job) and wait for it to publish
	@mkdir -p $(JOBS_OUT)
	RUN_ID=$(RUN_ID) $(RENDER) run --overlay $(OVERLAY) > $(JOBS_OUT)/run-$(RUN_ID).yaml
	$(KUBECTL) create -f $(JOBS_OUT)/run-$(RUN_ID).yaml
	@echo "run $(RUN_ID): follow with make logs RUN_ID=$(RUN_ID)"
	@$(call wait-job,pi-run-$(RUN_ID))

.PHONY: resume
resume: ## Resume a failed run from its commits: make resume RUN_ID=<id> (replaces its Jobs)
	@$(call require-run-id,the failed run; make logs lists the Jobs)
	@# Job names are fixed per run id, so the failed Jobs go first. Committed tasks stay
	@# committed in the store; only what had not committed runs again.
	$(KUBECTL) -n $(NAMESPACE) delete job pi-run-$(RUN_ID) pi-worker-$(RUN_ID) --ignore-not-found
	$(MAKE) run RUN_ID=$(RUN_ID)

.PHONY: verify
verify: ## Acceptance queries and content digest against the alias, as a Job
	@mkdir -p $(JOBS_OUT)
	JOB_ID=$(RUN_ID) $(RENDER) verify --overlay $(OVERLAY) > $(JOBS_OUT)/verify-$(RUN_ID).yaml
	$(KUBECTL) create -f $(JOBS_OUT)/verify-$(RUN_ID).yaml
	@$(call wait-job,pi-verify-$(RUN_ID))
	$(KUBECTL) -n $(NAMESPACE) logs job/pi-verify-$(RUN_ID)

.PHONY: logs
logs: ## Follow the coordinator of RUN_ID, or list the Jobs without RUN_ID
	@if [[ "$(RUN_ID_GIVEN)" == yes ]]; then \
	  $(KUBECTL) -n $(NAMESPACE) logs -f job/pi-run-$(RUN_ID); \
	else \
	  $(KUBECTL) -n $(NAMESPACE) get jobs -l app.kubernetes.io/name=people-index --sort-by=.metadata.creationTimestamp; \
	  echo "pass RUN_ID=<id> to follow one run"; \
	fi

.PHONY: worker-logs
worker-logs: ## The last lines of every worker pod of RUN_ID
	@$(call require-run-id,the run)
	$(KUBECTL) -n $(NAMESPACE) logs -l people-index.io/run-id=$(RUN_ID),app.kubernetes.io/component=worker --prefix --tail=100 --max-log-requests=50

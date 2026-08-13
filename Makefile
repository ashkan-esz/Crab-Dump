PROJECT := crab-dump
IMAGE ?= $(PROJECT):latest
CARGO ?= cargo
COMPOSE ?= docker compose

.DEFAULT_GOAL := help

.PHONY: help check build release test fmt fmt-check lint verify dry-run \
	docker-build docker-smoke docker-run compose-up compose-down compose-logs clean

help: ## Show available commands
	@awk 'BEGIN {FS = ":.*##"; printf "Usage: make <target>\n\nTargets:\n"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  %-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

check: ## Check the project without producing a release binary
	$(CARGO) check

build: ## Build a debug binary
	$(CARGO) build

release: ## Build the optimized release binary
	$(CARGO) build --release

test: ## Run the test suite
	$(CARGO) test

fmt: ## Format Rust source files
	$(CARGO) fmt

fmt-check: ## Check Rust formatting without changing files
	$(CARGO) fmt --check

lint: ## Run Clippy with warnings treated as errors
	$(CARGO) clippy -- -D warnings

verify: fmt-check lint test ## Run all pre-submit checks

dry-run: ## Validate configuration and pg_dump availability
	$(CARGO) run --release -- --dry-run

docker-build: ## Build the Docker image
	docker build -t $(IMAGE) .

docker-smoke: docker-build ## Verify the image contains the real CLI
	@help_output="$$(docker run --rm --entrypoint /usr/local/bin/crab-dump $(IMAGE) --help)" \
	&& case "$$help_output" in *"Usage:"*"crab-dump"*) ;; *) \
		echo "docker smoke test failed: --help did not produce crab-dump CLI output" >&2; exit 1 ;; \
	esac \
	&& docker run --rm --entrypoint /usr/local/bin/crab-dump \
		-e DATABASE_URL_0=postgresql://user:password@host.docker.internal:5432/dbname \
		-e TG_BOT_TOKEN=smoke-test-token \
		-e TG_CHAT_ID_0=smoke-test-chat \
		-e DASHBOARD_USERNAME=smoke-test-admin \
		-e DASHBOARD_PASSWORD=smoke-test-password-123 \
		-e DASHBOARD_HOST=127.0.0.1 \
		$(IMAGE) --dry-run

docker-run: ## Run a one-shot backup with Docker Compose
	$(COMPOSE) run --rm crab-dump

compose-up: ## Start the scheduled Compose service
	$(COMPOSE) up -d

compose-down: ## Stop the Compose service
	$(COMPOSE) down

compose-logs: ## Follow Compose service logs
	$(COMPOSE) logs -f crab-dump

clean: ## Remove Rust build artifacts
	$(CARGO) clean

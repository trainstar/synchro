.PHONY: \
	help \
	build \
	build-check \
	run \
	db-init \
	db-prepare-server \
	db-reset \
	db-clean-slots \
	lint-go \
	lint-rust \
	lint-rn \
	test \
	test-go-unit \
	test-go-unit-race \
	test-go-integration \
	test-go-integration-race \
	test-go-wal \
	test-rust-core \
	test-rust-pg \
	test-rust-pg-all \
	test-adapter \
	test-adapter-setup \
	test-adapter-teardown \
	ext-build \
	ext-install \
	ext-test \
	ext-seed \
	test-swift \
	test-swift-unit \
	test-kotlin \
	test-kotlin-unit \
	test-kotlin-integration \
	test-rn-unit \
	test-rn-e2e-ios \
	test-rn-e2e-android \
	test-rn \
	test-integration \
	synchrod-test-start \
	synchrod-test-stop \
	synchrod-test-restart \
	synchrod-pg-test-start \
	synchrod-pg-test-stop \
	synchrod-pg-test-restart \
	release-check \
	release-swift-local \
	release-kotlin-local \
	release-npm-dry-run \
	clean \
	seed-build \
	seed-generate \
	seed-generate-example \
	seed-bundle-example

# Local infrastructure defaults.
DB_HOST ?= 10.0.0.86
DB_PORT ?= 5432
DB_NAME ?= synchro_test
DB_USER ?= postgres
DB_PASSWORD ?= postgres

# Android SDK (Homebrew install via android-commandlinetools).
ANDROID_HOME ?= /opt/homebrew/share/android-commandlinetools

# Synchrod test server defaults.
SYNCHRO_TEST_HOST ?= localhost
SYNCHRO_TEST_PORT ?= 8080
SYNCHRO_TEST_JWT_SECRET ?= test-secret-for-integration-tests
MIN_CLIENT_VERSION ?= 1.0.0
SYNCHROD_PID_FILE ?= .synchrod-test.pid
SYNCHROD_LOG_FILE ?= .synchrod-test.log

DATABASE_URL ?= postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)?sslmode=disable
REPLICATION_URL ?= postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/$(DB_NAME)?replication=database&sslmode=disable
SYNCHRO_TEST_URL ?= http://$(SYNCHRO_TEST_HOST):$(SYNCHRO_TEST_PORT)

TEST_ENV = \
	TEST_DATABASE_URL="$(DATABASE_URL)" \
	TEST_REPLICATION_URL="$(REPLICATION_URL)" \
	SYNCHRO_TEST_URL="$(SYNCHRO_TEST_URL)" \
	SYNCHRO_TEST_JWT_SECRET="$(SYNCHRO_TEST_JWT_SECRET)"

help:
	@echo "Available targets:"
	@echo "  build                 - Build the synchrod binary"
	@echo "  build-check           - Build library and cross-compile smoke test"
	@echo "  run                   - Run synchrod locally with current env"
	@echo "  db-init               - Create the test database if missing"
	@echo "  db-prepare-server     - Reset the shared synchrod test DB and install test schema"
	@echo "  db-reset              - Drop and recreate the test database"
	@echo "  db-clean-slots        - Drop inactive replication slots in the test DB"
	@echo "  lint-go               - Run Go linters and license check"
	@echo "  lint-rn               - Run React Native typecheck and ESLint"
	@echo "  test                  - Run local default validation (Go unit + Swift + Kotlin unit tests)"
	@echo "  test-go-unit          - Run Go tests without integration env"
	@echo "  test-go-unit-race     - Run Go tests with race detector"
	@echo "  test-go-integration   - Run Go integration tests against the configured test DB"
	@echo "  test-go-integration-race - Run Go integration tests with race detector"
	@echo "  test-go-wal           - Run Go WAL/replication tests against the configured test DB"
	@echo "  synchrod-test-start   - Start the synchrod integration server in the background"
	@echo "  synchrod-test-stop    - Stop the synchrod integration server"
	@echo "  synchrod-test-restart - Restart the synchrod integration server"
	@echo "  test-swift-unit       - Run Swift tests"
	@echo "  test-swift            - Run Swift tests with integration env wired"
	@echo "  test-kotlin-unit      - Run Kotlin unit tests"
	@echo "  test-kotlin           - Run Kotlin tests with integration env wired"
	@echo "  test-kotlin-integration - Alias of test-kotlin"
	@echo "  test-rn-unit          - Run React Native Jest unit tests"
	@echo "  test-rn-e2e-ios       - Run React Native Detox E2E tests on iOS"
	@echo "  test-rn-e2e-android   - Run React Native Detox E2E tests on Android"
	@echo "  test-rn               - Run React Native E2E tests on both platforms"
	@echo "  test-integration      - Run Go integration + WAL + Swift + Kotlin + RN integration against a live synchrod"
	@echo "  release-check         - Run all SDK unit tests locally"
	@echo "  release-swift-local   - Dry-run subtree split for Swift SDK"
	@echo "  release-kotlin-local  - Publish Kotlin SDK to mavenLocal"
	@echo "  release-npm-dry-run   - Dry-run npm pack for React Native SDK"
	@echo "  seed-build            - Build the synchroseed CLI tool"
	@echo "  seed-generate         - Generate a seed database from the test server"
	@echo "  seed-generate-example - Generate a seed database into the RN example app"
	@echo "  seed-bundle-example   - Generate and copy seed into both iOS and Android example apps"
	@echo "  clean                 - Remove local build/test artifacts"
	@echo ""
	@echo "Extension targets:"
	@echo "  ext-build             - Build the synchro_pg extension"
	@echo "  ext-install           - Install extension into local PG 18"
	@echo "  ext-test              - Run pgrx integration tests (PG 18)"
	@echo "  ext-seed              - Generate TPC-H test seed data"
	@echo "  test-rust-core        - Run synchro-core unit tests"
	@echo "  test-rust-pg          - Run pgrx integration tests (PG 18)"
	@echo "  test-rust-pg-all      - Run pgrx tests on PG 14-18"
	@echo "  test-adapter          - Run Go adapter integration tests"
	@echo "  test-adapter-setup    - Set up PG for adapter tests"
	@echo "  test-adapter-teardown - Tear down adapter test PG"
	@echo "  lint-rust             - Run Rust linters (fmt + clippy)"
	@echo "  synchrod-pg-test-start   - Start extension-backed test server"
	@echo "  synchrod-pg-test-stop    - Stop extension-backed test server"
	@echo "  synchrod-pg-test-restart - Restart extension-backed test server"

build:
	go build -o bin/synchrod ./cmd/synchrod

build-check:
	go build ./...
	GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build ./cmd/synchrod

run:
	go run ./cmd/synchrod

db-init:
	@echo "Checking/creating $(DB_NAME) on $(DB_HOST):$(DB_PORT)..."
	@psql "postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -tc \
		"SELECT 1 FROM pg_database WHERE datname = '$(DB_NAME)'" | grep -q 1 || \
		psql "postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -c \
		"CREATE DATABASE $(DB_NAME);"
	@echo "Database ready: $(DATABASE_URL)"

db-prepare-server: db-init
	@echo "Preparing shared synchrod test database..."
	@DATABASE_URL="$(DATABASE_URL)" go run ./cmd/synchrotestdb

db-reset:
	@echo "Resetting $(DB_NAME) on $(DB_HOST):$(DB_PORT)..."
	@psql "postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -c \
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$(DB_NAME)' AND pid <> pg_backend_pid();" >/dev/null 2>&1 || true
	@psql "postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -c "DROP DATABASE IF EXISTS $(DB_NAME);"
	@psql "postgres://$(DB_USER):$(DB_PASSWORD)@$(DB_HOST):$(DB_PORT)/postgres" -c "CREATE DATABASE $(DB_NAME);"
	@echo "Database reset: $(DATABASE_URL)"

db-clean-slots:
	@echo "Dropping inactive replication slots on $(DB_HOST):$(DB_PORT)..."
	@psql "$(DATABASE_URL)" -c \
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE active = false;" 2>/dev/null || true
	@echo "Done."

lint-go:
	golangci-lint run
	go-licenses check ./... --allowed_licenses=MIT,BSD-2-Clause,BSD-3-Clause,Apache-2.0,ISC,MPL-2.0,CC0-1.0 --ignore modernc.org/mathutil

lint-rn:
	cd clients/react-native && yarn typecheck
	cd clients/react-native && yarn lint

test: test-go-unit test-swift-unit test-kotlin-unit test-rn-unit

test-go-unit:
	go test ./...

test-go-unit-race:
	go test -race ./...

test-go-integration:
	$(TEST_ENV) go test -count=1 -p 1 ./...

test-go-integration-race:
	$(TEST_ENV) go test -race -count=1 -p 1 ./...

test-go-wal:
	$(TEST_ENV) go test -count=1 -p 1 -v ./...

synchrod-test-start: db-init
	@set -e; \
	if [ -f "$(SYNCHROD_PID_FILE)" ] && kill -0 "$$(cat "$(SYNCHROD_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod test server already running on $(SYNCHRO_TEST_URL)"; \
		exit 0; \
	fi; \
	EXISTING_PID="$$(lsof -tiTCP:$(SYNCHRO_TEST_PORT) -sTCP:LISTEN 2>/dev/null | head -n 1)"; \
	if [ -n "$$EXISTING_PID" ]; then \
		EXISTING_CMD="$$(ps -p "$$EXISTING_PID" -o comm= 2>/dev/null | tr -d ' ')"; \
		EXISTING_BASE="$$(basename "$$EXISTING_CMD")"; \
		if [ "$$EXISTING_BASE" = "synchrod" ]; then \
			echo "Reusing existing synchrod on $(SYNCHRO_TEST_URL) (pid $$EXISTING_PID)"; \
			echo "$$EXISTING_PID" >"$(SYNCHROD_PID_FILE)"; \
			exit 0; \
		fi; \
		echo "Port $(SYNCHRO_TEST_PORT) is already in use by $$EXISTING_CMD (pid $$EXISTING_PID)"; \
		exit 1; \
	fi; \
	rm -f "$(SYNCHROD_PID_FILE)"; \
	echo "Starting synchrod test server on $(SYNCHRO_TEST_URL)..."; \
	nohup env \
	JWT_SECRET="$(SYNCHRO_TEST_JWT_SECRET)" \
	MIN_CLIENT_VERSION="$(MIN_CLIENT_VERSION)" \
	DATABASE_URL="$(DATABASE_URL)" \
	REPLICATION_URL="$(REPLICATION_URL)" \
	go run ./cmd/synchrod >"$(SYNCHROD_LOG_FILE)" 2>&1 </dev/null & echo $$! >"$(SYNCHROD_PID_FILE)"; \
	sleep 2; \
	if ! kill -0 "$$(cat "$(SYNCHROD_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod failed to start; log follows:"; \
		test -f "$(SYNCHROD_LOG_FILE)" && cat "$(SYNCHROD_LOG_FILE)"; \
		rm -f "$(SYNCHROD_PID_FILE)"; \
		exit 1; \
	fi; \
	echo "synchrod test server running on $(SYNCHRO_TEST_URL)"

synchrod-test-stop:
	@STOPPED=0; \
	if [ -f "$(SYNCHROD_PID_FILE)" ]; then \
		PID="$$(cat "$(SYNCHROD_PID_FILE)")"; \
		if kill -0 "$$PID" 2>/dev/null; then \
			kill "$$PID"; \
			wait "$$PID" 2>/dev/null || true; \
			STOPPED=1; \
		fi; \
		rm -f "$(SYNCHROD_PID_FILE)"; \
	fi; \
	for PID in $$(lsof -tiTCP:$(SYNCHRO_TEST_PORT) -sTCP:LISTEN 2>/dev/null); do \
		CMD="$$(ps -p "$$PID" -o comm= 2>/dev/null | tr -d ' ')"; \
		BASE="$$(basename "$$CMD")"; \
		if [ "$$BASE" = "synchrod" ]; then \
			kill "$$PID"; \
			wait "$$PID" 2>/dev/null || true; \
			STOPPED=1; \
		fi; \
	done; \
	if [ "$$STOPPED" -eq 1 ]; then \
		echo "synchrod stopped"; \
	else \
		echo "synchrod not running"; \
	fi

synchrod-test-restart: synchrod-test-stop db-prepare-server
	@$(MAKE) synchrod-test-start

test-swift-unit:
	cd clients/swift && swift test

test-swift: synchrod-test-restart
	cd clients/swift && $(TEST_ENV) swift test

test-kotlin-unit:
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ./gradlew :synchro:test

test-kotlin: synchrod-test-restart
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" $(TEST_ENV) ./gradlew :synchro:test

test-kotlin-integration: test-kotlin

test-rn-unit:
	cd clients/react-native && npm test -- --testPathIgnorePatterns=e2e __mocks__

test-rn-e2e-ios: synchrod-test-restart
	cd clients/react-native/example && \
		$(TEST_ENV) npx detox test --configuration ios.sim.debug

test-rn-e2e-android: synchrod-test-restart
	cd clients/react-native/example && \
		$(TEST_ENV) npx detox test --configuration android.emu.debug

test-rn: test-rn-e2e-ios test-rn-e2e-android

test-integration:
	$(TEST_ENV) $(MAKE) test-go-integration
	$(TEST_ENV) $(MAKE) test-go-wal
	$(TEST_ENV) $(MAKE) test-swift
	$(TEST_ENV) $(MAKE) test-kotlin
	$(TEST_ENV) $(MAKE) test-rn

release-check: test-go-unit test-swift-unit test-kotlin-unit test-rn-unit
	@echo "All SDK tests passed."

release-swift-local:
	git subtree split --prefix=clients/swift -b swift-sdk
	@echo "Branch 'swift-sdk' created. Verify Package.swift is at root:"
	@git show swift-sdk:Package.swift | head -5

release-kotlin-local:
	cd clients/kotlin && ./gradlew :synchro:publishToMavenLocal
	@echo "Published to mavenLocal. Check:"
	@ls -la ~/.m2/repository/com/trainstar/synchro/ 2>/dev/null || echo "Not found in ~/.m2"

release-npm-dry-run:
	cd clients/react-native && npm pack --dry-run

seed-build:
	go build -o bin/synchroseed ./cmd/synchroseed

seed-generate: seed-build
	bin/synchroseed -server=$(SYNCHRO_TEST_URL) -jwt-secret=$(SYNCHRO_TEST_JWT_SECRET) -output=seed.db

SEED_OUTPUT ?= clients/react-native/example/seed.db

seed-generate-example: seed-build
	bin/synchroseed -server=$(SYNCHRO_TEST_URL) -jwt-secret=$(SYNCHRO_TEST_JWT_SECRET) -output=$(SEED_OUTPUT)

seed-bundle-example: seed-generate-example
	mkdir -p clients/react-native/example/android/app/src/main/assets
	cp $(SEED_OUTPUT) clients/react-native/example/android/app/src/main/assets/seed.db

# ---------------------------------------------------------------------------
# Extension targets
# ---------------------------------------------------------------------------

PGRX_PG ?= pg18
PGRX_PORT ?= 28818
ADAPTER_TEST_DB ?= postgres
ADAPTER_TEST_URL ?= postgres://$(USER)@localhost:$(PGRX_PORT)/$(ADAPTER_TEST_DB)?sslmode=disable
SYNCHROD_PG_PID_FILE ?= .synchrod-pg-test.pid
SYNCHROD_PG_LOG_FILE ?= .synchrod-pg-test.log
SYNCHROD_PG_PORT ?= 8081

ext-build:
	cd extensions/synchro-pg && cargo build

ext-install:
	cd extensions/synchro-pg && cargo pgrx install

ext-test: test-rust-pg

ext-seed:
	python3 extensions/testdata/generate/generate.py

test-rust-core:
	cd extensions && cargo test -p synchro-core

test-rust-pg:
	cd extensions/synchro-pg && cargo pgrx test $(PGRX_PG)

test-rust-pg-all:
	@for v in 14 15 16 17 18; do \
		echo "=== PG $$v ==="; \
		cd extensions/synchro-pg && cargo pgrx test pg$$v || exit 1; \
		cd ../..; \
	done
	@echo "All PG versions passed."

lint-rust:
	cd extensions && cargo fmt --check
	cd extensions && cargo clippy -- -D warnings

# Adapter test lifecycle: start pgrx PG, create DB, install extension, run tests, stop.
test-adapter-setup:
	@echo "Setting up adapter test database..."
	@# Ensure wal_level=logical and bgworker enabled for E2E testing.
	@grep -q "^wal_level = logical" ~/.pgrx/data-18/postgresql.conf 2>/dev/null || \
		echo "wal_level = logical" >> ~/.pgrx/data-18/postgresql.conf
	@sed -i 's/^synchro.auto_start = off/synchro.auto_start = on/' ~/.pgrx/data-18/postgresql.conf 2>/dev/null || true
	@sed -i "s/^synchro.database = .*/synchro.database = '$(ADAPTER_TEST_DB)'/" ~/.pgrx/data-18/postgresql.conf 2>/dev/null || \
		echo "synchro.database = '$(ADAPTER_TEST_DB)'" >> ~/.pgrx/data-18/postgresql.conf
	@cd extensions/synchro-pg && cargo pgrx start $(PGRX_PG)
	@if [ "$(ADAPTER_TEST_DB)" != "postgres" ]; then \
		psql -h localhost -p $(PGRX_PORT) -U $(USER) -d postgres -c \
			"DROP DATABASE IF EXISTS $(ADAPTER_TEST_DB)" 2>/dev/null || true; \
		psql -h localhost -p $(PGRX_PORT) -U $(USER) -d postgres -c \
			"CREATE DATABASE $(ADAPTER_TEST_DB)"; \
	fi
	@psql -h localhost -p $(PGRX_PORT) -U $(USER) -d $(ADAPTER_TEST_DB) -c \
		"CREATE EXTENSION IF NOT EXISTS synchro_pg CASCADE" 2>/dev/null || true
	@echo "Adapter test database ready: $(ADAPTER_TEST_URL)"

test-adapter-teardown-restore:
	@# Restore auto_start = off for pgrx unit tests.
	@sed -i 's/^synchro.auto_start = on/synchro.auto_start = off/' ~/.pgrx/data-18/postgresql.conf 2>/dev/null || true

test-adapter-teardown:
	@echo "Tearing down adapter test environment..."
	@if [ "$(ADAPTER_TEST_DB)" != "postgres" ]; then \
		psql -h localhost -p $(PGRX_PORT) -U $(USER) -d postgres -c \
			"DROP DATABASE IF EXISTS $(ADAPTER_TEST_DB)" 2>/dev/null || true; \
	fi
	@cd extensions/synchro-pg && cargo pgrx stop $(PGRX_PG) 2>/dev/null || true
	@echo "Done."

test-adapter: test-adapter-setup
	@echo "Running adapter integration tests..."
	cd api/go && TEST_DATABASE_URL="$(ADAPTER_TEST_URL)" go test -v -count=1 ./...
	@$(MAKE) test-adapter-teardown-restore
	@$(MAKE) test-adapter-teardown

# Extension-backed synchrod lifecycle (for client SDK tests).
synchrod-pg-test-start: test-adapter-setup
	@set -e; \
	if [ -f "$(SYNCHROD_PG_PID_FILE)" ] && kill -0 "$$(cat "$(SYNCHROD_PG_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod-pg already running"; \
		exit 0; \
	fi; \
	echo "Loading schema and registering tables..."; \
	psql -h localhost -p $(PGRX_PORT) -U $(USER) -d $(ADAPTER_TEST_DB) -f extensions/testdata/schema.sql >/dev/null 2>&1; \
	psql -h localhost -p $(PGRX_PORT) -U $(USER) -d $(ADAPTER_TEST_DB) -f extensions/testdata/register.sql >/dev/null 2>&1; \
	echo "Reloading bgworker registry..."; \
	psql -h localhost -p $(PGRX_PORT) -U $(USER) -d $(ADAPTER_TEST_DB) -c "SELECT pg_reload_conf()" >/dev/null 2>&1; \
	sleep 1; \
	if [ -f extensions/testdata/seed.sql ]; then \
		echo "Loading seed data (this may take a minute)..."; \
		psql -h localhost -p $(PGRX_PORT) -U $(USER) -d $(ADAPTER_TEST_DB) -f extensions/testdata/seed.sql >/dev/null 2>&1; \
	else \
		echo "No seed.sql found. Run 'make ext-seed' to generate TPC-H data."; \
	fi; \
	echo "Starting synchrod-pg on :$(SYNCHROD_PG_PORT)..."; \
	nohup env \
		DATABASE_URL="$(ADAPTER_TEST_URL)" \
		JWT_SECRET="$(SYNCHRO_TEST_JWT_SECRET)" \
		MIN_CLIENT_VERSION="$(MIN_CLIENT_VERSION)" \
		LISTEN_ADDR=":$(SYNCHROD_PG_PORT)" \
		go run ./cmd/synchrod-pg >"$(SYNCHROD_PG_LOG_FILE)" 2>&1 </dev/null & echo $$! >"$(SYNCHROD_PG_PID_FILE)"; \
	sleep 2; \
	if ! kill -0 "$$(cat "$(SYNCHROD_PG_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod-pg failed to start:"; \
		cat "$(SYNCHROD_PG_LOG_FILE)"; \
		rm -f "$(SYNCHROD_PG_PID_FILE)"; \
		exit 1; \
	fi; \
	echo "synchrod-pg running on http://localhost:$(SYNCHROD_PG_PORT)"

synchrod-pg-test-stop:
	@if [ -f "$(SYNCHROD_PG_PID_FILE)" ]; then \
		PID="$$(cat "$(SYNCHROD_PG_PID_FILE)")"; \
		if kill -0 "$$PID" 2>/dev/null; then \
			kill "$$PID"; \
			wait "$$PID" 2>/dev/null || true; \
			echo "synchrod-pg stopped"; \
		fi; \
		rm -f "$(SYNCHROD_PG_PID_FILE)"; \
	else \
		echo "synchrod-pg not running"; \
	fi
	@$(MAKE) test-adapter-teardown

synchrod-pg-test-restart: synchrod-pg-test-stop
	@$(MAKE) synchrod-pg-test-start

clean:
	rm -rf bin/ "$(SYNCHROD_PID_FILE)" "$(SYNCHROD_LOG_FILE)" "$(SYNCHROD_PG_PID_FILE)" "$(SYNCHROD_PG_LOG_FILE)"

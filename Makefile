.PHONY: \
	help \
	build \
	build-check \
	run \
	docs-build \
	docs-dev \
	lint-go \
	lint-rn \
	lint-rust-core \
	lint-rust-pg \
	lint-rust \
	test \
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
	test-swift-unit \
	test-swift \
	test-kotlin-unit \
	test-kotlin \
	test-kotlin-integration \
	test-rn-unit \
	rn-seed-asset \
	rn-ios-pods \
	test-rn-e2e-ios \
	test-rn-e2e-android \
	test-rn \
	synchrod-pg-test-start \
	synchrod-pg-test-stop \
	synchrod-pg-test-restart \
	release-check \
	release-kotlin-local \
	release-npm-dry-run \
	clean

ANDROID_HOME ?= /opt/homebrew/share/android-commandlinetools
ANDROID_JAVA_HOME ?= $(shell \
	if [ -d /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home ]; then \
		echo /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home; \
	elif [ -x /usr/libexec/java_home ]; then \
		CANDIDATE="$$(/usr/libexec/java_home -v 17 2>/dev/null || true)"; \
		if [ -n "$$CANDIDATE" ] && "$$CANDIDATE/bin/java" -version 2>&1 | grep -q '"17\.'; then \
			echo "$$CANDIDATE"; \
		fi; \
	fi)

PGRX_PG ?= pg18
PGRX_PORT ?= 28818
PGRX_READY_TIMEOUT ?= 90
PGRX_PG_MAJOR := $(patsubst pg%,%,$(PGRX_PG))
PGRX_DATA_DIR ?= $(HOME)/.pgrx/data-$(PGRX_PG_MAJOR)
PGRX_SOCKET_DIR ?= $(HOME)/.pgrx
PGRX_ADMIN_HOST ?= $(if $(filter Darwin,$(shell uname -s)),$(PGRX_SOCKET_DIR),localhost)
PGRX_ADMIN_USER ?= $(shell id -un)
PGRX_LOG_FILE ?= $(HOME)/.pgrx/$(PGRX_PG_MAJOR).log
PGRX_AUTOSTART ?= on
PGRX_PG_CONFIG ?= $(shell awk -F'"' '/^$(PGRX_PG)[[:space:]]*=/ { print $$2 }' $(HOME)/.pgrx/config.toml)
PGRX_PG_BIN_DIR ?= $(dir $(PGRX_PG_CONFIG))
PGRX_PSQL ?= $(PGRX_PG_BIN_DIR)psql
PGRX_TARGET_DIR ?= $(CURDIR)/.pgrx-target
ADAPTER_TEST_DB ?= synchro_adapter_test
ADAPTER_TEST_URL ?= postgres://$(USER)@localhost:$(PGRX_PORT)/$(ADAPTER_TEST_DB)?sslmode=disable
REPLICATION_URL ?= postgres://$(USER)@localhost:$(PGRX_PORT)/$(ADAPTER_TEST_DB)?replication=database&sslmode=disable

SYNCHROD_PG_PORT ?= 8081
SYNCHRO_TEST_HOST ?= localhost
SYNCHRO_TEST_PORT ?= $(SYNCHROD_PG_PORT)
SYNCHRO_TEST_URL ?= http://$(SYNCHRO_TEST_HOST):$(SYNCHRO_TEST_PORT)
SYNCHRO_TEST_JWT_SECRET ?= test-secret-for-integration-tests
MIN_CLIENT_VERSION ?= 1.0.0
SYNCHROD_PG_PID_FILE ?= .synchrod-pg-test.pid
SYNCHROD_PG_LOG_FILE ?= .synchrod-pg-test.log

BINARY ?= bin/synchrod-pg
GRADLE_TEST_ARGS ?= --rerun-tasks

TEST_ENV = \
	TEST_DATABASE_URL="$(ADAPTER_TEST_URL)" \
	TEST_REPLICATION_URL="$(REPLICATION_URL)" \
	SYNCHRO_TEST_URL="$(SYNCHRO_TEST_URL)" \
	SYNCHRO_TEST_JWT_SECRET="$(SYNCHRO_TEST_JWT_SECRET)"

help:
	@echo "Available targets:"
	@echo "  build                 - Build the synchrod-pg adapter binary"
	@echo "  build-check           - Build the Go adapter module"
	@echo "  run                   - Run synchrod-pg locally with current env"
	@echo "  docs-build            - Install docs dependencies and build the docs site"
	@echo "  docs-dev              - Run the docs site locally"
	@echo "  lint-go               - Run Go formatting checks and go vet"
	@echo "  lint-rn               - Run React Native typecheck and ESLint"
	@echo "  lint-rust-core        - Run Rust fmt and clippy for the shared core"
	@echo "  lint-rust-pg          - Run Rust fmt and clippy for the PostgreSQL extension"
	@echo "  lint-rust             - Run all Rust fmt and clippy checks"
	@echo "  test                  - Run the default local validation set"
	@echo "  test-rust-core        - Run synchro-core unit tests"
	@echo "  test-rust-pg          - Run pgrx integration tests on PG 18"
	@echo "  test-rust-pg-all      - Run pgrx tests on PG 14 through PG 18"
	@echo "  test-adapter          - Run Go adapter integration tests"
	@echo "  test-swift-unit       - Run Swift unit tests"
	@echo "  test-swift            - Run Swift integration tests against the local adapter"
	@echo "  test-kotlin-unit      - Run Kotlin unit tests"
	@echo "  test-kotlin           - Run Kotlin integration tests against the local adapter"
	@echo "  test-rn-unit          - Run React Native Jest tests"
	@echo "  test-rn-e2e-ios       - Run React Native Detox tests on iOS"
	@echo "  test-rn-e2e-android   - Run React Native Detox tests on Android"
	@echo "  test-rn               - Run React Native Detox tests on both platforms"
	@echo "  synchrod-pg-test-start   - Start the extension-backed test adapter"
	@echo "  synchrod-pg-test-stop    - Stop the extension-backed test adapter"
	@echo "  synchrod-pg-test-restart - Restart the extension-backed test adapter"
	@echo "  release-check         - Run the full release validation matrix"
	@echo "  release-kotlin-local  - Publish Kotlin SDK to mavenLocal"
	@echo "  release-npm-dry-run   - Dry-run npm pack for the React Native package"
	@echo "  clean                 - Remove local build and server artifacts"

build:
	@mkdir -p "$(dir $(BINARY))"
	cd api/go && GOWORK=off go build -o ../../$(BINARY) ./cmd/synchrod-pg

build-check:
	cd api/go && GOWORK=off go build ./...

run:
	cd api/go && GOWORK=off go run ./cmd/synchrod-pg

docs-build:
	cd docs && npm ci
	cd docs && npm run build

docs-dev:
	cd docs && npm run dev

lint-rn:
	cd clients/react-native && yarn typecheck
	cd clients/react-native && yarn lint

test: test-rust-core test-adapter test-swift-unit test-kotlin-unit test-rn-unit docs-build

test-swift-unit:
	cd clients/swift && swift test

test-swift: synchrod-pg-test-restart
	cd clients/swift && $(TEST_ENV) swift test

test-kotlin-unit:
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew $(GRADLE_TEST_ARGS) :synchro:test

test-kotlin: synchrod-pg-test-restart
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" $(TEST_ENV) ./gradlew $(GRADLE_TEST_ARGS) :synchro:test

test-kotlin-integration: test-kotlin

test-rn-unit:
	cd clients/react-native && yarn test:unit

rn-seed-asset:
	@test -f clients/react-native/example/seed.db || (echo "Missing clients/react-native/example/seed.db bundled seed asset"; exit 1)
	@mkdir -p clients/react-native/example/android/app/src/main/assets
	@if ! cmp -s clients/react-native/example/seed.db clients/react-native/example/android/app/src/main/assets/seed.db 2>/dev/null; then \
		cp clients/react-native/example/seed.db clients/react-native/example/android/app/src/main/assets/seed.db; \
	fi

rn-watchman-reset:
	@if command -v watchman >/dev/null 2>&1; then \
		watchman watch-del "$(PWD)/clients/react-native" >/dev/null 2>&1 || true; \
		watchman watch-project "$(PWD)/clients/react-native" >/dev/null; \
	fi

rn-ios-pods:
	cd clients/react-native/example/ios && \
		if [ ! -f Pods/Manifest.lock ] || [ ! -f SynchroReactNativeExample.xcworkspace/contents.xcworkspacedata ] || ! cmp -s Podfile.lock Pods/Manifest.lock || [ Podfile -nt Pods/Manifest.lock ] || [ ../../../SynchroReactNative.podspec -nt Pods/Manifest.lock ] || [ ../../../../Synchro.podspec -nt Pods/Manifest.lock ]; then \
			pod install; \
		else \
			echo "React Native iOS pods already match Podfile.lock"; \
		fi

test-rn-e2e-ios-build: rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug

test-rn-e2e-ios-run: synchrod-pg-test-restart
	cd clients/react-native/example && \
		$(TEST_ENV) npx detox test --configuration ios.sim.debug $(DETOX_ARGS)

test-rn-e2e-ios: test-rn-e2e-ios-build test-rn-e2e-ios-run

test-rn-e2e-android-build: release-kotlin-local rn-seed-asset rn-watchman-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration android.emu.debug

test-rn-e2e-android-run: synchrod-pg-test-restart
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && \
		ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
		JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
		$(TEST_ENV) npx detox test --configuration android.emu.debug $(DETOX_ARGS)

test-rn-e2e-android: test-rn-e2e-android-build test-rn-e2e-android-run

test-rn: test-rn-e2e-ios test-rn-e2e-android

release-check: lint-go lint-rust-core lint-rn test-rust-core test-rust-pg test-adapter test-swift test-kotlin test-rn docs-build
	@echo "Release validation passed."

release-kotlin-local:
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew :synchro:publishToMavenLocal
	@echo "Published to mavenLocal."

release-npm-dry-run:
	cd clients/react-native && npm pack --dry-run

ext-build:
	cd extensions/synchro-pg && cargo build

ext-install:
	cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx install --pg-config "$(PGRX_PG_CONFIG)"

ext-test: test-rust-pg

ext-seed:
	python3 extensions/testdata/generate/generate.py

test-rust-core:
	cd extensions && cargo test -p synchro-core

test-rust-pg:
	cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx test $(PGRX_PG)

test-rust-pg-all:
	@for v in 14 15 16 17 18; do \
		echo "=== PG $$v ==="; \
		cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx test pg$$v || exit 1; \
		cd ../..; \
	done
	@echo "All PG versions passed."

lint-go:
	@test -z "$$(find api/go -name '*.go' -not -path '*/vendor/*' -print0 | xargs -0 gofmt -l)"
	cd api/go && GOWORK=off go vet ./...

lint-rust-core:
	cd extensions && cargo fmt --check -p synchro-core
	cd extensions && cargo clippy -p synchro-core -- -D warnings

lint-rust-pg:
	cd extensions && cargo fmt --check -p synchro-pg
	cd extensions && cargo clippy -p synchro-pg --features pg18 -- -D warnings

lint-rust: lint-rust-core lint-rust-pg

test-adapter-setup: ext-install
	@echo "Setting up adapter test database..."
	@if [ ! -f "$(PGRX_DATA_DIR)/postgresql.conf" ]; then \
		echo "missing pgrx config: $(PGRX_DATA_DIR)/postgresql.conf"; \
		exit 1; \
	fi
	@if grep -q "^wal_level" "$(PGRX_DATA_DIR)/postgresql.conf"; then \
		perl -0pi -e "s/^wal_level\s*=.*$$/wal_level = logical/m" "$(PGRX_DATA_DIR)/postgresql.conf"; \
	else \
		printf "\nwal_level = logical\n" >> "$(PGRX_DATA_DIR)/postgresql.conf"; \
	fi
	@if grep -q "^shared_preload_libraries" "$(PGRX_DATA_DIR)/postgresql.conf"; then \
		if ! grep -q "^shared_preload_libraries.*synchro_pg" "$(PGRX_DATA_DIR)/postgresql.conf"; then \
			perl -0pi -e "s/^shared_preload_libraries\\s*=\\s*'(.*?)'\\s*$$/shared_preload_libraries = '\\1,synchro_pg'/m" \"$(PGRX_DATA_DIR)/postgresql.conf\"; \
		fi; \
	else \
		printf "\nshared_preload_libraries = 'synchro_pg'\n" >> "$(PGRX_DATA_DIR)/postgresql.conf"; \
	fi
	@if grep -q "^synchro.auto_start" "$(PGRX_DATA_DIR)/postgresql.conf"; then \
		perl -0pi -e "s/^synchro\.auto_start\s*=.*$$/synchro.auto_start = off/m" "$(PGRX_DATA_DIR)/postgresql.conf"; \
	else \
		printf "\nsynchro.auto_start = off\n" >> "$(PGRX_DATA_DIR)/postgresql.conf"; \
	fi
	@cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx start $(PGRX_PG)
	@READY=0; \
	LAST_ERR=""; \
	for attempt in $$(seq 1 $(PGRX_READY_TIMEOUT)); do \
		PROBE_OUTPUT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -Atqc "SELECT CASE WHEN pg_is_in_recovery() THEN '0' ELSE '1' END" 2>&1 || true); \
		if [ "$$PROBE_OUTPUT" = "1" ]; then \
			READY=1; \
			break; \
		fi; \
		LAST_ERR="$$PROBE_OUTPUT"; \
		sleep 1; \
	done; \
	if [ "$$READY" -ne 1 ]; then \
		echo "pgrx postgres did not become writable in $(PGRX_READY_TIMEOUT)s"; \
		if [ -n "$$LAST_ERR" ]; then echo "$$LAST_ERR"; fi; \
		if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; \
		exit 1; \
	fi
	@$(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -c "DROP DATABASE IF EXISTS $(ADAPTER_TEST_DB)" 2>/dev/null || true
	@$(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -c "CREATE DATABASE $(ADAPTER_TEST_DB)"
	@$(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -c "CREATE EXTENSION IF NOT EXISTS synchro_pg CASCADE"
	@if grep -q "^synchro.auto_start" "$(PGRX_DATA_DIR)/postgresql.conf"; then \
		perl -0pi -e "s/^synchro\.auto_start\s*=.*$$/synchro.auto_start = $(PGRX_AUTOSTART)/m" "$(PGRX_DATA_DIR)/postgresql.conf"; \
	else \
		printf "\nsynchro.auto_start = $(PGRX_AUTOSTART)\n" >> "$(PGRX_DATA_DIR)/postgresql.conf"; \
	fi
	@if grep -q "^synchro.database" "$(PGRX_DATA_DIR)/postgresql.conf"; then \
		perl -0pi -e "s/^synchro\.database\s*=.*$$/synchro.database = '$(ADAPTER_TEST_DB)'/m" "$(PGRX_DATA_DIR)/postgresql.conf"; \
	else \
		printf "\nsynchro.database = '$(ADAPTER_TEST_DB)'\n" >> "$(PGRX_DATA_DIR)/postgresql.conf"; \
	fi
	@cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx stop $(PGRX_PG)
	@cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx start $(PGRX_PG)
	@READY=0; \
	LAST_ERR=""; \
	for attempt in $$(seq 1 $(PGRX_READY_TIMEOUT)); do \
		PROBE_OUTPUT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -Atqc "SELECT CASE WHEN pg_is_in_recovery() THEN '0' ELSE '1' END" 2>&1 || true); \
		if [ "$$PROBE_OUTPUT" = "1" ]; then \
			READY=1; \
			break; \
		fi; \
		LAST_ERR="$$PROBE_OUTPUT"; \
		sleep 1; \
	done; \
	if [ "$$READY" -ne 1 ]; then \
		echo "pgrx postgres did not become writable in $(PGRX_READY_TIMEOUT)s after enabling the worker"; \
		if [ -n "$$LAST_ERR" ]; then echo "$$LAST_ERR"; fi; \
		if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; \
		exit 1; \
	fi
	@echo "Adapter test database ready: $(ADAPTER_TEST_URL)"

test-adapter-teardown:
	@echo "Tearing down adapter test database..."
	@$(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -c "DROP DATABASE IF EXISTS $(ADAPTER_TEST_DB)" 2>/dev/null || true
	@cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx stop $(PGRX_PG) 2>/dev/null || true
	@echo "Done."

test-adapter: test-adapter-setup
	@echo "Running adapter integration tests..."
	cd api/go && GOWORK=off TEST_DATABASE_URL="$(ADAPTER_TEST_URL)" go test -v -count=1 ./...
	@$(MAKE) test-adapter-teardown

synchrod-pg-test-start:
	@set -e; \
	if [ -f "$(SYNCHROD_PG_PID_FILE)" ] && kill -0 "$$(cat "$(SYNCHROD_PG_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod-pg already running"; \
		exit 0; \
	fi; \
	$(MAKE) test-adapter-setup PGRX_AUTOSTART=off; \
	echo "Loading schema and registering tables..."; \
	$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -f extensions/testdata/schema.sql || { if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; exit 1; }; \
	$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -f extensions/testdata/register.sql || { if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; exit 1; }; \
	if [ -f extensions/testdata/seed.sql ]; then \
		echo "Loading seed data (this may take a minute)..."; \
		$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -f extensions/testdata/seed.sql || { if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; exit 1; }; \
		echo "Waiting for bgworker to observe seeded rows..."; \
		for attempt in $$(seq 1 60); do \
			EDGE_COUNT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -Atqc "SELECT count(*) FROM sync_bucket_edges" 2>/dev/null || echo 0); \
			if [ "$$EDGE_COUNT" -gt 0 ] 2>/dev/null; then \
				break; \
			fi; \
			sleep 1; \
		done; \
	else \
		echo "No seed.sql found. Run 'make ext-seed' to generate test data."; \
	fi; \
	echo "Backfilling scope edges..."; \
	$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -c "SELECT synchro_backfill_bucket_edges()" || { if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; exit 1; }; \
	echo "Restarting PostgreSQL with bgworker enabled..."; \
	if grep -q "^synchro.auto_start" "$(PGRX_DATA_DIR)/postgresql.conf"; then \
		perl -0pi -e "s/^synchro\.auto_start\s*=.*$$/synchro.auto_start = on/m" "$(PGRX_DATA_DIR)/postgresql.conf"; \
	else \
		printf "\nsynchro.auto_start = on\n" >> "$(PGRX_DATA_DIR)/postgresql.conf"; \
	fi; \
	cd "$(CURDIR)/extensions/synchro-pg" && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx stop $(PGRX_PG); \
	cd "$(CURDIR)/extensions/synchro-pg" && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx start $(PGRX_PG); \
	READY=0; \
	LAST_ERR=""; \
	for attempt in $$(seq 1 $(PGRX_READY_TIMEOUT)); do \
		PROBE_OUTPUT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -Atqc "SELECT CASE WHEN pg_is_in_recovery() THEN '0' ELSE '1' END" 2>&1 || true); \
		if [ "$$PROBE_OUTPUT" = "1" ]; then \
			READY=1; \
			break; \
		fi; \
		LAST_ERR="$$PROBE_OUTPUT"; \
		sleep 1; \
	done; \
	if [ "$$READY" -ne 1 ]; then \
		echo "pgrx postgres did not become writable in $(PGRX_READY_TIMEOUT)s after re-enabling the worker"; \
		if [ -n "$$LAST_ERR" ]; then echo "$$LAST_ERR"; fi; \
		if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; \
		exit 1; \
	fi; \
	echo "Starting synchrod-pg on :$(SYNCHROD_PG_PORT)..."; \
	nohup env \
		DATABASE_URL="$(ADAPTER_TEST_URL)" \
		JWT_SECRET="$(SYNCHRO_TEST_JWT_SECRET)" \
		MIN_CLIENT_VERSION="$(MIN_CLIENT_VERSION)" \
		LISTEN_ADDR=":$(SYNCHROD_PG_PORT)" \
		sh -c 'cd "$(CURDIR)/api/go" && GOWORK=off go run ./cmd/synchrod-pg' >"$(SYNCHROD_PG_LOG_FILE)" 2>&1 </dev/null & echo $$! >"$(SYNCHROD_PG_PID_FILE)"; \
	sleep 2; \
	if ! kill -0 "$$(cat "$(SYNCHROD_PG_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod-pg failed to start:"; \
		cat "$(SYNCHROD_PG_LOG_FILE)"; \
		rm -f "$(SYNCHROD_PG_PID_FILE)"; \
		exit 1; \
	fi; \
	echo "synchrod-pg running on http://localhost:$(SYNCHROD_PG_PORT)"

synchrod-pg-test-stop:
	@STOPPED=0; \
	if [ -f "$(SYNCHROD_PG_PID_FILE)" ]; then \
		PID="$$(cat "$(SYNCHROD_PG_PID_FILE)")"; \
		if kill -0 "$$PID" 2>/dev/null; then \
			kill "$$PID"; \
			wait "$$PID" 2>/dev/null || true; \
			echo "synchrod-pg stopped"; \
			STOPPED=1; \
		fi; \
		rm -f "$(SYNCHROD_PG_PID_FILE)"; \
	fi; \
	for PID in $$(lsof -tiTCP:$(SYNCHROD_PG_PORT) -sTCP:LISTEN 2>/dev/null); do \
		kill "$$PID" 2>/dev/null || true; \
		wait "$$PID" 2>/dev/null || true; \
		STOPPED=1; \
	done; \
	if [ "$$STOPPED" -eq 0 ]; then \
		echo "synchrod-pg not running"; \
	fi
	@$(MAKE) test-adapter-teardown

synchrod-pg-test-restart: synchrod-pg-test-stop
	@$(MAKE) synchrod-pg-test-start

clean:
	rm -rf bin/ "$(SYNCHROD_PG_PID_FILE)" "$(SYNCHROD_PG_LOG_FILE)"

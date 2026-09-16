.PHONY: \
	help \
	version-print \
	version-check \
	version-sync \
	set-version \
	build \
	build-seed \
	build-check \
	run \
	docs-build \
	docs-dev \
	verify-contract \
	conformance-mod-download \
	build-conformance \
	lint-conformance \
	test-conformance-testresult \
	test-integration-mutant-manifest \
	test-conformance-imports \
	test-conformance-contract \
	test-conformance-drivers \
	update-conformance-catalog \
	check-conformance-catalog \
	test-conformance-scenarios \
	test-vectors \
	test-reference \
	test-conformance-faults \
	test-invariants \
	test-conformance-invariants \
	soak \
	test-local-postgres \
	test-blackbox-harness \
	test-blackbox-components \
	test-blackbox-wal \
	test-blackbox-configured-bounds \
	test-blackbox-mutation-control \
	test-r1-benchmark-units \
	record-r1-benchmark \
	test-r1-benchmark \
	_run-r1-benchmark \
	parse-testresult \
	conformance-adapter-artifact \
	conformance-seed-artifact \
	conformance-pg18-extension-artifact \
	conformance-pg18-extension-test-artifact \
	release-stage-server \
	release-stage-packages \
	release-stage \
	release-verify \
	release-consumer-artifacts \
	release-run-support-cell \
	test-conformance \
	test-blackbox \
	test-release-artifacts \
	test-release-publish \
	test-server-consumer-helper \
	test-consumer-go \
	lint-go \
	lint-rn \
	lint-rust-core \
	lint-rust-pg \
	lint-rust \
	test \
	test-rust-core \
	test-rust-mutants \
	test-rust-mutants-broad \
	test-integration-mutants \
	test-integration-mutants-broad \
	test-integration-mutant \
	test-rust-pg \
	test-rust-pg-all \
	test-adapter \
	local-postgres-start \
	local-postgres-stop \
	build-local-postgres \
	ext-build \
	ext-install \
	ext-test \
	ext-seed \
	build-swift-native-runner \
	build-kotlin-library \
	build-kotlin-conformance-app \
	test-swift-unit \
	test-client-schema-identity \
	_test-client-schema-identity \
	test-swift-warm-connect \
	test-swift-scenarios \
	test-swift \
	test-kotlin-unit \
	test-kotlin-warm-connect \
	test-kotlin-scenarios \
	test-kotlin-instrumentation \
	test-kotlin \
	test-kotlin-integration \
	test-rn-unit \
	test-rn-android-parity \
	test-rn-ios-parity \
	test-rn-native-parity \
	test-rn-warm-connect-control \
	test-rn-warm-connect-ios \
	test-rn-performance-ios \
	test-rn-performance-android \
	test-rn-pending-cycle-ios \
	test-rn-pending-cycle-android \
	test-rn-provenance-android \
	test-rn-provenance-ios \
	test-rn-push-android \
	test-rn-push-ios \
	test-rn-retention-android \
	test-rn-retention-ios \
	test-rn-check-android \
	test-rn-check-ios \
	test-rn-requests-android \
	test-rn-requests-ios \
	test-rn-forged-android \
	test-rn-forged-ios \
	test-rn-sqm-android \
	test-rn-sqm-ios \
	test-rn-cardinality-android \
	test-rn-cardinality-ios \
	test-rn-queue-replay-ios \
	test-rn-queue-replay-android \
	test-rn-seeded-empty-startup-ios \
	test-rn-seeded-empty-startup-android \
	test-rn-rebuild-apply-ios \
	test-rn-rebuild-apply-android \
	test-rn-warm-connect-android \
	verify-rn-seed \
	refresh-rn-seed \
	rn-seed-asset \
	rn-e2e-server-seed \
	rn-watchman-reset \
	rn-ios-pods \
	rn-android-emulator-reset \
	android-emulator-prepare \
	test-rn-e2e-ios-build \
	test-rn-e2e-ios-run \
	test-rn-e2e-ios \
	test-rn-e2e-android-build \
	test-rn-e2e-android-run \
	test-rn-e2e-android \
	test-rn \
	synchrod-pg-test-start \
	synchrod-pg-test-stop \
	synchrod-pg-test-restart \
	release-pods-check \
	release-kotlin-local \
	release-npm-dry-run \
	client-consumer-apple-artifact \
	client-consumer-kotlin-artifact \
	client-consumer-rn-artifact \
	client-consumer-artifacts \
	local-consumer-artifacts \
	test-consumer-swift \
	test-consumer-swift-smoke \
	test-consumer-swift-ios \
	test-consumer-kotlin \
	test-consumer-kotlin-device \
	test-consumer-kotlin-device-smoke \
	test-consumer-rn-ios \
	test-consumer-rn-android \
	test-consumer-rn-ios-smoke \
	test-consumer-rn-android-smoke \
	test-client-platforms \
	test-packaged-smoke \
	test-packaged-smoke-structure \
	test-packaged-consumers \
	generate-pg-sql \
	check-pg-sql \
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
KOTLIN_ANDROID_SERIAL ?= $(ANDROID_SERIAL)
RN_ANDROID_DETOX_CONFIG ?= android.emu.release
PGRX_PG ?= pg18
PGRX_PG_CONFIG ?= $(shell awk -F'"' '/^$(PGRX_PG)[[:space:]]*=/ { print $$2 }' $(HOME)/.pgrx/config.toml)
PGRX_PG_BIN_DIR ?= $(dir $(PGRX_PG_CONFIG))
PGRX_TARGET_DIR ?= $(CURDIR)/.pgrx-target
MUTATION_CONTROL_TEST ?=
MUTATION_CONTROL_EXPECT ?= target_pass
INTEGRATION_MUTANT_ID ?=
SOAK_SEED ?= 1
SOAK_DURATION ?= 1s
TESTRESULT_TEST_NAME ?=
BLACKBOX_TEST_COUNT ?= 1
CONFORMANCE_ADAPTER_ARTIFACT_DIR ?= $(CURDIR)/dist/conformance/synchrod-pg-adapter
CONFORMANCE_SEED_ARTIFACT ?= $(CURDIR)/dist/conformance/synchro-seed
CONFORMANCE_EXTENSION_ARTIFACT ?= $(CURDIR)/dist/conformance/synchro-pg-pg18
ADAPTER_TEST_URL ?=
REPLICATION_URL = $(ADAPTER_TEST_URL)
override R1_BENCHMARK_BASELINE := $(CURDIR)/conformance/blackbox/integration/testdata/r1-benchmark-baseline.json
R1_BENCHMARK_EXTENSION_TARGET ?= conformance-pg18-extension-artifact

SYNCHROD_PG_PORT ?= 8091
SYNCHRO_TEST_HOST ?= localhost
SYNCHRO_TEST_PORT ?= $(SYNCHROD_PG_PORT)
SYNCHRO_TEST_URL ?= http://$(SYNCHRO_TEST_HOST):$(SYNCHRO_TEST_PORT)
SYNCHRO_TEST_JWT_SECRET ?= test-secret-for-integration-tests
MIN_CLIENT_VERSION ?= 1.0.0
SYNCHROD_PG_PID_FILE ?= .synchrod-pg-test.pid
SYNCHROD_PG_LOG_FILE ?= .synchrod-pg-test.log
LOCAL_POSTGRES_STATE_DIR ?= $(CURDIR)/.ignore/r2/tmp/local-postgres
LOCAL_POSTGRES_PID_FILE ?= $(LOCAL_POSTGRES_STATE_DIR)/postgres.pid
LOCAL_POSTGRES_LOG_FILE ?= $(LOCAL_POSTGRES_STATE_DIR)/postgres.log
LOCAL_POSTGRES_URL_FILE ?= $(LOCAL_POSTGRES_STATE_DIR)/postgres.url
LOCAL_POSTGRES_ATTACH_ENV_FILE ?= $(LOCAL_POSTGRES_STATE_DIR)/attach.env
LOCAL_POSTGRES_LISTEN ?= 127.0.0.1
LOCAL_POSTGRES_BINARY ?= $(CURDIR)/bin/synchro-local-postgres
# One warm-connect gate run consumes one freshly started provisioner
# instance. Restart local-postgres-start before each run.
WARM_CONNECT_ENV_FILE ?=
WARM_CONNECT_ENV = if [ -n "$(WARM_CONNECT_ENV_FILE)" ]; then \
		test -r "$(WARM_CONNECT_ENV_FILE)"; \
		SYNCHRO_ATTACH_DIR="$$(cd "$$(dirname "$(WARM_CONNECT_ENV_FILE)")" && pwd)"; \
		export SYNCHRO_ATTACH_DIR; \
		set -a; . "$(WARM_CONNECT_ENV_FILE)"; set +a; \
		if [ -z "$${SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT:-}" ]; then \
			test -x "$(CONFORMANCE_ADAPTER_ARTIFACT_DIR)/synchrod-pg" || $(MAKE) conformance-adapter-artifact; \
			SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT="$(CONFORMANCE_ADAPTER_ARTIFACT_DIR)/synchrod-pg"; \
			export SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT; \
		fi; \
	fi;

BINARY ?= bin/synchrod-pg
SEED_BINARY ?= bin/synchro-seed
RN_PINNED_SEED ?= clients/react-native/example/seed.db
RN_CONSUMER_SEED ?= clients/react-native/example/verification/seed.db
RN_ANDROID_SEED_ASSET ?= clients/react-native/example/android/app/src/main/assets/seed.db
CLIENT_INTEGRATION_SEED ?= $(CURDIR)/.ignore/client-integration/seed.db
REFRESH_RN_SEED_OUTPUT ?= $(CURDIR)/clients/react-native/example/seed.db
GO_TEST_ARGS ?= -v -count=1 -p 1
GO_TEST_PKGS ?= ./...
GRADLE_TEST_ARGS ?= --rerun-tasks
CLIENT_ARTIFACT_DIR ?= $(CURDIR)/dist/local-consumer
LOCAL_CONSUMER_DIR ?= $(CLIENT_ARTIFACT_DIR)
CURRENT_VERSION := $(shell cat VERSION 2>/dev/null)
SWIFTPM_GIT_ENV := GIT_CONFIG_COUNT=1 GIT_CONFIG_KEY_0=safe.bareRepository GIT_CONFIG_VALUE_0=all
PACKAGED_SMOKE_EVIDENCE ?= $(CURDIR)/dist/verification/packaged-smoke-summary.json
PACKAGED_SMOKE_CELL_DIR ?= $(CURDIR)/dist/verification/packaged-smoke-cells
PACKAGED_SMOKE_TMP_ROOT ?= $(CURDIR)/.ignore/r2/tmp
RELEASE_DIR ?=
RELEASE_SERVER_DIR ?= $(CURDIR)/dist/release-components/server
RELEASE_PACKAGE_DIR ?= $(CURDIR)/dist/release-components/packages
RELEASE_CONSUMER_DIR ?= $(CURDIR)/dist/release-consumer
RELEASE_SBOM ?=
RELEASE_SUPPORT_ENVIRONMENTS ?=
RELEASE_EVIDENCE_DIR ?= $(CURDIR)/dist/verification/release-evidence
RELEASE_PG18_BIN_DIR ?=
RELEASE_PROVISIONER ?=
RELEASE_SERVER_LISTEN_URL ?= http://127.0.0.1:8091
RELEASE_CANDIDATE_CI_RUN_ID ?=
RELEASE_CANDIDATE_CI_RUN_ATTEMPT ?=
RELEASE_BUILD_RUN_ID ?= $(GITHUB_RUN_ID)
RELEASE_BUILD_RUN_ATTEMPT ?= $(GITHUB_RUN_ATTEMPT)
RELEASE_STAGED_ARTIFACTS ?= 0
CLIENT_ARTIFACTS_PREPARED ?= 0
RELEASE_INVENTORY := $(CURDIR)/conformance/artifacts/inventory.json
RELEASE_SUPPORT_MATRIX := $(CURDIR)/conformance/support-matrix.json

TEST_ENV = \
	TEST_DATABASE_URL="$(ADAPTER_TEST_URL)" \
	TEST_REPLICATION_URL="$(REPLICATION_URL)" \
	SYNCHRO_TEST_URL="$(SYNCHRO_TEST_URL)" \
	SYNCHRO_TEST_JWT_SECRET="$(SYNCHRO_TEST_JWT_SECRET)" \
	SYNCHRO_TEST_SEED_PATH="$(CURDIR)/clients/react-native/example/seed.db"

help:
	@echo "Available targets:"
	@echo "  version-print         - Print the canonical repo version from VERSION"
	@echo "  version-check         - Verify every public release surface matches VERSION"
	@echo "  version-sync          - Sync versioned metadata from VERSION"
	@echo "  set-version           - Set VERSION=X.Y.Z and sync public metadata"
	@echo "  build                 - Build the synchrod-pg adapter binary"
	@echo "  build-seed            - Build the seed database generator binary"
	@echo "  test-client-schema-identity - Verify Go seed DDL converges with Swift and Kotlin"
	@echo "  build-check           - Build the Go adapter module"
	@echo "  run                   - Run synchrod-pg locally with current env"
	@echo "  docs-build            - Verify the contract and build the docs site"
	@echo "  docs-dev              - Run the docs site locally"
	@echo "  verify-contract       - Validate the JavaScript-authored release contract"
	@echo "  conformance-mod-download - Download standalone conformance dependencies"
	@echo "  build-conformance     - Build every standalone conformance package"
	@echo "  lint-conformance      - Format and vet the standalone conformance module"
	@echo "  test-conformance-testresult - Test the structured Go test-result parser"
	@echo "  test-integration-mutant-manifest - Validate integration mutant bindings"
	@echo "  test-conformance-imports - Test standalone conformance import policy"
	@echo "  test-conformance-contract - Test strict contract loading and snapshots"
	@echo "  test-conformance-drivers - Test the plain Swift and Kotlin process drivers"
	@echo "  update-conformance-catalog - Write the deterministic scenario catalog"
	@echo "  check-conformance-catalog - Check the deterministic scenario catalog"
	@echo "  test-conformance-scenarios - Test strict scenario loading and catalog generation"
	@echo "  test-vectors          - Test canonical protocol 3 vectors"
	@echo "  test-reference        - Test the independent protocol 3 reference model"
	@echo "  test-conformance-invariants - Test the invariant engine and soak driver"
	@echo "  soak                  - Run the bounded seeded desktop soak"
	@echo "  test-conformance      - Run the independent protocol conformance suite"
	@echo "  test-blackbox         - Run the packaged server black-box suite"
	@echo "  test-blackbox-configured-bounds - Run the real configured-limit measurement proof"
	@echo "  test-blackbox-mutation-control - Run one structured real mutation control"
	@echo "  record-r1-benchmark   - Record one R1 benchmark candidate"
	@echo "  test-r1-benchmark     - Compare R1 benchmark results with the tracked baseline"
	@echo "  release-stage-server  - Build Linux x64 server release components"
	@echo "  release-stage-packages - Build signed Maven and npm release components"
	@echo "  release-stage         - Assemble and seal already built release components"
	@echo "  release-verify        - Verify one sealed release without building"
	@echo "  release-consumer-artifacts - Prepare sealed payloads for package consumers"
	@echo "  test-release-publish  - Test publication identity and recovery state"
	@echo "  test-server-consumer-helper - Run server packaged-consumer helper unit tests"
	@echo "  test-consumer-go      - Resolve and compile the public Go module consumer"
	@echo "  lint-go               - Run Go formatting checks and go vet"
	@echo "  lint-rn               - Run React Native typecheck and ESLint"
	@echo "  lint-rust-core        - Run Rust fmt and clippy for the shared core"
	@echo "  lint-rust-pg          - Run Rust fmt and clippy for the PostgreSQL extension"
	@echo "  lint-rust             - Run all Rust fmt and clippy checks"
	@echo "  test                  - Run the default local validation set"
	@echo "  test-rust-core        - Run synchro-core unit tests"
	@echo "  test-rust-mutants     - Run targeted synchro-core mutation tests"
	@echo "  test-rust-mutants-broad - Run broad synchro-core mutation search"
	@echo "  test-integration-mutants - Run curated production integration mutants"
	@echo "  test-integration-mutants-broad - Run every production integration mutant"
	@echo "  test-integration-mutant - Run one manifest mutant with INTEGRATION_MUTANT_ID"
	@echo "  test-rust-pg          - Run pgrx integration tests on PG 18"
	@echo "  test-rust-pg-all      - Run pgrx tests on PG 14 through PG 18"
	@echo "  test-adapter          - Run Go adapter integration tests (override GO_TEST_PKGS to focus)"
	@echo "                         Set ADAPTER_TEST_URL to the one test PostgreSQL database URL"
	@echo "  local-postgres-start  - Start an isolated PostgreSQL 18 through the Go provisioner"
	@echo "  local-postgres-stop   - Stop the isolated PostgreSQL 18 provisioner"
	@echo "  build-swift-native-runner - Build the macOS native conformance process"
	@echo "  build-kotlin-library  - Build the Kotlin client library"
	@echo "  build-kotlin-conformance-app - Build the Android native conformance test APK"
	@echo "  test-swift-unit       - Run Swift unit tests"
	@echo "  test-swift-warm-connect - Run the direct Swift warm-connect scenario"
	@echo "  test-swift-scenarios  - Run the direct Swift correctness scenarios"
	@echo "  test-swift            - Run Swift integration tests against the local adapter"
	@echo "  test-kotlin-unit      - Run Kotlin unit tests"
	@echo "  test-kotlin-scenarios - Run the direct Kotlin correctness scenarios"
	@echo "  test-kotlin-instrumentation - Run Android instrumentation on the selected device"
	@echo "  test-kotlin           - Run Kotlin integration tests against the local adapter"
	@echo "  test-rn-unit          - Run React Native Jest tests"
	@echo "  test-rn-android-parity - Regenerate the TurboModule spec and compile the Android implementation"
	@echo "  test-rn-ios-parity     - Compile the iOS implementation against the generated TurboModule spec"
	@echo "  test-rn-native-parity  - Compile both native implementations against one TurboModule spec"
	@echo "  test-rn-warm-connect-control - Run the exact React Native warm-connect negative control"
	@echo "  test-rn-warm-connect-ios - Run direct React Native warm-connect through the iOS bridge"
	@echo "  test-rn-performance-android - Run direct React Native steady-pull through the Android bridge"
	@echo "  test-rn-pending-cycle-ios - Run direct React Native pending-cycle through the iOS bridge"
	@echo "  test-rn-pending-cycle-android - Run direct React Native pending-cycle through the Android bridge"
	@echo "  test-rn-provenance-android - Run direct React Native multi-scope provenance through the Android bridge"
	@echo "  test-rn-provenance-ios - Run direct React Native multi-scope provenance through the iOS bridge"
	@echo "  test-rn-retention-ios - Run retention reconnect through the iOS bridge"
	@echo "  test-rn-retention-android - Run retention reconnect through the Android bridge"
	@echo "  test-rn-queue-replay-ios - Run direct React Native queue-replay through the iOS bridge"
	@echo "  test-rn-queue-replay-android - Run direct React Native queue-replay through the Android bridge"
	@echo "  test-rn-seeded-empty-startup-ios - Run seeded and empty startup through the iOS bridge"
	@echo "  test-rn-seeded-empty-startup-android - Run seeded and empty startup through the Android bridge"
	@echo "  test-rn-warm-connect-android - Run direct React Native warm-connect through the Android bridge"
	@echo "  verify-rn-seed        - Verify the pinned React Native seed digest"
	@echo "  refresh-rn-seed       - Regenerate and pin the React Native seed"
	@echo "  test-rn-e2e-ios       - Run React Native Detox tests on iOS"
	@echo "  test-rn-e2e-android   - Run React Native Detox tests on Android ($(RN_ANDROID_DETOX_CONFIG))"
	@echo "  test-rn               - Run React Native Detox tests on both platforms"
	@echo "  rn-android-emulator-reset - Stop any running Pixel_7_API_34 emulator before Detox"
	@echo "  android-emulator-prepare - Keep the booted Android test device awake and focused"
	@echo "  synchrod-pg-test-start   - Start the extension-backed test adapter for ADAPTER_TEST_URL"
	@echo "  synchrod-pg-test-stop    - Stop the extension-backed test adapter"
	@echo "  synchrod-pg-test-restart - Restart the extension-backed test adapter"
	@echo "  release-pods-check    - Validate Apple package metadata surfaces"
	@echo "  release-kotlin-local  - Publish Kotlin SDK to mavenLocal"
	@echo "  release-npm-dry-run   - Dry-run npm pack for the React Native package"
	@echo "  client-consumer-artifacts - Stage Apple, Kotlin, and React Native consumer artifacts"
	@echo "  local-consumer-artifacts - Build local-consumer artifacts for RN, Kotlin, and Apple"
	@echo "  test-consumer-swift   - Run the packaged Swift consumer"
	@echo "  test-consumer-swift-ios - Run the packaged Swift consumer on an iOS simulator"
	@echo "  test-consumer-kotlin  - Build the packaged Kotlin app and instrumentation APK"
	@echo "  test-consumer-kotlin-device - Run the packaged Kotlin consumer on a connected Android device"
	@echo "  test-consumer-rn-ios  - Build an isolated RN iOS consumer from packaged artifacts"
	@echo "  test-consumer-rn-android - Build an isolated RN Android consumer from packaged artifacts"
	@echo "  test-client-platforms - Run one packaged client support cell (SUPPORT_CELL_ID required)"
	@echo "  test-packaged-smoke   - Validate five terminal checks for every non-excluded support cell"
	@echo "  test-packaged-smoke-structure - Run packaged smoke summary failure controls"
	@echo "  test-packaged-consumers - Run all packaged consumer checks"
	@echo "  check-pg-sql          - Verify tracked SQL matches pgrx generation"
	@echo "  clean                 - Remove local build and server artifacts"

version-print:
	@cd api/go && GOWORK=off go run ./cmd/synchro-version print

version-check:
	@cd api/go && GOWORK=off go run ./cmd/synchro-version check $(if $(EXPECTED_TAG),--expected-tag "$(EXPECTED_TAG)")

version-sync:
	@cd api/go && GOWORK=off go run ./cmd/synchro-version sync

set-version:
	@test -n "$(VERSION)" || (echo "Provide VERSION=X.Y.Z"; exit 1)
	cd api/go && GOWORK=off go run ./cmd/synchro-version set "$(VERSION)"

build:
	@mkdir -p "$(dir $(BINARY))"
	cd api/go && GOWORK=off go build -o "$(abspath $(BINARY))" ./cmd/synchrod-pg

build-seed:
	@mkdir -p "$(dir $(SEED_BINARY))"
	cd api/go && GOWORK=off go build -o "$(abspath $(SEED_BINARY))" ./cmd/synchro-seed

build-check:
	cd api/go && GOWORK=off go build ./...

run:
	cd api/go && GOWORK=off go run ./cmd/synchrod-pg

docs-build: verify-contract
	cd docs && npm run build

docs-dev:
	cd docs && npm run dev

# This target validates the JavaScript-authored contract.
verify-contract:
	cd docs && npm ci
	cd docs && npm run verify:contract

conformance-mod-download:
	cd conformance && GOFLAGS= GOWORK=off go mod download all

build-conformance: conformance-mod-download
	cd conformance && GOFLAGS= GOWORK=off go build ./...

lint-conformance: conformance-mod-download
	@test -z "$$(find conformance -name '*.go' -print0 | xargs -0 gofmt -l)"
	cd conformance && GOFLAGS= GOWORK=off go vet ./...

test-conformance-testresult:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./cmd/testresult -count=1
	@cd conformance && if GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./cmd/testresult -count=1 -run '^TestDoesNotExist$$'; then \
		echo "testresult accepted a zero-match run" >&2; \
		exit 1; \
	fi

test-integration-mutant-manifest: conformance-mod-download
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./mutants -count=1

test-conformance-imports:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./internal/importguard -count=1

test-conformance-contract:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./internal/jsonstrict ./internal/schemavalidator ./internal/contract -count=1

test-conformance-drivers:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./swift ./kotlin ./reactnative -count=1

update-conformance-catalog:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/synchro-conformance catalog --repo-root .. --write

check-conformance-catalog:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/synchro-conformance catalog --repo-root .. --check

test-conformance-scenarios:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./scenarios/... ./modelrunner ./cmd/synchro-conformance -count=1

test-vectors:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./vectors -count=1

test-reference:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./reference -count=1

test-conformance-faults:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./barriers ./faults -count=1

test-invariants:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./invariants -count=1

test-conformance-invariants: test-invariants
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./soak -count=1

soak:
	@$(WARM_CONNECT_ENV) \
		test -n "$${SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT:-}" || { echo "the black-box environment is required for soak: set WARM_CONNECT_ENV_FILE or export SYNCHRO_CONFORMANCE_* variables" >&2; exit 1; }; \
		cd conformance && GOFLAGS= GOWORK=off \
			SOAK_SEED="$(SOAK_SEED)" SOAK_DURATION="$(SOAK_DURATION)" \
			go run ./cmd/testresult suite -- go test -json ./blackbox/integration -count=1 -timeout=35m \
			-run '^TestSoak$$' -args --provision --install

test-local-postgres:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./cmd/synchro-local-postgres -count=1

test-blackbox-harness:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./blackbox -count=1

test-blackbox-components:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./observer -count=1

test-blackbox-wal: conformance-mod-download test-blackbox-harness
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./blackbox/integration -run '^TestRealWALPipeline$$' -count=1 -args --provision --install

test-blackbox-configured-bounds: conformance-mod-download test-blackbox-harness
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite \
		-- go test -json ./blackbox/integration -count=1 -timeout=20m \
		-run '^TestRealConfiguredBoundsMeasurement$$' -args --provision --install

test-blackbox-mutation-control:
	@test -n "$(MUTATION_CONTROL_TEST)" || { echo "MUTATION_CONTROL_TEST is required" >&2; exit 1; }
	@case "$(MUTATION_CONTROL_EXPECT)" in \
		target_pass|target_semantic_test_failure) ;; \
		*) echo "MUTATION_CONTROL_EXPECT is invalid" >&2; exit 1 ;; \
	esac
	@target='$(MUTATION_CONTROL_TEST)'; test_name=$${target%%/*}; assertion=$${target#*/}; \
	if [ "$$assertion" = "$$target" ]; then assertion=assertion; fi; \
	case "$$test_name" in \
		TestRealMutationControlCursorAdvancement|TestRealMutationControlWALAcknowledgement|TestRealMutationControlMutationConservation|TestRealMutationControlChecksumCorrectness|TestRealMutationControlScopeIsolation|TestRealMutationControlProgressOrder|TestRealS02DivergentPullPaginationIsStarvationFree|\
		TestRealIssue49ConnectRejectsFreshReuseAndInvalidEnvelopeValues|TestRealIssue49SemanticVersionPrecedence|TestRealIssue49PortableIntegerBoundariesAndCounterOverflow|TestRealIssue49MutationLifecycleVersionsVocabularyAndCrossBatchReplay|TestRealIssue49PortableSeedScopeContinuationAndTokenBindings|TestRealIssue49ConcurrentUpdateDeletePreservesOneAuthoritativeWinner|TestRealIssue49RebuildReplayEpochAndMonotonicCursor|TestRealIssue49PublishedSchemaIdentityIsImmutable|\
		TestRealIssue49SecurityAdapterAuthorityAndScopeBoundary|TestRealIssue49SecurityRegistryIdentityAndKeys|TestRealIssue49SecurityCaptureHealthFailsClosed|TestRealIssue49SecurityDatabaseAuthority|TestRealIssue49SecurityOperationalRedaction|TestRealIssue49SecurityInstallationAuthority|\
		TestRealIssue49WALIsTheOnlyAtomicPublicationPath|TestRealIssue49WALPoisonBlocksContiguousProgress|TestRealIssue49ResetLifecycleAndFenceCoverage|TestRealIssue49FenceCorrelationAndCapturePending|TestRealIssue49CompletePullVisibleWALRepresentation|TestRealIssue49CaptureReadinessRequiresEveryCheck|TestRealIssue49FenceCorrelatesOldRecordIdentity|TestRealIssue49FenceCorrelatesCaptureKeys|TestRealIssue49ResetCoversEveryFenceOperation|TestRealIssue49MembershipBackfillRetainsContinuationAcrossWorkerLoss|\
		TestRealIssue49RemainingSemantics) ;; \
		*) echo "MUTATION_CONTROL_TEST is not a supported mutation control" >&2; exit 1 ;; \
	esac; \
	case "$$assertion" in assertion|assertion\#[0-9][0-9]) ;; *) echo "MUTATION_CONTROL_TEST does not name a supported assertion" >&2; exit 1 ;; esac; \
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test "$$target" \
		-expect "$(MUTATION_CONTROL_EXPECT)" \
		-- go test -json ./blackbox/integration -count=1 -run "^$$test_name$$/^$$assertion$$" -args --provision --install

test-r1-benchmark-units:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -tags r1benchmark -json ./blackbox/integration -count=1 -run '^TestR1Benchmark(StrictParser|ThresholdLogic|ResultPathSafety)$$'

record-r1-benchmark: test-r1-benchmark-units
	@$(MAKE) --no-print-directory _run-r1-benchmark R1_BENCHMARK_RUN_MODE=record

test-r1-benchmark: test-r1-benchmark-units
	@$(MAKE) --no-print-directory _run-r1-benchmark R1_BENCHMARK_RUN_MODE=compare

_run-r1-benchmark:
	@case "$(R1_BENCHMARK_RUN_MODE)" in record|compare) ;; *) echo "R1 benchmark run mode is invalid" >&2; exit 1 ;; esac
	@test -n "$(R1_BENCHMARK_RESULT)" || { echo "R1_BENCHMARK_RESULT is required" >&2; exit 1; }
	@test "$(abspath $(R1_BENCHMARK_RESULT))" != "$(R1_BENCHMARK_BASELINE)" || { echo "R1_BENCHMARK_RESULT must not replace the baseline" >&2; exit 1; }
	@result="$(abspath $(R1_BENCHMARK_RESULT))"; repo="$(CURDIR)"; \
		case "$$result" in "$$repo"|"$$repo"/*) echo "R1_BENCHMARK_RESULT must be outside the repository" >&2; exit 1 ;; esac
	@test -z "$$(git status --porcelain --untracked-files=normal)" || { echo "R1 benchmark requires a clean worktree" >&2; exit 1; }
	@git ls-files --error-unmatch -- "conformance/blackbox/integration/real_r1_benchmark_test.go" >/dev/null 2>&1 || { echo "R1 benchmark definition is not tracked" >&2; exit 1; }
	@if [ "$(R1_BENCHMARK_RUN_MODE)" = compare ]; then \
		test -f "$(R1_BENCHMARK_BASELINE)" || { echo "tracked R1 benchmark baseline is missing" >&2; exit 1; }; \
		git ls-files --error-unmatch -- "conformance/blackbox/integration/testdata/r1-benchmark-baseline.json" >/dev/null 2>&1 || { echo "R1 benchmark baseline is not tracked" >&2; exit 1; }; \
	fi
	@set -eu; \
		revision="$$(git rev-parse --verify HEAD)"; \
		test "$${#revision}" -eq 40; \
		repo="$$(pwd -P)"; \
		temp_parent="$${TMPDIR:-/tmp}"; \
		temp_parent="$$(cd "$$temp_parent" && pwd -P)"; \
		case "$$temp_parent" in "$$repo"|"$$repo"/*) echo "R1 benchmark temporary directory must be outside the repository" >&2; exit 1 ;; esac; \
		artifact_root="$$(mktemp -d "$$temp_parent/synchro-r1-$$revision.XXXXXX")"; \
		cleanup() { rm -rf "$$artifact_root"; }; \
		trap cleanup EXIT HUP INT TERM; \
		adapter_bundle="$$artifact_root/adapter"; \
		extension_bundle="$$artifact_root/extension"; \
		secrets_dir="$$artifact_root/secrets"; \
		mkdir "$$secrets_dir"; \
		umask 077; \
		for name in admin adapter observer worker operator jwt; do openssl rand -hex 32 > "$$secrets_dir/$$name-password"; done; \
		pg_config="$$(while IFS=' =' read -r key value; do test "$$key" = pg18 || continue; value="$${value#\"}"; value="$${value%\"}"; printf '%s\n' "$$value"; break; done < "$$HOME/.pgrx/config.toml")"; \
		test -x "$$pg_config" || { echo "pgrx PostgreSQL 18 configuration is unavailable" >&2; exit 1; }; \
		pg_bindir="$$(dirname "$$pg_config")"; \
		$(MAKE) --no-print-directory conformance-adapter-artifact CONFORMANCE_ADAPTER_ARTIFACT_DIR="$$adapter_bundle"; \
		$(MAKE) --no-print-directory $(R1_BENCHMARK_EXTENSION_TARGET) CONFORMANCE_EXTENSION_ARTIFACT="$$extension_bundle" PGRX_TARGET_DIR="$$artifact_root/cargo-target"; \
		test -z "$$(git status --porcelain --untracked-files=normal)" || { echo "R1 artifact packaging changed the worktree" >&2; exit 1; }; \
		test "$$(git rev-parse --verify HEAD)" = "$$revision" || { echo "R1 benchmark revision changed during packaging" >&2; exit 1; }; \
		cd conformance; \
		SYNCHRO_CONFORMANCE_ADAPTER_ARTIFACT="$$adapter_bundle/synchrod-pg" \
		SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT="$$extension_bundle" \
		SYNCHRO_CONFORMANCE_PG18_BINDIR="$$pg_bindir" \
		SYNCHRO_CONFORMANCE_ADMIN_USER="synchro_cf_admin" \
		SYNCHRO_CONFORMANCE_ADMIN_PASSWORD_FILE="$$secrets_dir/admin-password" \
		SYNCHRO_CONFORMANCE_ADAPTER_USER="synchro_cf_adapter" \
		SYNCHRO_CONFORMANCE_ADAPTER_PASSWORD_FILE="$$secrets_dir/adapter-password" \
		SYNCHRO_CONFORMANCE_OBSERVER_USER="synchro_cf_observer" \
		SYNCHRO_CONFORMANCE_OBSERVER_PASSWORD_FILE="$$secrets_dir/observer-password" \
		SYNCHRO_CONFORMANCE_WORKER_USER="synchro_cf_worker" \
		SYNCHRO_CONFORMANCE_WORKER_PASSWORD_FILE="$$secrets_dir/worker-password" \
		SYNCHRO_CONFORMANCE_OPERATOR_USER="synchro_cf_operator" \
		SYNCHRO_CONFORMANCE_OPERATOR_PASSWORD_FILE="$$secrets_dir/operator-password" \
		SYNCHRO_CONFORMANCE_JWT_SECRET_FILE="$$secrets_dir/jwt-password" \
		SYNCHRO_CONFORMANCE_INSTALL_LOCK="$$artifact_root/install.lock" \
		R1_BENCHMARK_MODE="$(R1_BENCHMARK_RUN_MODE)" \
		R1_BENCHMARK_REVISION="$$revision" \
		R1_BENCHMARK_RESULT="$(abspath $(R1_BENCHMARK_RESULT))" \
		GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
			-test TestRealR1PerformanceBenchmark \
			-expect target_pass \
			-- go test -tags r1benchmark -json ./blackbox/integration -count=1 -timeout=20m \
			-run '^TestRealR1PerformanceBenchmark$$' -args --provision --install

parse-testresult:
	@test -n "$(TESTRESULT_TEST_NAME)" || { echo "TESTRESULT_TEST_NAME is required" >&2; exit 1; }
	@cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult --test "$(TESTRESULT_TEST_NAME)"

conformance-adapter-artifact:
	@set -eu; \
		final="$(CONFORMANCE_ADAPTER_ARTIFACT_DIR)"; \
		parent="$$(dirname "$$final")"; \
		stage="$$final.tmp.$$$$"; \
		lock="$$final.publish-lock"; \
		mkdir -p "$$parent"; \
		mkdir "$$lock" || { echo "adapter artifact publication is locked" >&2; exit 1; }; \
		cleanup() { rm -rf "$$stage"; rmdir "$$lock" 2>/dev/null || true; }; \
		trap cleanup EXIT HUP INT TERM; \
		test ! -e "$$final" || { echo "$$final already exists" >&2; exit 1; }; \
		mkdir "$$stage"; \
		(cd api/go && GOWORK=off go build -o "$$stage/synchrod-pg" ./cmd/synchrod-pg); \
		test -x "$$stage/synchrod-pg"; \
		digest="$$(shasum -a 256 "$$stage/synchrod-pg" | cut -d ' ' -f 1)"; \
		test -n "$$digest"; \
		printf '%s\n' "$$digest" > "$$stage/synchrod-pg.sha256.tmp"; \
		mv "$$stage/synchrod-pg.sha256.tmp" "$$stage/synchrod-pg.sha256"; \
		mv "$$stage" "$$final"; \
		rmdir "$$lock"; \
		trap - EXIT HUP INT TERM

conformance-seed-artifact:
	@set -eu; \
		final="$(CONFORMANCE_SEED_ARTIFACT)"; \
		parent="$$(dirname "$$final")"; \
		stage="$$final.tmp.$$$$"; \
		lock="$$final.publish-lock"; \
		mkdir -p "$$parent"; \
		mkdir "$$lock" || { echo "seed artifact publication is locked" >&2; exit 1; }; \
		cleanup() { rm -f "$$stage" "$$stage.sha256.tmp"; rmdir "$$lock" 2>/dev/null || true; }; \
		trap cleanup EXIT HUP INT TERM; \
		test ! -e "$$final" || { echo "$$final already exists" >&2; exit 1; }; \
		(cd api/go && GOWORK=off go build -o "$$stage" ./cmd/synchro-seed); \
		test -x "$$stage"; \
		digest="$$(shasum -a 256 "$$stage" | cut -d ' ' -f 1)"; \
		test -n "$$digest"; \
		printf '%s\n' "$$digest" > "$$stage.sha256.tmp"; \
		mv "$$stage" "$$final"; \
		mv "$$stage.sha256.tmp" "$$final.sha256"; \
		rmdir "$$lock"; \
		trap - EXIT HUP INT TERM

conformance-pg18-extension-artifact: override CONFORMANCE_PG18_EXTENSION_ARTIFACT_POLICY := certified
conformance-pg18-extension-test-artifact: override CONFORMANCE_PG18_EXTENSION_ARTIFACT_POLICY := runtime
conformance-pg18-extension-artifact conformance-pg18-extension-test-artifact:
	@set -eu; \
		test -n "$(PGRX_PG_CONFIG)" || { echo "PGRX_PG_CONFIG is required" >&2; exit 1; }; \
		postgresql_version="$$($(PGRX_PG_CONFIG) --version | awk '{print $$2}')"; \
		case "$(CONFORMANCE_PG18_EXTENSION_ARTIFACT_POLICY)" in \
			certified) test "$$postgresql_version" = "18.3" || { echo "PGRX_PG_CONFIG must select PostgreSQL 18.3, found: $$($(PGRX_PG_CONFIG) --version)" >&2; exit 1; } ;; \
			runtime) printf '%s\n' "$$postgresql_version" | awk 'NR == 1 && $$0 ~ /^18\.[0-9]+$$/ { valid = 1 } END { exit valid && NR == 1 ? 0 : 1 }' || { echo "PGRX_PG_CONFIG must select PostgreSQL 18.x, found: $$($(PGRX_PG_CONFIG) --version)" >&2; exit 1; } ;; \
			*) echo "extension artifact PostgreSQL version policy is invalid" >&2; exit 1 ;; \
		esac; \
		final="$(CONFORMANCE_EXTENSION_ARTIFACT)"; \
		parent="$$(dirname "$$final")"; \
		out="$$final.tmp.$$$$"; \
		lock="$$final.publish-lock"; \
		mkdir -p "$$parent"; \
		mkdir "$$lock" || { echo "extension artifact publication is locked" >&2; exit 1; }; \
		cleanup() { rm -rf "$$out"; rmdir "$$lock" 2>/dev/null || true; }; \
		trap cleanup EXIT HUP INT TERM; \
		test ! -e "$$final" || { echo "$$final already exists" >&2; exit 1; }; \
		(cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx package --pg-config "$(PGRX_PG_CONFIG)" --out-dir "$$out"); \
		pkglibdir="$$($(PGRX_PG_CONFIG) --pkglibdir)"; \
		sharedir="$$($(PGRX_PG_CONFIG) --sharedir)"; \
		case "$$(uname -s)" in Darwin) suffix=dylib ;; *) suffix=so ;; esac; \
		library="$$out$$pkglibdir/synchro_pg.$$suffix"; \
		control="$$out$$sharedir/extension/synchro_pg.control"; \
		sql="$$out$$sharedir/extension/synchro_pg--$(CURRENT_VERSION).sql"; \
		test -f "$$library" && test -f "$$control" && test -f "$$sql"; \
		perl -pi -e 's/[ \t]+$$//' "$$sql"; \
		perl -0pi -e 's/\n+\z/\n/' "$$sql"; \
		cmp -s extensions/synchro-pg/sql/synchro_pg--$(CURRENT_VERSION).sql "$$sql"; \
		cmp -s extensions/synchro-pg/synchro_pg.control "$$control"; \
		library_path="$${library#"$$out"/}"; \
		control_path="$${control#"$$out"/}"; \
		sql_path="$${sql#"$$out"/}"; \
		library_hash="$$(shasum -a 256 "$$library" | cut -d ' ' -f 1)"; \
		control_hash="$$(shasum -a 256 "$$control" | cut -d ' ' -f 1)"; \
		sql_hash="$$(shasum -a 256 "$$sql" | cut -d ' ' -f 1)"; \
		printf '%s\n' \
			'{' \
			'  "format": "synchro-pg18-extension-bundle-v1",' \
			'  "postgresql_major": 18,' \
			"  \"postgresql_version\": \"$$postgresql_version\"," \
			'  "files": [' \
			"    {\"path\": \"$$library_path\", \"destination\": \"pkglibdir/synchro_pg.$$suffix\", \"sha256\": \"$$library_hash\"}," \
			"    {\"path\": \"$$control_path\", \"destination\": \"sharedir/extension/synchro_pg.control\", \"sha256\": \"$$control_hash\"}," \
			"    {\"path\": \"$$sql_path\", \"destination\": \"sharedir/extension/synchro_pg--$(CURRENT_VERSION).sql\", \"sha256\": \"$$sql_hash\"}" \
			'  ]' \
			'}' > "$$out/artifact-manifest.json.tmp"; \
		mv "$$out/artifact-manifest.json.tmp" "$$out/artifact-manifest.json"; \
		manifest_digest="$$(shasum -a 256 "$$out/artifact-manifest.json" | cut -d ' ' -f 1)"; \
		test -n "$$manifest_digest"; \
		printf '%s\n' "$$manifest_digest" > "$$out/artifact-manifest.json.sha256.tmp"; \
		mv "$$out/artifact-manifest.json.sha256.tmp" "$$out/artifact-manifest.json.sha256"; \
		mv "$$out" "$$final"; \
		rmdir "$$lock"; \
		trap - EXIT HUP INT TERM

test-blackbox: conformance-mod-download test-blackbox-harness test-blackbox-components
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test $(GO_TEST_ARGS) -json ./blackbox/integration -count=$(BLACKBOX_TEST_COUNT) -timeout=20m -args --provision --install

test-conformance: conformance-mod-download test-conformance-testresult test-conformance-imports test-conformance-contract test-conformance-drivers test-conformance-scenarios check-conformance-catalog test-vectors test-reference test-conformance-faults test-invariants test-conformance-invariants test-blackbox-harness

release-stage-server: version-check
	@test -n "$(VERSION)" && test "$(VERSION)" = "$(CURRENT_VERSION)" || { echo "VERSION=$(CURRENT_VERSION) is required" >&2; exit 1; }
	@set -eu; \
		test "$$(uname -s)" = Linux && test "$$(uname -m)" = x86_64 || { echo "release server components require Linux x64" >&2; exit 1; }; \
		test -z "$$(git status --porcelain --untracked-files=normal)" || { echo "release staging requires a clean worktree" >&2; exit 1; }; \
		revision="$$(git rev-parse --verify HEAD)"; \
		final="$(abspath $(RELEASE_SERVER_DIR))"; \
		stage="$$final.tmp.$$$$"; \
		trap 'rm -rf "$$stage"' EXIT HUP INT TERM; \
		test ! -e "$$final" || { echo "server component staging already exists: $$final" >&2; exit 1; }; \
		mkdir -p "$$stage/extension" "$$stage/adapter" "$$stage/seed"; \
		$(MAKE) --no-print-directory conformance-pg18-extension-artifact CONFORMANCE_EXTENSION_ARTIFACT="$$stage/extension-bundle"; \
		GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(MAKE) --no-print-directory build BINARY="$$stage/adapter/synchrod-pg-linux-x64-$(VERSION)"; \
		GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(MAKE) --no-print-directory build-seed SEED_BINARY="$$stage/seed/synchro-seed-linux-x64-$(VERSION)"; \
		python3 scripts/release-artifacts.py archive-extension --source "$$stage/extension-bundle" --output "$$stage/extension/synchro-pg-pg18-ubuntu24.04-linux-x64-$(VERSION).tar.gz"; \
		rm -rf "$$stage/extension-bundle"; \
		python3 scripts/release-artifacts.py server-metadata --output "$$stage/server-metadata.json" --source-commit "$$revision" \
			--binary "adapter/synchrod-pg-linux-x64-$(VERSION)=$$stage/adapter/synchrod-pg-linux-x64-$(VERSION)" \
			--binary "seed/synchro-seed-linux-x64-$(VERSION)=$$stage/seed/synchro-seed-linux-x64-$(VERSION)"; \
		test -z "$$(git status --porcelain --untracked-files=normal)" && test "$$(git rev-parse --verify HEAD)" = "$$revision" || { echo "source changed during release server staging" >&2; exit 1; }; \
		mkdir -p "$$(dirname "$$final")"; \
		mv "$$stage" "$$final"; \
		trap - EXIT HUP INT TERM

release-stage-packages: version-check
	@test -n "$(VERSION)" && test "$(VERSION)" = "$(CURRENT_VERSION)" || { echo "VERSION=$(CURRENT_VERSION) is required" >&2; exit 1; }
	@test -n "$(ANDROID_JAVA_HOME)" || { echo "ANDROID_JAVA_HOME is required" >&2; exit 1; }
	@test -d "$(ANDROID_HOME)" || { echo "ANDROID_HOME is required" >&2; exit 1; }
	@set -eu; \
		test -z "$$(git status --porcelain --untracked-files=normal)" || { echo "release staging requires a clean worktree" >&2; exit 1; }; \
		revision="$$(git rev-parse --verify HEAD)"; \
		final="$(abspath $(RELEASE_PACKAGE_DIR))"; \
		stage="$$final.tmp.$$$$"; \
		trap 'rm -rf "$$stage"' EXIT HUP INT TERM; \
		test ! -e "$$final" || { echo "package component staging already exists: $$final" >&2; exit 1; }; \
		mkdir -p "$$stage/maven" "$$stage/npm" "$$stage/maven-repository"; \
		(cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
			JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
			SYNCHRO_RELEASE_MAVEN_REPOSITORY="$$stage/maven-repository" \
			./gradlew -Pversion="$(VERSION)" :synchro:releaseBundle); \
		python3 scripts/release-artifacts.py prepare-maven --repository "$$stage/maven-repository" --version "$(VERSION)"; \
		python3 scripts/release-artifacts.py archive-maven --source "$$stage/maven-repository" --output "$$stage/maven/synchro-maven-$(VERSION).zip" --version "$(VERSION)"; \
		rm -rf "$$stage/maven-repository"; \
		(cd clients/react-native && corepack enable >/dev/null 2>&1 && yarn install --immutable && yarn prepare && npm pack --ignore-scripts --silent --pack-destination "$$stage/npm"); \
		test -f "$$stage/npm/trainstar-synchro-react-native-$(VERSION).tgz"; \
		python3 scripts/release-artifacts.py package-metadata --output "$$stage/package-metadata.json" --source-commit "$$revision"; \
		test -z "$$(git status --porcelain --untracked-files=normal)" && test "$$(git rev-parse --verify HEAD)" = "$$revision" || { echo "source changed during release package staging" >&2; exit 1; }; \
		mkdir -p "$$(dirname "$$final")"; \
		mv "$$stage" "$$final"; \
		trap - EXIT HUP INT TERM

release-stage:
	@test -n "$(VERSION)" && test "$(VERSION)" = "$(CURRENT_VERSION)" || { echo "VERSION=$(CURRENT_VERSION) is required" >&2; exit 1; }
	@test -n "$(RELEASE_DIR)" || { echo "RELEASE_DIR is required" >&2; exit 1; }
	@test -f "$(RELEASE_SBOM)" || { echo "RELEASE_SBOM is required" >&2; exit 1; }
	@test -f "$(RELEASE_SUPPORT_ENVIRONMENTS)" || { echo "RELEASE_SUPPORT_ENVIRONMENTS is required" >&2; exit 1; }
	@test -n "$(RELEASE_CANDIDATE_CI_RUN_ID)" && test -n "$(RELEASE_CANDIDATE_CI_RUN_ATTEMPT)" || { echo "RELEASE_CANDIDATE_CI_RUN_ID and RELEASE_CANDIDATE_CI_RUN_ATTEMPT are required" >&2; exit 1; }
	@test -n "$(RELEASE_BUILD_RUN_ID)" && test -n "$(RELEASE_BUILD_RUN_ATTEMPT)" || { echo "RELEASE_BUILD_RUN_ID and RELEASE_BUILD_RUN_ATTEMPT are required" >&2; exit 1; }
	@test -z "$$(git status --porcelain --untracked-files=normal)" || { echo "release staging requires a clean worktree" >&2; exit 1; }
	@revision="$$(git rev-parse --verify HEAD)"; \
		python3 scripts/release-artifacts.py stage \
			--release-dir "$(abspath $(RELEASE_DIR))" --version "$(VERSION)" --source-commit "$$revision" \
			--inventory "$(RELEASE_INVENTORY)" --support-matrix "$(RELEASE_SUPPORT_MATRIX)" \
			--support-resolution "$(abspath $(RELEASE_SUPPORT_ENVIRONMENTS))" \
			--server-dir "$(abspath $(RELEASE_SERVER_DIR))" --packages-dir "$(abspath $(RELEASE_PACKAGE_DIR))" \
			--sbom "$(abspath $(RELEASE_SBOM))" --repo-root "$(CURDIR)" \
			--source-tree "repo-root=$$(git rev-parse HEAD^{tree})" --source-tree "api/go=$$(git rev-parse HEAD:api/go)" \
			--candidate-ci-run-id "$(RELEASE_CANDIDATE_CI_RUN_ID)" --candidate-ci-run-attempt "$(RELEASE_CANDIDATE_CI_RUN_ATTEMPT)" \
			--build-run-id "$(RELEASE_BUILD_RUN_ID)" --build-run-attempt "$(RELEASE_BUILD_RUN_ATTEMPT)"

release-verify:
	@test -n "$(VERSION)" || { echo "VERSION is required" >&2; exit 1; }
	@test -n "$(RELEASE_DIR)" || { echo "RELEASE_DIR is required" >&2; exit 1; }
	@python3 scripts/release-artifacts.py verify --release-dir "$(abspath $(RELEASE_DIR))" --version "$(VERSION)" \
		--inventory "$(RELEASE_INVENTORY)" --support-matrix "$(RELEASE_SUPPORT_MATRIX)" \
		--source-commit "$$(git rev-parse --verify HEAD)"

release-consumer-artifacts: release-verify
	@test -n "$(VERSION)" && test "$(VERSION)" = "$(CURRENT_VERSION)" || { echo "VERSION=$(CURRENT_VERSION) is required" >&2; exit 1; }
	@set -eu; \
		release="$(abspath $(RELEASE_DIR))"; \
		final="$(abspath $(RELEASE_CONSUMER_DIR))"; \
		manifest_hash="$$(shasum -a 256 "$$release/release-manifest.json" | cut -d ' ' -f 1)"; \
		if [ -d "$$final" ]; then \
			test "$$(cat "$$final/.release-manifest.sha256")" = "$$manifest_hash"; \
			exit 0; \
		fi; \
		stage="$$final.tmp.$$$$"; \
		trap 'rm -rf "$$stage"' EXIT HUP INT TERM; \
		test ! -e "$$final" || { echo "release consumer artifact path is not a directory: $$final" >&2; exit 1; }; \
		mkdir -p "$$stage/apple/Synchro" "$$stage/maven" "$$stage/npm"; \
		git archive --format=tar HEAD Package.swift Synchro.podspec LICENSE clients/swift/Sources \
			| tar -xf - -C "$$stage/apple/Synchro"; \
		test ! -e "$$stage/apple/Synchro/Package.resolved"; \
		find "$$stage/apple/Synchro" -exec touch -t 202601010000 {} +; \
		COPYFILE_DISABLE=1 tar -cf - -C "$$stage/apple" Synchro | gzip -n > "$$stage/apple/synchro-spm-$(VERSION).tar.gz"; \
		python3 -m zipfile -e "$$release/artifacts/synchro-maven-$(VERSION).zip" "$$stage/maven"; \
		cp "$$release/artifacts/trainstar-synchro-react-native-$(VERSION).tgz" "$$stage/npm/"; \
		git clone --bare --quiet . "$$stage/source.git"; \
		git --git-dir="$$stage/source.git" tag -f "v$(VERSION)" "$$(git rev-parse HEAD)"; \
		git --git-dir="$$stage/source.git" tag -f "api/go/v$(VERSION)" "$$(git rev-parse HEAD)"; \
		printf '%s\n' "$$manifest_hash" > "$$stage/.release-manifest.sha256"; \
		mkdir -p "$$(dirname "$$final")"; \
		mv "$$stage" "$$final"; \
		trap - EXIT HUP INT TERM

release-run-support-cell:
	@test -n "$(SUPPORT_CELL_ID)" || { echo "SUPPORT_CELL_ID is required" >&2; exit 1; }
	@$(MAKE) --no-print-directory release-verify VERSION="$(VERSION)" RELEASE_DIR="$(abspath $(RELEASE_DIR))"
	@set -eu; \
		release="$(abspath $(RELEASE_DIR))"; \
		mkdir -p "$(RELEASE_EVIDENCE_DIR)/cells"; \
		case "$(SUPPORT_CELL_ID)" in \
		SUP-PG-LINUX-X64-001) \
			test -d "$(RELEASE_PG18_BIN_DIR)" || { echo "RELEASE_PG18_BIN_DIR is required" >&2; exit 1; }; \
			test -x "$(RELEASE_PROVISIONER)" || { echo "RELEASE_PROVISIONER is required" >&2; exit 1; }; \
			chmod u+x "$$release/artifacts/synchrod-pg-linux-x64-$(VERSION)" "$$release/artifacts/synchro-seed-linux-x64-$(VERSION)"; \
			python3 verification/packaged_smoke.py begin-cell --repo-root "$(CURDIR)" \
				--cell "$(SUPPORT_CELL_ID)" --output "$(RELEASE_EVIDENCE_DIR)/cells/$(SUPPORT_CELL_ID).json"; \
			hashes="$$(python3 scripts/release-artifacts.py print-payload-hashes --release-dir "$$release" --version "$(VERSION)" \
				--inventory "$(RELEASE_INVENTORY)" --support-matrix "$(RELEASE_SUPPORT_MATRIX)" \
				--role pg-extension --role adapter --role seed-tool)"; \
			python3 scripts/release-artifacts.py run-verified --release-dir "$$release" --version "$(VERSION)" \
				--inventory "$(RELEASE_INVENTORY)" --support-matrix "$(RELEASE_SUPPORT_MATRIX)" \
				--source-commit "$$(git rev-parse --verify HEAD)" -- \
				sh verification/consumers/server/test-consumer.sh \
					"$(abspath $(RELEASE_PG18_BIN_DIR))" \
					"$$release/artifacts/synchro-pg-pg18-ubuntu24.04-linux-x64-$(VERSION).tar.gz" \
					"$(abspath $(RELEASE_PROVISIONER))" \
					"$$release/artifacts/synchrod-pg-linux-x64-$(VERSION)" \
					"$$release/artifacts/synchro-seed-linux-x64-$(VERSION)" \
					"$(RELEASE_SERVER_LISTEN_URL)" "$(CURDIR)" "$(SUPPORT_CELL_ID)" \
					"$(RELEASE_EVIDENCE_DIR)/cells/$(SUPPORT_CELL_ID).json" "$$hashes" ;; \
		SUP-IOS-MIN-001|SUP-IOS-CURRENT-001|SUP-ANDROID-MIN-001|SUP-ANDROID-CURRENT-001|SUP-RN-IOS-CURRENT-001|SUP-RN-ANDROID-CURRENT-001) \
			$(MAKE) --no-print-directory release-consumer-artifacts VERSION="$(VERSION)" RELEASE_DIR="$$release" \
				RELEASE_CONSUMER_DIR="$(abspath $(RELEASE_CONSUMER_DIR))"; \
			case "$(SUPPORT_CELL_ID)" in \
			SUP-IOS-*) artifacts="$$release/release-manifest.json" ;; \
			SUP-ANDROID-*) artifacts="$$release/artifacts/synchro-maven-$(VERSION).zip" ;; \
			SUP-RN-IOS-*) artifacts="$$release/release-manifest.json $$release/artifacts/trainstar-synchro-react-native-$(VERSION).tgz" ;; \
			SUP-RN-ANDROID-*) artifacts="$$release/artifacts/synchro-maven-$(VERSION).zip $$release/artifacts/trainstar-synchro-react-native-$(VERSION).tgz" ;; \
			esac; \
			hashes=""; for artifact in $$artifacts; do hashes="$$hashes $$(shasum -a 256 "$$artifact" | cut -d ' ' -f 1)"; done; \
			$(MAKE) --no-print-directory test-client-platforms \
				SUPPORT_CELL_ID="$(SUPPORT_CELL_ID)" SUPPORT_PLATFORM_VERSION="$(SUPPORT_PLATFORM_VERSION)" \
				CLIENT_ARTIFACT_DIR="$(abspath $(RELEASE_CONSUMER_DIR))" CLIENT_ARTIFACTS_PREPARED=1 \
				PACKAGED_SMOKE_CELL_DIR="$(abspath $(RELEASE_EVIDENCE_DIR))/cells" \
				PACKAGED_SMOKE_DISTRIBUTION_ARTIFACTS="$$artifacts" PACKAGED_SMOKE_EXPECTED_ARTIFACT_HASHES="$$hashes" \
				SYNCHRO_CONSUMER_RESOLUTION=prepublication \
				SYNCHRO_PREPUBLICATION_GIT_URL="file://$(abspath $(RELEASE_CONSUMER_DIR))/source.git" ;; \
		*) echo "unsupported required release support cell: $(SUPPORT_CELL_ID)" >&2; exit 1 ;; \
		esac
	@$(MAKE) --no-print-directory release-verify VERSION="$(VERSION)" RELEASE_DIR="$(abspath $(RELEASE_DIR))"

test-release-artifacts:
	@PYTHONPYCACHEPREFIX="$(PACKAGED_SMOKE_TMP_ROOT)/python-cache" python3 -m unittest scripts.ci.test_release_artifacts

test-release-publish:
	@PYTHONPYCACHEPREFIX="$(PACKAGED_SMOKE_TMP_ROOT)/python-cache" python3 -m unittest scripts.ci.test_release_publish

test-server-consumer-helper:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -dir ../verification/consumers/server -- env GO111MODULE=off go test -json -count=1

test-consumer-go:
	sh verification/consumers/go/test-consumer.sh "$(CURDIR)" "$(CURRENT_VERSION)"

lint-rn:
	cd clients/react-native && yarn typecheck
	cd clients/react-native && yarn lint

test: test-rust-core test-adapter test-swift-unit test-kotlin-unit test-rn-unit verify-contract docs-build

build-swift-native-runner:
	cd clients/swift && $(SWIFTPM_GIT_ENV) swift build --product synchro-native-runner

build-kotlin-library:
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew $(GRADLE_TEST_ARGS) :synchro:compileDebugKotlin

build-kotlin-conformance-app:
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew $(GRADLE_TEST_ARGS) :conformance-app:assembleDebug :conformance-app:assembleDebugAndroidTest

test-swift-unit:
	rm -rf clients/swift/.build/test-results/unit.xcresult
	mkdir -p clients/swift/.build/test-results
	cd clients/swift && $(SWIFTPM_GIT_ENV) xcodebuild test -quiet -scheme Synchro-Package -destination 'platform=macOS' -skip-testing:SynchroTests/IntegrationTests -skip-testing:SynchroTests/SchemaIntegrationTests -skip-testing:SynchroTests/ClientSchemaIdentityTests -resultBundlePath .build/test-results/unit.xcresult
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult xcresult -path ../clients/swift/.build/test-results/unit.xcresult

test-client-schema-identity: conformance-mod-download
	@test -n "$(ADAPTER_TEST_URL)" || { echo "ADAPTER_TEST_URL is required" >&2; exit 1; }
	@set -e; \
		status=0; \
		if $(MAKE) --no-print-directory _test-client-schema-identity; then status=0; else status=$$?; fi; \
		rm -f clients/swift/.build/test-results/schema-identity-seed.db*; \
		exit $$status

_test-client-schema-identity:
	rm -f clients/swift/.build/test-results/schema-identity-seed.db*
	mkdir -p clients/swift/.build/test-results
	cd conformance && SYNCHRO_DDL_IDENTITY_SEED_PATH="$(CURDIR)/clients/swift/.build/test-results/schema-identity-seed.db" TEST_DATABASE_URL="$(ADAPTER_TEST_URL)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-dir ../api/go \
		-test TestCanonicalClientSeedMatchesSeedDBDDL \
		-expect target_pass \
		-- go test -tags ddlidentity -json ./seeddb -count=1 -run '^TestCanonicalClientSeedMatchesSeedDBDDL$$'
	rm -rf clients/swift/.build/test-results/schema-identity.xcresult
	cd clients/swift && $(SWIFTPM_GIT_ENV) xcodebuild test -quiet -scheme Synchro-Package -destination 'platform=macOS' \
		-only-testing:SynchroTests/ClientSchemaIdentityTests/testCanonicalGoSeedDDLConvergesWithFreshSwiftDDL \
		-resultBundlePath .build/test-results/schema-identity.xcresult
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult xcresult -path ../clients/swift/.build/test-results/schema-identity.xcresult
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	rm -rf clients/kotlin/synchro/build/test-results
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" $(TEST_ENV) SYNCHRO_TEST_SEED_PATH="$(CURDIR)/clients/swift/.build/test-results/schema-identity-seed.db" ./gradlew $(GRADLE_TEST_ARGS) -PsynchroTestSuite=integration :synchro:testDebugUnitTest --tests 'com.trainstar.synchro.SchemaIntegrationTests.testCanonicalGoSeedDDLConvergesWithFreshKotlinDDL'
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult junit -path ../clients/kotlin/synchro/build/test-results

test-swift-warm-connect: conformance-mod-download build-swift-native-runner
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		runner_dir="$$(cd clients/swift && $(SWIFTPM_GIT_ENV) swift build --show-bin-path)"; \
		test -x "$$runner_dir/synchro-native-runner"; \
		cd conformance; \
		SYNCHRO_SWIFT_NATIVE_RUNNER="$$runner_dir/synchro-native-runner" \
			GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
			-test TestRealSwiftWarmConnect \
			-expect target_pass \
			-- go test -tags swiftintegration -json ./swift -count=1 -timeout=10m \
			-run '^TestRealSwiftWarmConnect$$' -args --provision --install

test-swift-scenarios: conformance-mod-download build-swift-native-runner build-seed
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		runner_dir="$$(cd clients/swift && $(SWIFTPM_GIT_ENV) swift build --show-bin-path)"; \
		test -x "$$runner_dir/synchro-native-runner"; \
		test -x "$(CURDIR)/$(SEED_BINARY)"; \
		cd conformance; \
		SYNCHRO_SWIFT_NATIVE_RUNNER="$$runner_dir/synchro-native-runner" \
		SYNCHRO_SEED_TOOL="$(CURDIR)/$(SEED_BINARY)" \
			GOFLAGS= GOWORK=off go run ./cmd/testresult suite \
			-- go test -tags swiftintegration -json ./swift -count=1 -timeout=30m \
			-run '^TestRealSwiftScenarios$$' $(GO_TEST_ARGS) -args --provision --install

test-swift: test-swift-warm-connect test-swift-scenarios
	$(MAKE) --no-print-directory REFRESH_RN_SEED=1 REFRESH_RN_SEED_OUTPUT="$(CLIENT_INTEGRATION_SEED)" synchrod-pg-test-restart
	rm -rf clients/swift/.build/integration-derived-data clients/swift/.build/test-results/integration.xcresult
	mkdir -p clients/swift/.build/test-results
	cd clients/swift && $(SWIFTPM_GIT_ENV) xcodebuild build-for-testing -quiet -scheme Synchro-Package -destination 'platform=macOS' -derivedDataPath .build/integration-derived-data
	@set -eu; \
		set -- clients/swift/.build/integration-derived-data/Build/Products/*.xctestrun; \
		test "$$#" -eq 1 && test -f "$$1"; \
		xctestrun="$$1"; \
		environment_path='TestConfigurations.0.TestTargets.0.EnvironmentVariables'; \
		if plutil -type "$$environment_path" "$$xctestrun" >/dev/null 2>&1; then \
			plutil -replace "$$environment_path" -dictionary "$$xctestrun"; \
		else \
			plutil -insert "$$environment_path" -dictionary "$$xctestrun"; \
		fi; \
		plutil -insert "$$environment_path.TEST_DATABASE_URL" -string "$(ADAPTER_TEST_URL)" "$$xctestrun"; \
		plutil -insert "$$environment_path.TEST_REPLICATION_URL" -string "$(REPLICATION_URL)" "$$xctestrun"; \
		plutil -insert "$$environment_path.SYNCHRO_TEST_URL" -string "$(SYNCHRO_TEST_URL)" "$$xctestrun"; \
		plutil -insert "$$environment_path.SYNCHRO_TEST_JWT_SECRET" -string "$(SYNCHRO_TEST_JWT_SECRET)" "$$xctestrun"; \
		plutil -insert "$$environment_path.SYNCHRO_TEST_SEED_PATH" -string "$(CLIENT_INTEGRATION_SEED)" "$$xctestrun"; \
		xcodebuild test-without-building -quiet -xctestrun "$$xctestrun" -destination 'platform=macOS' -skip-testing:SynchroTests/ClientSchemaIdentityTests -resultBundlePath clients/swift/.build/test-results/integration.xcresult
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult xcresult -path ../clients/swift/.build/test-results/integration.xcresult

test-kotlin-unit:
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	rm -rf clients/kotlin/synchro/build/test-results
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew $(GRADLE_TEST_ARGS) -PsynchroTestSuite=unit :synchro:test
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult junit -path ../clients/kotlin/synchro/build/test-results

test-kotlin-warm-connect: conformance-mod-download build-kotlin-conformance-app
	@test -x "$(ANDROID_HOME)/platform-tools/adb" || (echo "adb not found at $(ANDROID_HOME)/platform-tools/adb"; exit 1)
	@test -n "$(KOTLIN_ANDROID_SERIAL)" || (echo "Set KOTLIN_ANDROID_SERIAL to one booted Android device."; exit 1)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		application_apk="$(CURDIR)/clients/kotlin/conformance-app/build/outputs/apk/debug/conformance-app-debug.apk"; \
		instrumentation_apk="$(CURDIR)/clients/kotlin/conformance-app/build/outputs/apk/androidTest/debug/conformance-app-debug-androidTest.apk"; \
		test -f "$$application_apk"; \
		test -f "$$instrumentation_apk"; \
		cd conformance; \
		SYNCHRO_KOTLIN_ADB="$(ANDROID_HOME)/platform-tools/adb" \
			SYNCHRO_KOTLIN_DEVICE_SERIAL="$(KOTLIN_ANDROID_SERIAL)" \
			SYNCHRO_KOTLIN_APPLICATION_APK="$$application_apk" \
			SYNCHRO_KOTLIN_INSTRUMENTATION_APK="$$instrumentation_apk" \
			GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
			-test TestRealKotlinWarmConnect \
			-expect target_pass \
			-- go test -tags kotlinintegration -json ./kotlin -count=1 -timeout=12m \
			-run '^TestRealKotlinWarmConnect$$' -args --provision --install

test-kotlin-scenarios: conformance-mod-download build-kotlin-conformance-app build-seed
	@test -x "$(ANDROID_HOME)/platform-tools/adb" || (echo "adb not found at $(ANDROID_HOME)/platform-tools/adb"; exit 1)
	@test -n "$(KOTLIN_ANDROID_SERIAL)" || (echo "Set KOTLIN_ANDROID_SERIAL to one booted Android device."; exit 1)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		application_apk="$(CURDIR)/clients/kotlin/conformance-app/build/outputs/apk/debug/conformance-app-debug.apk"; \
		instrumentation_apk="$(CURDIR)/clients/kotlin/conformance-app/build/outputs/apk/androidTest/debug/conformance-app-debug-androidTest.apk"; \
		test -f "$$application_apk"; \
		test -f "$$instrumentation_apk"; \
		test -x "$(CURDIR)/$(SEED_BINARY)"; \
		cd conformance; \
		SYNCHRO_KOTLIN_ADB="$(ANDROID_HOME)/platform-tools/adb" \
			SYNCHRO_KOTLIN_DEVICE_SERIAL="$(KOTLIN_ANDROID_SERIAL)" \
			SYNCHRO_KOTLIN_APPLICATION_APK="$$application_apk" \
			SYNCHRO_KOTLIN_INSTRUMENTATION_APK="$$instrumentation_apk" \
			SYNCHRO_SEED_TOOL="$(CURDIR)/$(SEED_BINARY)" \
			GOFLAGS= GOWORK=off go run ./cmd/testresult suite \
			-- go test -tags kotlinintegration -json ./kotlin -count=1 -timeout=75m \
			-run '^TestRealKotlinScenarios$$' $(GO_TEST_ARGS) -args --provision --install

test-kotlin-instrumentation: build-kotlin-conformance-app
	@test -x "$(ANDROID_HOME)/platform-tools/adb" || (echo "adb not found at $(ANDROID_HOME)/platform-tools/adb"; exit 1)
	@test -n "$(KOTLIN_ANDROID_SERIAL)" || (echo "Set KOTLIN_ANDROID_SERIAL to one booted Android device."; exit 1)
	rm -rf clients/kotlin/conformance-app/build/outputs/androidTest-results/connected
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew $(GRADLE_TEST_ARGS) -Pandroid.injected.device.serial="$(KOTLIN_ANDROID_SERIAL)" -Pandroid.testInstrumentationRunnerArguments.notClass=com.trainstar.synchro.conformance.NativeSessionInstrumentationTest :conformance-app:connectedDebugAndroidTest
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult junit -path ../clients/kotlin/conformance-app/build/outputs/androidTest-results/connected

test-kotlin: test-kotlin-warm-connect test-kotlin-scenarios
	$(MAKE) --no-print-directory REFRESH_RN_SEED=1 REFRESH_RN_SEED_OUTPUT="$(CLIENT_INTEGRATION_SEED)" synchrod-pg-test-restart
	# Repeat preparation to prove that the integration fixture is idempotent.
	$(MAKE) --no-print-directory REFRESH_RN_SEED=1 REFRESH_RN_SEED_OUTPUT="$(CLIENT_INTEGRATION_SEED)" synchrod-pg-test-restart
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	rm -rf clients/kotlin/synchro/build/test-results
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" $(TEST_ENV) SYNCHRO_TEST_SEED_PATH="$(CLIENT_INTEGRATION_SEED)" ./gradlew $(GRADLE_TEST_ARGS) -PsynchroTestSuite=integration :synchro:test
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult junit -path ../clients/kotlin/synchro/build/test-results

test-kotlin-integration: test-kotlin

test-rn-unit:
	rm -f clients/react-native/example/artifacts/unit-test-results.json
	mkdir -p clients/react-native/example/artifacts
	cd clients/react-native && yarn test:unit --json --outputFile example/artifacts/unit-test-results.json
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult jest -path ../clients/react-native/example/artifacts/unit-test-results.json

test-rn-android-parity: rn-seed-asset release-kotlin-local
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example/android && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew :trainstar_synchro-react-native:clean :trainstar_synchro-react-native:generateCodegenArtifactsFromSchema :trainstar_synchro-react-native:compileDebugKotlin --rerun-tasks

test-rn-ios-parity: rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && xcodebuild -quiet -workspace ios/SynchroReactNativeExample.xcworkspace -scheme SynchroReactNative -configuration Debug -sdk iphonesimulator -destination 'generic/platform=iOS Simulator' -derivedDataPath ios/build/parity ONLY_ACTIVE_ARCH=YES clean build

test-rn-native-parity:
	@$(MAKE) test-rn-android-parity
	@$(MAKE) test-rn-ios-parity

test-rn-warm-connect-control: conformance-mod-download
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestWarmConnectScopeAuthorityNegativeControl \
		-expect target_pass \
		-- go test -json ./reactnative -count=1 \
		-run '^TestWarmConnectScopeAuthorityNegativeControl$$'

test-rn-warm-connect-ios: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeWarmConnectIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=15m \
			-run '^TestRealReactNativeWarmConnectIOS$$' -args --provision --install

test-rn-performance-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeSteadyPullIOS \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
		-run '^TestRealReactNativeSteadyPullIOS$$' -args --provision --install

test-rn-performance-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeSteadyPullAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
		-run '^TestRealReactNativeSteadyPullAndroid$$' -args --provision --install

test-rn-pending-cycle-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativePendingCycleIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativePendingCycleIOS$$' -args --provision --install

test-rn-pending-cycle-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativePendingCycleAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativePendingCycleAndroid$$' -args --provision --install

test-rn-provenance-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeMultiScopeProvenanceAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeMultiScopeProvenanceAndroid$$' -args --provision --install

test-rn-push-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativePushResponseLossAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativePushResponseLossAndroid$$' -args --provision --install

test-rn-push-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativePushResponseLossIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativePushResponseLossIOS$$' -args --provision --install

test-rn-retention-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeRetentionReconnectAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeRetentionReconnectAndroid$$' -args --provision --install

test-rn-retention-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeRetentionReconnectIOS \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeRetentionReconnectIOS$$' -args --provision --install

test-rn-check-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeSchemaCheckAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeSchemaCheckAndroid$$' -args --provision --install

test-rn-check-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeSchemaCheckIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeSchemaCheckIOS$$' -args --provision --install

test-rn-requests-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeRebuildRequestsAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeRebuildRequestsAndroid$$' -args --provision --install

test-rn-requests-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeRebuildRequestsIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeRebuildRequestsIOS$$' -args --provision --install

test-rn-forged-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeForgedCursorAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeForgedCursorAndroid$$' -args --provision --install

test-rn-forged-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeForgedCursorIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeForgedCursorIOS$$' -args --provision --install

test-rn-sqm-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeSchemaQueuedMutationAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeSchemaQueuedMutationAndroid$$' -args --provision --install

test-rn-sqm-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeSchemaQueuedMutationIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeSchemaQueuedMutationIOS$$' -args --provision --install

test-rn-cardinality-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeRebuildCardinalityAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeRebuildCardinalityAndroid$$' -args --provision --install

test-rn-cardinality-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeRebuildCardinalityIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeRebuildCardinalityIOS$$' -args --provision --install

test-rn-provenance-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeMultiScopeProvenanceIOS \
		-expect target_pass \
			-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeMultiScopeProvenanceIOS$$' -args --provision --install

test-rn-seeded-empty-startup-ios: conformance-mod-download test-blackbox-harness build-seed rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeSeededEmptyStartupIOS \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=50m \
		-run '^TestRealReactNativeSeededEmptyStartupIOS$$' -args --provision --install

test-rn-seeded-empty-startup-android: conformance-mod-download test-blackbox-harness build-seed test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install." >&2; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install." >&2; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeSeededEmptyStartupAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=50m \
		-run '^TestRealReactNativeSeededEmptyStartupAndroid$$' -args --provision --install

test-rn-queue-replay-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeQueueReplayIOS \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeQueueReplayIOS$$' -args --provision --install

test-rn-queue-replay-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install." >&2; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install." >&2; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeQueueReplayAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeQueueReplayAndroid$$' -args --provision --install

test-rn-rebuild-apply-ios: conformance-mod-download test-blackbox-harness rn-seed-asset rn-watchman-reset rn-ios-pods
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeRebuildApplyIOS \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeRebuildApplyIOS$$' -args --provision --install

test-rn-rebuild-apply-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeRebuildApplyAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=35m \
			-run '^TestRealReactNativeRebuildApplyAndroid$$' -args --provision --install

test-rn-warm-connect-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		cd conformance && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeWarmConnectAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=25m \
		-run '^TestRealReactNativeWarmConnectAndroid$$' -args --provision --install

verify-rn-seed:
	@cd clients/react-native/example && shasum -a 256 -c seed.db.sha256

refresh-rn-seed:
	@$(MAKE) REFRESH_RN_SEED=1 synchrod-pg-test-restart

rn-seed-asset: verify-rn-seed
	@test -f "$(RN_PINNED_SEED)" || (echo "Missing $(RN_PINNED_SEED) bundled seed asset"; exit 1)
	@mkdir -p "$(dir $(RN_CONSUMER_SEED))" "$(dir $(RN_ANDROID_SEED_ASSET))"
	@if ! cmp -s "$(RN_PINNED_SEED)" "$(RN_CONSUMER_SEED)" 2>/dev/null; then \
		cp "$(RN_PINNED_SEED)" "$(RN_CONSUMER_SEED)"; \
	fi
	@if ! cmp -s "$(RN_CONSUMER_SEED)" "$(RN_ANDROID_SEED_ASSET)" 2>/dev/null; then \
		cp "$(RN_CONSUMER_SEED)" "$(RN_ANDROID_SEED_ASSET)"; \
	fi

rn-e2e-server-seed: synchrod-pg-test-restart
	@set -eu; \
		final="$(CURDIR)/$(RN_CONSUMER_SEED)"; \
		temporary="$$final.tmp"; \
		mkdir -p "$$(dirname "$$final")" "$(CURDIR)/$(dir $(RN_ANDROID_SEED_ASSET))"; \
		rm -f "$$temporary" "$$temporary-wal" "$$temporary-shm"; \
		trap 'rm -f "$$temporary" "$$temporary-wal" "$$temporary-shm"' EXIT HUP INT TERM; \
		DATABASE_URL="$(ADAPTER_TEST_URL)" "$(CURDIR)/$(SEED_BINARY)" --output "$$temporary" --overwrite; \
		mv "$$temporary" "$$final"; \
		cp "$$final" "$(CURDIR)/$(RN_ANDROID_SEED_ASSET)"; \
		trap - EXIT HUP INT TERM

rn-watchman-reset:
	@if command -v watchman >/dev/null 2>&1; then \
		watchman watch-del "$(PWD)/clients/react-native" >/dev/null 2>&1 || true; \
		watchman watch-project "$(PWD)/clients/react-native" >/dev/null; \
	fi

rn-ios-pods:
	cd clients/react-native/example/ios && pod install

rn-android-emulator-reset:
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	@test -x "$(ANDROID_HOME)/platform-tools/adb" || (echo "adb not found at $(ANDROID_HOME)/platform-tools/adb"; exit 1)
	@ADB="$(ANDROID_HOME)/platform-tools/adb"; \
	SERIALS="$$($$ADB devices | awk '/^emulator-/{print $$1}')"; \
	for serial in $$SERIALS; do \
		AVD_NAME="$$($$ADB -s $$serial emu avd name 2>/dev/null | tr -d '\r' | head -n1)"; \
		if [ "$$AVD_NAME" = "Pixel_7_API_34" ]; then \
			echo "Stopping Android emulator $$serial ($$AVD_NAME)"; \
			$$ADB -s $$serial emu kill >/dev/null 2>&1 || true; \
		fi; \
	done; \
	if [ -n "$$SERIALS" ]; then \
		sleep 5; \
	fi

test-rn-e2e-ios-build: rn-watchman-reset rn-ios-pods
	@$(MAKE) rn-e2e-server-seed
	cd clients/react-native/example && npx detox build --configuration ios.sim.debug

test-rn-e2e-ios-run:
	rm -f clients/react-native/example/artifacts/ios-test-results.json
	mkdir -p clients/react-native/example/artifacts
	cd clients/react-native/example && \
		$(TEST_ENV) npx detox test --configuration ios.sim.debug $(DETOX_ARGS) --json --outputFile artifacts/ios-test-results.json
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult jest -path ../clients/react-native/example/artifacts/ios-test-results.json
	@$(MAKE) --no-print-directory test-rn-scenarios-ios

test-rn-e2e-ios:
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-ios-build
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-ios-run

test-rn-e2e-android-build:
	@$(MAKE) test-rn-android-parity
	@$(MAKE) rn-watchman-reset
	@$(MAKE) rn-android-emulator-reset
	@$(MAKE) rn-e2e-server-seed
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)

android-emulator-prepare:
	@test -x "$(ANDROID_HOME)/platform-tools/adb" || (echo "adb not found at $(ANDROID_HOME)/platform-tools/adb"; exit 1)
	@set -eu; \
		adb="$(ANDROID_HOME)/platform-tools/adb"; \
		serial="$${ANDROID_SERIAL:-$(KOTLIN_ANDROID_SERIAL)}"; \
		set -- "$$adb"; \
		if [ -n "$$serial" ]; then set -- "$$@" -s "$$serial"; fi; \
		"$$@" wait-for-device; \
		"$$@" shell svc power stayon true; \
		"$$@" shell settings put system screen_off_timeout 2147483647; \
		"$$@" shell settings put global hide_error_dialogs 1; \
		"$$@" shell locksettings set-disabled true >/dev/null; \
		"$$@" shell input keyevent 224; \
		"$$@" shell wm dismiss-keyguard; \
		"$$@" shell am broadcast -a android.intent.action.CLOSE_SYSTEM_DIALOGS >/dev/null; \
		"$$@" shell input keyevent 3; \
		home_component="$$("$$@" shell cmd package resolve-activity --brief -a android.intent.action.MAIN -c android.intent.category.HOME | tr -d '\r' | tail -n 1)"; \
		home_package="$${home_component%%/*}"; \
		test -n "$$home_package"; \
		for _ in $$(seq 1 30); do \
			if "$$@" shell dumpsys window | grep -F 'mCurrentFocus=' | grep -F "$$home_package" >/dev/null; then exit 0; fi; \
			sleep 1; \
		done; \
		echo "Android Home did not receive window focus" >&2; \
		"$$@" shell dumpsys power | grep -E 'mWakefulness=|mStayOn=' >&2 || true; \
		"$$@" shell dumpsys window | grep -E 'mCurrentFocus=|mFocusedApp=' >&2 || true; \
		exit 1

test-rn-e2e-android-run: android-emulator-prepare
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	rm -f clients/react-native/example/artifacts/android-test-results.json
	mkdir -p clients/react-native/example/artifacts
	@set +e; \
		cd clients/react-native/example && \
			ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
			JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
			$(TEST_ENV) npx detox test --configuration $(RN_ANDROID_DETOX_CONFIG) $(DETOX_ARGS) --json --outputFile artifacts/android-test-results.json; \
		status=$$?; \
		if [ "$$status" -ne 0 ]; then \
			adb="$(ANDROID_HOME)/platform-tools/adb"; \
			serial="$${ANDROID_SERIAL:-$(KOTLIN_ANDROID_SERIAL)}"; \
			set -- "$$adb"; \
			if [ -n "$$serial" ]; then set -- "$$@" -s "$$serial"; fi; \
			"$$@" shell dumpsys power | grep -E 'mWakefulness=|mStayOn=' >&2 || true; \
			"$$@" shell dumpsys window | grep -E 'mCurrentFocus=|mFocusedApp=' >&2 || true; \
		fi; \
		exit "$$status"
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult jest -path ../clients/react-native/example/artifacts/android-test-results.json
	@$(MAKE) --no-print-directory test-rn-scenarios-android

.PHONY: test-rn-scenarios-ios test-rn-scenarios-android
test-rn-scenarios-ios test-rn-scenarios-android: conformance-mod-download
	@set -eu; \
		case "$@" in \
			test-rn-scenarios-ios) platform=IOS; configuration=ios.sim.debug ;; \
			test-rn-scenarios-android) platform=Android; configuration="$(RN_ANDROID_DETOX_CONFIG)"; \
				export ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)"; \
				export JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ;; \
		esac; \
		cd conformance; \
		SYNCHRO_RN_DETOX_CONFIGURATION="$$configuration" GOFLAGS= GOWORK=off \
			go run ./cmd/testresult suite -- go test -tags reactnativeintegration -json ./reactnative \
			-count=1 -timeout=120m -run "^TestRealReactNativeCorpus$$platform$$" -args --provision --install

test-rn-e2e-android:
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-android-build
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-android-run

test-rn:
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-ios
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-android

release-pods-check: version-check
	@command -v pod >/dev/null 2>&1 || (echo "CocoaPods CLI is required for release-pods-check."; exit 1)
	pod ipc spec Synchro.podspec >/dev/null
	$(SWIFTPM_GIT_ENV) swift package dump-package >/dev/null
	@echo "Apple package metadata validated."

release-kotlin-local: version-check
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew :synchro:publishToMavenLocal
	@echo "Published to mavenLocal."

release-npm-dry-run: version-check
	cd clients/react-native && corepack enable
	cd clients/react-native && yarn install --immutable
	cd clients/react-native && npm pack --dry-run

client-consumer-apple-artifact: version-check release-pods-check
	@set -eu; \
		if [ "$(CLIENT_ARTIFACTS_PREPARED)" = 1 ]; then \
			test -f "$(abspath $(CLIENT_ARTIFACT_DIR))/apple/Synchro/Package.swift"; \
			test -f "$(abspath $(CLIENT_ARTIFACT_DIR))/apple/synchro-spm-$(CURRENT_VERSION).tar.gz"; \
			exit 0; \
		fi; \
		if [ "$(RELEASE_STAGED_ARTIFACTS)" = 1 ]; then $(MAKE) --no-print-directory release-verify VERSION="$(CURRENT_VERSION)" RELEASE_DIR="$(abspath $(RELEASE_DIR))"; exit 0; fi; \
		final="$(abspath $(CLIENT_ARTIFACT_DIR))/apple"; \
		stage="$$final.tmp.$$$$"; \
		cleanup() { rm -rf "$$stage"; }; \
		trap cleanup EXIT HUP INT TERM; \
		rm -rf "$$stage"; \
		mkdir -p "$$stage/Synchro/clients/swift"; \
		cp Package.swift Synchro.podspec LICENSE "$$stage/Synchro/"; \
		cp -R clients/swift/Sources "$$stage/Synchro/clients/swift/"; \
		find "$$stage/Synchro" -exec touch -t 202601010000 {} +; \
		COPYFILE_DISABLE=1 tar -cf - -C "$$stage" Synchro | gzip -n > "$$stage/synchro-spm-$(CURRENT_VERSION).tar.gz"; \
		mkdir -p "$$(dirname "$$final")"; \
		rm -rf "$$final"; \
		mv "$$stage" "$$final"; \
		trap - EXIT HUP INT TERM

client-consumer-kotlin-artifact: version-check
	@set -eu; \
		if [ "$(CLIENT_ARTIFACTS_PREPARED)" = 1 ]; then \
			test -f "$(abspath $(CLIENT_ARTIFACT_DIR))/maven/fit/trainstar/synchro/$(CURRENT_VERSION)/synchro-$(CURRENT_VERSION).aar"; \
			exit 0; \
		fi; \
		if [ "$(RELEASE_STAGED_ARTIFACTS)" = 1 ]; then $(MAKE) --no-print-directory release-verify VERSION="$(CURRENT_VERSION)" RELEASE_DIR="$(abspath $(RELEASE_DIR))"; exit 0; fi; \
		test -n "$(ANDROID_JAVA_HOME)" || { echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1; }; \
		test -d "$(ANDROID_HOME)" || { echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1; }; \
		final="$(abspath $(CLIENT_ARTIFACT_DIR))/maven"; \
		stage="$$final.tmp.$$$$"; \
		cleanup() { rm -rf "$$stage"; }; \
		trap cleanup EXIT HUP INT TERM; \
		rm -rf "$$stage"; \
		mkdir -p "$$stage"; \
		(cd clients/kotlin && \
			ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
			JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
			SYNCHRO_CONSUMER_MAVEN_REPOSITORY="$$stage" \
			./gradlew -Pversion="$(CURRENT_VERSION)" :synchro:publishAllPublicationsToConsumerRepository); \
		test -f "$$stage/fit/trainstar/synchro/$(CURRENT_VERSION)/synchro-$(CURRENT_VERSION).aar"; \
		mkdir -p "$$(dirname "$$final")"; \
		rm -rf "$$final"; \
		mv "$$stage" "$$final"; \
		trap - EXIT HUP INT TERM

client-consumer-rn-artifact: version-check
	@set -eu; \
		if [ "$(CLIENT_ARTIFACTS_PREPARED)" = 1 ]; then \
			test -f "$(abspath $(CLIENT_ARTIFACT_DIR))/npm/trainstar-synchro-react-native-$(CURRENT_VERSION).tgz"; \
			exit 0; \
		fi; \
		if [ "$(RELEASE_STAGED_ARTIFACTS)" = 1 ]; then $(MAKE) --no-print-directory release-verify VERSION="$(CURRENT_VERSION)" RELEASE_DIR="$(abspath $(RELEASE_DIR))"; exit 0; fi; \
		final="$(abspath $(CLIENT_ARTIFACT_DIR))/npm"; \
		stage="$$final.tmp.$$$$"; \
		cleanup() { rm -rf "$$stage"; }; \
		trap cleanup EXIT HUP INT TERM; \
		rm -rf "$$stage"; \
		mkdir -p "$$stage"; \
		(cd clients/react-native && \
			corepack enable >/dev/null 2>&1 && \
			yarn install --immutable && \
			yarn prepare && \
			npm pack --ignore-scripts --silent --pack-destination "$$stage"); \
		test -f "$$stage/trainstar-synchro-react-native-$(CURRENT_VERSION).tgz"; \
		mkdir -p "$$(dirname "$$final")"; \
		rm -rf "$$final"; \
		mv "$$stage" "$$final"; \
		trap - EXIT HUP INT TERM

client-consumer-artifacts: client-consumer-apple-artifact client-consumer-kotlin-artifact client-consumer-rn-artifact
	@set -eu; \
		final="$(abspath $(CLIENT_ARTIFACT_DIR))"; \
		(cd "$$final" && find . -type f ! -name artifacts.sha256 -print0 | LC_ALL=C sort -z | xargs -0 shasum -a 256 > artifacts.sha256); \
		echo "Client consumer artifacts ready at $$final"

local-consumer-artifacts: client-consumer-artifacts

test-consumer-swift: client-consumer-apple-artifact
	@set -eu; \
		artifact="$(abspath $(CLIENT_ARTIFACT_DIR))/apple/Synchro"; \
		mkdir -p "$(PACKAGED_SMOKE_TMP_ROOT)"; \
		tmp="$$(mktemp -d "$(PACKAGED_SMOKE_TMP_ROOT)/synchro-swift-consumer.XXXXXX")"; \
		trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
		$(SWIFTPM_GIT_ENV) \
		SYNCHRO_SWIFT_PACKAGE_PATH="$$artifact" swift package \
			--package-path verification/consumers/swift \
			--scratch-path "$$tmp/build" \
			--disable-dependency-cache \
			show-dependencies --format json > "$$tmp/dependencies.json"; \
		grep -F "$$artifact" "$$tmp/dependencies.json" >/dev/null; \
		if grep -F "$(CURDIR)/clients/swift" "$$tmp/dependencies.json" >/dev/null; then \
			echo "Swift consumer resolved workspace client sources" >&2; \
			exit 1; \
		fi; \
		$(SWIFTPM_GIT_ENV) \
		SYNCHRO_SWIFT_PACKAGE_PATH="$$artifact" swift run \
			--package-path verification/consumers/swift \
			--scratch-path "$$tmp/build" \
			--disable-dependency-cache \
			SynchroConsumer

test-consumer-swift-smoke: client-consumer-apple-artifact
	PACKAGED_SMOKE_TMP_ROOT="$(PACKAGED_SMOKE_TMP_ROOT)" \
		sh verification/consumers/swift/test-consumer.sh \
			"$(CURDIR)" "$(abspath $(CLIENT_ARTIFACT_DIR))" \
			"$(PACKAGED_SMOKE_CELL_ID)" "$(PACKAGED_SMOKE_CELL_RESULT)"

test-consumer-swift-ios: client-consumer-apple-artifact
	SUPPORT_PLATFORM_VERSION="$(SUPPORT_PLATFORM_VERSION)" \
		sh verification/consumers/swift-ios/test-consumer.sh "$(abspath $(CLIENT_ARTIFACT_DIR))"

test-consumer-kotlin: client-consumer-kotlin-artifact
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	@set -eu; \
		mkdir -p "$(PACKAGED_SMOKE_TMP_ROOT)"; \
		tmp="$$(mktemp -d "$(PACKAGED_SMOKE_TMP_ROOT)/synchro-kotlin-consumer.XXXXXX")"; \
		trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
		SYNCHRO_CONSUMER_MAVEN_REPOSITORY="$(abspath $(CLIENT_ARTIFACT_DIR))/maven" \
		ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
		JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
		clients/kotlin/gradlew --project-dir verification/consumers/kotlin --no-daemon \
			-PsynchroVersion="$(CURRENT_VERSION)" \
			:app:assembleDebug :app:assembleDebugAndroidTest; \
		SYNCHRO_CONSUMER_MAVEN_REPOSITORY="$(abspath $(CLIENT_ARTIFACT_DIR))/maven" \
		ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
		JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
		clients/kotlin/gradlew --project-dir verification/consumers/kotlin --no-daemon \
			-PsynchroVersion="$(CURRENT_VERSION)" \
			:app:dependencyInsight --dependency fit.trainstar:synchro \
			--configuration debugRuntimeClasspath > "$$tmp/dependencies.txt"; \
		grep -F "fit.trainstar:synchro:$(CURRENT_VERSION)" "$$tmp/dependencies.txt" >/dev/null; \
		if grep -F "project :synchro" "$$tmp/dependencies.txt" >/dev/null; then \
			echo "Kotlin consumer resolved the workspace client project" >&2; \
			exit 1; \
		fi; \
		SYNCHRO_CONSUMER_MAVEN_REPOSITORY="$(abspath $(CLIENT_ARTIFACT_DIR))/maven" \
		ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
		JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
		sh verification/consumers/kotlin/test-internal-api-rejection.sh \
			"$(CURDIR)/clients/kotlin/gradlew" "$(CURRENT_VERSION)"

test-consumer-kotlin-device: client-consumer-kotlin-artifact
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	SYNCHRO_CONSUMER_MAVEN_REPOSITORY="$(abspath $(CLIENT_ARTIFACT_DIR))/maven" \
		ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
		JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
		clients/kotlin/gradlew --project-dir verification/consumers/kotlin --no-daemon \
			-PsynchroVersion="$(CURRENT_VERSION)" \
			:app:connectedDebugAndroidTest

test-consumer-kotlin-device-smoke: test-consumer-kotlin
	PACKAGED_SMOKE_TMP_ROOT="$(PACKAGED_SMOKE_TMP_ROOT)" \
		ANDROID_HOME="$(ANDROID_HOME)" KOTLIN_ANDROID_SERIAL="$(KOTLIN_ANDROID_SERIAL)" \
		sh verification/consumers/kotlin/test-consumer-device.sh \
			"$(CURDIR)" "$(abspath $(CLIENT_ARTIFACT_DIR))" \
			"$(PACKAGED_SMOKE_CELL_ID)" "$(PACKAGED_SMOKE_CELL_RESULT)" "$(CURRENT_VERSION)"

test-consumer-rn-ios: client-consumer-apple-artifact client-consumer-rn-artifact
	SUPPORT_PLATFORM_VERSION="$(SUPPORT_PLATFORM_VERSION)" \
		PACKAGED_SMOKE_TMP_ROOT="$(PACKAGED_SMOKE_TMP_ROOT)" \
		sh verification/consumers/react-native/test-consumer.sh ios "$(abspath $(CLIENT_ARTIFACT_DIR))" "$(CURRENT_VERSION)" build-only

test-consumer-rn-android: client-consumer-kotlin-artifact client-consumer-rn-artifact
	ANDROID_HOME="$(ANDROID_HOME)" ANDROID_JAVA_HOME="$(ANDROID_JAVA_HOME)" \
		PACKAGED_SMOKE_TMP_ROOT="$(PACKAGED_SMOKE_TMP_ROOT)" \
		sh verification/consumers/react-native/test-consumer.sh android "$(abspath $(CLIENT_ARTIFACT_DIR))" "$(CURRENT_VERSION)" build-only

test-consumer-rn-ios-smoke: client-consumer-apple-artifact client-consumer-rn-artifact
	SUPPORT_PLATFORM_VERSION="$(SUPPORT_PLATFORM_VERSION)" \
		PACKAGED_SMOKE_TMP_ROOT="$(PACKAGED_SMOKE_TMP_ROOT)" \
		PACKAGED_SMOKE_CELL_ID="$(PACKAGED_SMOKE_CELL_ID)" \
		PACKAGED_SMOKE_CELL_RESULT="$(PACKAGED_SMOKE_CELL_RESULT)" \
		sh verification/consumers/react-native/test-consumer.sh ios "$(abspath $(CLIENT_ARTIFACT_DIR))" "$(CURRENT_VERSION)"

test-consumer-rn-android-smoke: android-emulator-prepare client-consumer-kotlin-artifact client-consumer-rn-artifact
	ANDROID_HOME="$(ANDROID_HOME)" ANDROID_JAVA_HOME="$(ANDROID_JAVA_HOME)" \
		PACKAGED_SMOKE_TMP_ROOT="$(PACKAGED_SMOKE_TMP_ROOT)" \
		PACKAGED_SMOKE_CELL_ID="$(PACKAGED_SMOKE_CELL_ID)" \
		PACKAGED_SMOKE_CELL_RESULT="$(PACKAGED_SMOKE_CELL_RESULT)" \
		sh verification/consumers/react-native/test-consumer.sh android "$(abspath $(CLIENT_ARTIFACT_DIR))" "$(CURRENT_VERSION)"

test-client-platforms:
	@test -n "$(SUPPORT_CELL_ID)" || (echo "SUPPORT_CELL_ID is required" >&2; exit 1)
	@mkdir -p "$(PACKAGED_SMOKE_CELL_DIR)" "$(PACKAGED_SMOKE_TMP_ROOT)"
	@python3 verification/packaged_smoke.py begin-cell \
		--repo-root "$(CURDIR)" \
		--cell "$(SUPPORT_CELL_ID)" \
		--output "$(PACKAGED_SMOKE_CELL_DIR)/$(SUPPORT_CELL_ID).json"
	@set -eu; \
		$(WARM_CONNECT_ENV) \
		export SYNCHRO_TEST_URL="$(SYNCHRO_TEST_URL)"; \
		export SYNCHRO_TEST_JWT_SECRET="$(SYNCHRO_TEST_JWT_SECRET)"; \
		export PACKAGED_SMOKE_CELL_ID="$(SUPPORT_CELL_ID)"; \
		export PACKAGED_SMOKE_CELL_RESULT="$(PACKAGED_SMOKE_CELL_DIR)/$(SUPPORT_CELL_ID).json"; \
		case "$(SUPPORT_CELL_ID)" in \
		SUP-PG-LINUX-X64-001) \
			test "$$(uname -s)" = "Linux" && test "$$(uname -m)" = "x86_64" || { echo "linux-x64 is required" >&2; exit 1; }; \
			test -f "$${SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT:?SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT is required}/artifact-manifest.json"; \
			export PACKAGED_SMOKE_EXTRA_ARTIFACT="$$SYNCHRO_CONFORMANCE_EXTENSION_ARTIFACT/artifact-manifest.json"; \
			$(MAKE) test-consumer-kotlin-device-smoke ;; \
		SUP-IOS-MIN-001) \
			test "$(SUPPORT_PLATFORM_VERSION)" = "16" || { echo "SUPPORT_PLATFORM_VERSION must be 16" >&2; exit 1; }; \
			PACKAGED_SMOKE_CELL_ID="$$PACKAGED_SMOKE_CELL_ID" PACKAGED_SMOKE_CELL_RESULT="$$PACKAGED_SMOKE_CELL_RESULT" $(MAKE) test-consumer-swift-ios ;; \
		SUP-IOS-CURRENT-001) \
			test -n "$(SUPPORT_PLATFORM_VERSION)" || { echo "SUPPORT_PLATFORM_VERSION is required" >&2; exit 1; }; \
			PACKAGED_SMOKE_CELL_ID="$$PACKAGED_SMOKE_CELL_ID" PACKAGED_SMOKE_CELL_RESULT="$$PACKAGED_SMOKE_CELL_RESULT" $(MAKE) test-consumer-swift-ios ;; \
		SUP-MACOS-CURRENT-001) \
			test "$$(uname -s)" = "Darwin" || { echo "macOS is required" >&2; exit 1; }; \
			test -n "$(SUPPORT_PLATFORM_VERSION)" || { echo "SUPPORT_PLATFORM_VERSION is required" >&2; exit 1; }; \
			macos_version="$$(sw_vers -productVersion)"; \
			case "$(SUPPORT_PLATFORM_VERSION)" in *.*) test "$$macos_version" = "$(SUPPORT_PLATFORM_VERSION)" ;; *) test "$${macos_version%%.*}" = "$(SUPPORT_PLATFORM_VERSION)" ;; esac || { echo "macOS runtime does not match SUPPORT_PLATFORM_VERSION" >&2; exit 1; }; \
			$(MAKE) test-consumer-swift-smoke ;; \
		SUP-ANDROID-MIN-001) \
			test "$(SUPPORT_PLATFORM_VERSION)" = "24" || { echo "SUPPORT_PLATFORM_VERSION must be 24" >&2; exit 1; }; \
			test "$$($(ANDROID_HOME)/platform-tools/adb shell getprop ro.build.version.sdk | tr -d '\r')" = "24" || { echo "Android API 24 is required" >&2; exit 1; }; \
			$(MAKE) test-consumer-kotlin-device-smoke ;; \
		SUP-ANDROID-CURRENT-001|SUP-RN-ANDROID-CURRENT-001) \
			test -n "$(SUPPORT_PLATFORM_VERSION)" || { echo "SUPPORT_PLATFORM_VERSION is required" >&2; exit 1; }; \
			test "$$($(ANDROID_HOME)/platform-tools/adb shell getprop ro.build.version.sdk | tr -d '\r')" = "$(SUPPORT_PLATFORM_VERSION)" || { echo "Android runtime does not match SUPPORT_PLATFORM_VERSION" >&2; exit 1; }; \
			if [ "$(SUPPORT_CELL_ID)" = "SUP-ANDROID-CURRENT-001" ]; then $(MAKE) test-consumer-kotlin-device-smoke; else $(MAKE) test-consumer-rn-android-smoke; fi ;; \
		SUP-RN-IOS-CURRENT-001) \
			test -n "$(SUPPORT_PLATFORM_VERSION)" || { echo "SUPPORT_PLATFORM_VERSION is required" >&2; exit 1; }; \
			$(MAKE) test-consumer-rn-ios-smoke ;; \
		*) echo "unknown client support cell: $(SUPPORT_CELL_ID)" >&2; exit 1 ;; \
	esac

test-packaged-smoke:
	@python3 verification/packaged_smoke.py collect \
		--repo-root "$(CURDIR)" \
		--cells-dir "$(PACKAGED_SMOKE_CELL_DIR)" \
		--output "$(PACKAGED_SMOKE_EVIDENCE)"
	@python3 verification/packaged_smoke.py verify-summary \
		--repo-root "$(CURDIR)" \
		--summary "$(PACKAGED_SMOKE_EVIDENCE)"

test-packaged-smoke-structure:
	@mkdir -p "$(PACKAGED_SMOKE_TMP_ROOT)"
	TMPDIR="$(PACKAGED_SMOKE_TMP_ROOT)" \
		PYTHONPYCACHEPREFIX="$(PACKAGED_SMOKE_TMP_ROOT)/python-cache" \
		python3 verification/test_packaged_smoke.py

test-packaged-consumers: test-packaged-smoke-structure test-consumer-swift test-consumer-kotlin test-consumer-rn-ios test-consumer-rn-android

ext-build:
	cd extensions/synchro-pg && cargo build

generate-pg-sql:
	cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx schema pg18 --pg-config "$(PGRX_PG_CONFIG)" --out sql/synchro_pg--$(CURRENT_VERSION).sql
	perl -pi -e 's/[ \t]+$$//' extensions/synchro-pg/sql/synchro_pg--$(CURRENT_VERSION).sql
	perl -0pi -e 's/\n+\z/\n/' extensions/synchro-pg/sql/synchro_pg--$(CURRENT_VERSION).sql

check-pg-sql:
	@set -eu; \
		tmp="$$(mktemp -d "$${TMPDIR:-/tmp}/synchro-pg-sql.XXXXXX")"; \
		trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
		generated="$$tmp/synchro_pg--$(CURRENT_VERSION).sql"; \
		cd extensions/synchro-pg; \
		CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx schema pg18 --pg-config "$(PGRX_PG_CONFIG)" --out "$$generated"; \
		perl -pi -e 's/[ \t]+$$//' "$$generated"; \
		perl -0pi -e 's/\n+\z/\n/' "$$generated"; \
		if ! cmp -s sql/synchro_pg--$(CURRENT_VERSION).sql "$$generated"; then \
			diff -u sql/synchro_pg--$(CURRENT_VERSION).sql "$$generated" || true; \
			printf '%s\n' 'tracked PostgreSQL SQL differs from pgrx generation' >&2; \
			exit 1; \
		fi

ext-install:
	cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx install --pg-config "$(PGRX_PG_CONFIG)"

ext-test: test-rust-pg

ext-seed:
	python3 extensions/testdata/generate/generate.py

test-rust-core:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult rust -dir ../extensions -- cargo test -p synchro-core

test-rust-mutants:
	@command -v cargo-mutants >/dev/null || (echo "cargo-mutants 27.1.0 is required" >&2; exit 1)
	@test "$$(cargo mutants --version)" = "cargo-mutants 27.1.0" || (echo "cargo-mutants 27.1.0 is required" >&2; exit 1)
	cd extensions && SYNCHRO_REPO_ROOT="$(CURDIR)" cargo mutants \
		-p synchro-core \
		--config .cargo/mutants.toml \
		--baseline run \
		--jobs 4 \
		--timeout 120 \
		--no-shuffle

test-rust-mutants-broad:
	@command -v cargo-mutants >/dev/null || (echo "cargo-mutants 27.1.0 is required" >&2; exit 1)
	@test "$$(cargo mutants --version)" = "cargo-mutants 27.1.0" || (echo "cargo-mutants 27.1.0 is required" >&2; exit 1)
	cd extensions && SYNCHRO_REPO_ROOT="$(CURDIR)" cargo mutants \
		-p synchro-core \
		--no-config \
		--baseline run \
		--jobs 4 \
		--timeout 120 \
		--no-shuffle

test-integration-mutants: test-conformance-testresult
	sh conformance/mutants/integration_gate.sh "$(CURDIR)"

test-integration-mutants-broad: test-conformance-testresult
	INTEGRATION_MUTANTS_BROAD=1 sh conformance/mutants/integration_gate.sh "$(CURDIR)"

test-integration-mutant: test-conformance-testresult
	@test -n "$(INTEGRATION_MUTANT_ID)" || { echo "INTEGRATION_MUTANT_ID is required" >&2; exit 1; }
	sh conformance/mutants/integration_gate.sh "$(CURDIR)" "$(INTEGRATION_MUTANT_ID)"

test-rust-pg:
	cd conformance && GOFLAGS= GOWORK=off CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" go run ./cmd/testresult rust -dir ../extensions/synchro-pg -- cargo pgrx test $(PGRX_PG)

test-rust-pg-all:
	@for v in 14 15 16 17 18; do \
		echo "=== PG $$v ==="; \
		(cd conformance && GOFLAGS= GOWORK=off CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" go run ./cmd/testresult rust -dir ../extensions/synchro-pg -- cargo pgrx test pg$$v) || exit 1; \
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
	cd extensions && cargo clippy -p synchro-pg --features pg18,pg_test -- -D warnings

lint-rust: lint-rust-core lint-rust-pg

build-local-postgres:
	@mkdir -p "$(dir $(LOCAL_POSTGRES_BINARY))"
	cd conformance && GOFLAGS= GOWORK=off go build -o "$(abspath $(LOCAL_POSTGRES_BINARY))" ./cmd/synchro-local-postgres

local-postgres-start: build-local-postgres
	@test -x "$(CONFORMANCE_ADAPTER_ARTIFACT_DIR)/synchrod-pg" || $(MAKE) conformance-adapter-artifact
	@test -d "$(CONFORMANCE_EXTENSION_ARTIFACT)" || $(MAKE) conformance-pg18-extension-test-artifact
	@test -n "$(PGRX_PG_BIN_DIR)" || { echo "PGRX_PG_BIN_DIR is required" >&2; exit 1; }
	@test -x "$(PGRX_PG_BIN_DIR)"/initdb || { echo "PostgreSQL 18 binaries are required in $(PGRX_PG_BIN_DIR)" >&2; exit 1; }
	@set -eu; \
		state="$(LOCAL_POSTGRES_STATE_DIR)"; \
		mkdir -p "$$state"; \
		chmod 700 "$$state"; \
		if [ -f "$(LOCAL_POSTGRES_PID_FILE)" ] && kill -0 "$$(cat "$(LOCAL_POSTGRES_PID_FILE)")" 2>/dev/null; then \
			echo "local PostgreSQL provisioner already running"; \
			test -s "$(LOCAL_POSTGRES_URL_FILE)"; \
			exit 0; \
		fi; \
		rm -f "$(LOCAL_POSTGRES_PID_FILE)" "$(LOCAL_POSTGRES_URL_FILE)" "$(LOCAL_POSTGRES_LOG_FILE)"; \
		nohup "$(LOCAL_POSTGRES_BINARY)" start \
			--pg18-bin-dir "$(PGRX_PG_BIN_DIR)" \
			--extension-artifact "$(CONFORMANCE_EXTENSION_ARTIFACT)" \
			--adapter-artifact "$(CONFORMANCE_ADAPTER_ARTIFACT_DIR)/synchrod-pg" \
			--state-dir "$$state" \
			--temp-parent "$(CURDIR)/.ignore/r2/tmp" \
			--url-file "$(LOCAL_POSTGRES_URL_FILE)" \
			--attach-environment-file "$(LOCAL_POSTGRES_ATTACH_ENV_FILE)" \
			--listen "$(LOCAL_POSTGRES_LISTEN)" \
			>"$(LOCAL_POSTGRES_LOG_FILE)" 2>&1 </dev/null & \
		echo $$! >"$(LOCAL_POSTGRES_PID_FILE)"; \
		for attempt in $$(seq 1 180); do \
			if [ -s "$(LOCAL_POSTGRES_URL_FILE)" ]; then echo "local PostgreSQL provisioner ready"; exit 0; fi; \
			if ! kill -0 "$$(cat "$(LOCAL_POSTGRES_PID_FILE)")" 2>/dev/null; then \
				cat "$(LOCAL_POSTGRES_LOG_FILE)" >&2 || true; \
				rm -f "$(LOCAL_POSTGRES_PID_FILE)"; \
				exit 1; \
			fi; \
			sleep 1; \
		done; \
		echo "local PostgreSQL provisioner did not become ready" >&2; \
		cat "$(LOCAL_POSTGRES_LOG_FILE)" >&2 || true; \
		kill "$$(cat "$(LOCAL_POSTGRES_PID_FILE)")" 2>/dev/null || true; \
		rm -f "$(LOCAL_POSTGRES_PID_FILE)"; \
		exit 1

local-postgres-stop:
	@set -eu; \
		if [ -f "$(LOCAL_POSTGRES_PID_FILE)" ]; then \
			pid="$$(cat "$(LOCAL_POSTGRES_PID_FILE)")"; \
			if kill -0 "$$pid" 2>/dev/null; then \
				kill "$$pid"; \
				for attempt in $$(seq 1 30); do \
					if ! kill -0 "$$pid" 2>/dev/null; then break; fi; \
					sleep 1; \
				 done; \
				if kill -0 "$$pid" 2>/dev/null; then kill -9 "$$pid" 2>/dev/null || true; fi; \
				 echo "local PostgreSQL provisioner stopped"; \
			else \
				echo "local PostgreSQL provisioner is not running"; \
			fi; \
			rm -f "$(LOCAL_POSTGRES_PID_FILE)" "$(LOCAL_POSTGRES_URL_FILE)" "$(LOCAL_POSTGRES_ATTACH_ENV_FILE)"; \
		else \
			echo "local PostgreSQL provisioner is not running"; \
		fi

test-adapter:
	@test -n "$(ADAPTER_TEST_URL)" || { echo "ADAPTER_TEST_URL is required" >&2; exit 1; }
	@echo "Running adapter integration tests..."
	@set -e; \
	status=0; \
	if (cd conformance && GOFLAGS= GOWORK=off TEST_DATABASE_URL="$(ADAPTER_TEST_URL)" go run ./cmd/testresult suite -dir ../api/go -- go test -json $(GO_TEST_ARGS) $(GO_TEST_PKGS)); then \
		status=0; \
	else \
		status=$$?; \
	fi; \
	exit $$status

synchrod-pg-test-start: build build-seed verify-rn-seed
	@test -n "$(ADAPTER_TEST_URL)" || { echo "ADAPTER_TEST_URL is required" >&2; exit 1; }
	@set -e; \
	for PID in $$(lsof -tiTCP:$(SYNCHROD_PG_PORT) -sTCP:LISTEN 2>/dev/null); do \
		kill "$$PID" 2>/dev/null || true; \
		sleep 1; \
		if kill -0 "$$PID" 2>/dev/null; then kill -9 "$$PID" 2>/dev/null || true; fi; \
	done; \
	if [ -f "$(SYNCHROD_PG_PID_FILE)" ] && kill -0 "$$(cat "$(SYNCHROD_PG_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod-pg already running"; \
		exit 0; \
	fi; \
	echo "Preparing client integration database..."; \
	(cd conformance && DATABASE_URL="$(ADAPTER_TEST_URL)" GOFLAGS= GOWORK=off go run ./cmd/synchro-local-postgres prepare --repo-root ..); \
	echo "Starting synchrod-pg on :$(SYNCHROD_PG_PORT)..."; \
	MIN_CLIENT_VERSION="$(MIN_CLIENT_VERSION)" \
		DATABASE_URL="$(ADAPTER_TEST_URL)" \
		JWT_SECRET="$(SYNCHRO_TEST_JWT_SECRET)" \
		LISTEN_ADDR=":$(SYNCHROD_PG_PORT)" \
		SYNCHROD_ADAPTER_BINARY="$(CURDIR)/$(BINARY)" \
		SYNCHROD_ADAPTER_PID_FILE="$(SYNCHROD_PG_PID_FILE)" \
		SYNCHROD_ADAPTER_LOG_FILE="$(SYNCHROD_PG_LOG_FILE)" \
		scripts/ci/start-adapter.sh; \
	sleep 2; \
	if ! kill -0 "$$(cat "$(SYNCHROD_PG_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod-pg failed to start:"; \
		cat "$(SYNCHROD_PG_LOG_FILE)"; \
		rm -f "$(SYNCHROD_PG_PID_FILE)"; \
		exit 1; \
	fi; \
	HTTP_READY=0; \
	for attempt in $$(seq 1 30); do \
		if curl -fsS -o /dev/null "http://localhost:$(SYNCHROD_PG_PORT)/sync/schema" 2>/dev/null; then HTTP_READY=1; break; fi; \
		sleep 1; \
	done; \
	if [ "$$HTTP_READY" -ne 1 ]; then \
		echo "synchrod-pg HTTP schema endpoint did not become ready"; \
		cat "$(SYNCHROD_PG_LOG_FILE)" 2>/dev/null || true; \
		rm -f "$(SYNCHROD_PG_PID_FILE)"; \
		exit 1; \
	fi; \
	if [ "$(REFRESH_RN_SEED)" = "1" ]; then \
		seed_output="$(REFRESH_RN_SEED_OUTPUT)"; \
		echo "Refreshing client seed database..."; \
		if lsof "$$seed_output" "$$seed_output-wal" "$$seed_output-shm" >/dev/null 2>&1; then \
			echo "client seed database is in use"; \
			exit 1; \
		fi; \
		mkdir -p "$$(dirname "$$seed_output")"; \
		rm -f "$$seed_output-wal" "$$seed_output-shm"; \
		DATABASE_URL="$(ADAPTER_TEST_URL)" "$(CURDIR)/$(SEED_BINARY)" --output "$$seed_output" --overwrite || { \
			cat "$(SYNCHROD_PG_LOG_FILE)" 2>/dev/null || true; \
			rm -f "$(SYNCHROD_PG_PID_FILE)"; \
			exit 1; \
		}; \
		if [ "$$seed_output" = "$(CURDIR)/clients/react-native/example/seed.db" ]; then \
			cd "$(CURDIR)/clients/react-native/example"; \
			shasum -a 256 seed.db > seed.db.sha256; \
		fi; \
	fi; \
	echo "synchrod-pg running on http://localhost:$(SYNCHROD_PG_PORT)"

synchrod-pg-test-stop:
	@STOPPED=0; \
	if [ -f "$(SYNCHROD_PG_PID_FILE)" ]; then \
		PID="$$(cat "$(SYNCHROD_PG_PID_FILE)")"; \
		if kill -0 "$$PID" 2>/dev/null; then \
			kill "$$PID"; \
			sleep 1; \
			if kill -0 "$$PID" 2>/dev/null; then \
				kill -9 "$$PID" 2>/dev/null || true; \
			fi; \
			wait "$$PID" 2>/dev/null || true; \
			echo "synchrod-pg stopped"; \
			STOPPED=1; \
		fi; \
		rm -f "$(SYNCHROD_PG_PID_FILE)"; \
	fi; \
	for PID in $$(lsof -tiTCP:$(SYNCHROD_PG_PORT) -sTCP:LISTEN 2>/dev/null); do \
		kill "$$PID" 2>/dev/null || true; \
		sleep 1; \
		if kill -0 "$$PID" 2>/dev/null; then \
			kill -9 "$$PID" 2>/dev/null || true; \
		fi; \
		wait "$$PID" 2>/dev/null || true; \
		STOPPED=1; \
	done; \
	if [ "$$STOPPED" -eq 0 ]; then \
		echo "synchrod-pg not running"; \
	fi
synchrod-pg-test-restart: synchrod-pg-test-stop
	@$(MAKE) synchrod-pg-test-start

clean:
	rm -rf bin/ "$(SYNCHROD_PG_PID_FILE)" "$(SYNCHROD_PG_LOG_FILE)"

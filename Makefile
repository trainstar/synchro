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
	test-conformance-module \
	test-conformance-imports \
	test-conformance-contract \
	test-conformance-drivers \
	update-conformance-catalog \
	check-conformance-catalog \
	test-conformance-scenarios \
	test-vectors \
	test-reference \
	test-conformance-faults \
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
	conformance-pg18-extension-artifact \
	test-evidence \
	test-inventory \
	test-conformance \
	test-blackbox \
	rc-check-pg18 \
	evidence \
	lint-go \
	lint-rn \
	lint-rust-core \
	lint-rust-pg \
	lint-rust \
	test \
	test-rust-core \
	test-rust-mutants \
	test-integration-mutants \
	test-rust-pg \
	test-rust-pg-all \
	test-adapter \
	test-adapter-setup \
	test-adapter-teardown \
	adapter-db-external-probe \
	adapter-db-external-teardown \
	adapter-db-local-setup \
	adapter-db-local-teardown \
	ext-build \
	ext-install \
	ext-test \
	ext-seed \
	build-swift-native-runner \
	build-kotlin-conformance-app \
	test-swift-unit \
	test-client-schema-identity \
	_test-client-schema-identity \
	test-swift-warm-connect \
	test-swift \
	test-kotlin-unit \
	test-kotlin-warm-connect \
	test-kotlin-instrumentation \
	test-kotlin \
	test-kotlin-integration \
	test-rn-unit \
	test-rn-android-parity \
	test-rn-ios-parity \
	test-rn-native-parity \
	test-rn-warm-connect-control \
	test-rn-warm-connect-ios \
	test-rn-warm-connect-android \
	verify-rn-seed \
	refresh-rn-seed \
	rn-seed-asset \
	rn-e2e-server-seed \
	rn-watchman-reset \
	rn-ios-pods \
	rn-android-emulator-reset \
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
	validation-check \
	release-check \
	release-kotlin-local \
	release-npm-dry-run \
	client-consumer-apple-artifact \
	client-consumer-kotlin-artifact \
	client-consumer-rn-artifact \
	client-consumer-artifacts \
	local-consumer-artifacts \
	test-consumer-swift \
	test-consumer-swift-ios \
	test-consumer-kotlin \
	test-consumer-kotlin-device \
	test-consumer-rn-ios \
	test-consumer-rn-android \
	test-client-platforms \
	test-packaged-smoke \
	test-packaged-consumers \
	phase-5-check \
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
PGRX_PORT ?= 28818
PGRX_READY_TIMEOUT ?= 90
PGRX_PG_MAJOR := $(patsubst pg%,%,$(PGRX_PG))
PGRX_DATA_DIR ?= $(HOME)/.pgrx/data-$(PGRX_PG_MAJOR)
PGRX_SOCKET_DIR ?= $(HOME)/.pgrx
PGRX_ADMIN_HOST ?= $(if $(filter Darwin,$(shell uname -s)),$(PGRX_SOCKET_DIR),localhost)
PGRX_ADMIN_USER ?= $(shell id -un)
PGRX_LOG_FILE ?= $(HOME)/.pgrx/$(PGRX_PG_MAJOR).log
PGRX_AUTOSTART ?= on
PGRX_WORKER_LOGIN ?= synchro_test_worker_login
PGRX_PG_CONFIG ?= $(shell awk -F'"' '/^$(PGRX_PG)[[:space:]]*=/ { print $$2 }' $(HOME)/.pgrx/config.toml)
PGRX_PG_BIN_DIR ?= $(dir $(PGRX_PG_CONFIG))
PGRX_PSQL ?= $(PGRX_PG_BIN_DIR)psql
PGRX_TARGET_DIR ?= $(CURDIR)/.pgrx-target
MUTATION_CONTROL_TEST ?=
MUTATION_CONTROL_EXPECT ?= target_pass
TESTRESULT_TEST_NAME ?=
BLACKBOX_TEST_COUNT ?= 1
CONFORMANCE_ADAPTER_ARTIFACT_DIR ?= $(CURDIR)/dist/conformance/synchrod-pg-adapter
CONFORMANCE_EXTENSION_ARTIFACT ?= $(CURDIR)/dist/conformance/synchro-pg-pg18
ADAPTER_TEST_DB ?= synchro_adapter_test
ADAPTER_TEST_URL ?=
ifeq ($(strip $(ADAPTER_TEST_URL)),)
ADAPTER_TEST_URL := postgres://$(USER)@localhost:$(PGRX_PORT)/$(ADAPTER_TEST_DB)?sslmode=disable
ADAPTER_TEST_EXTERNAL := 0
REPLICATION_URL ?= postgres://$(USER)@localhost:$(PGRX_PORT)/$(ADAPTER_TEST_DB)?replication=database&sslmode=disable
else
ADAPTER_TEST_EXTERNAL := 1
REPLICATION_URL ?= $(ADAPTER_TEST_URL)
endif
ADAPTER_DB_SETUP := $(if $(filter 1,$(ADAPTER_TEST_EXTERNAL)),adapter-db-external-probe,adapter-db-local-setup)
ADAPTER_DB_TEARDOWN := $(if $(filter 1,$(ADAPTER_TEST_EXTERNAL)),adapter-db-external-teardown,adapter-db-local-teardown)
override R1_BENCHMARK_BASELINE := $(CURDIR)/conformance/blackbox/integration/testdata/r1-benchmark-baseline.json

SYNCHROD_PG_PORT ?= 8091
SYNCHRO_TEST_HOST ?= localhost
SYNCHRO_TEST_PORT ?= $(SYNCHROD_PG_PORT)
SYNCHRO_TEST_URL ?= http://$(SYNCHRO_TEST_HOST):$(SYNCHRO_TEST_PORT)
SYNCHRO_TEST_JWT_SECRET ?= test-secret-for-integration-tests
MIN_CLIENT_VERSION ?= 1.0.0
SYNCHROD_PG_PID_FILE ?= .synchrod-pg-test.pid
SYNCHROD_PG_LOG_FILE ?= .synchrod-pg-test.log

BINARY ?= bin/synchrod-pg
SEED_BINARY ?= bin/synchro-seed
RN_PINNED_SEED ?= clients/react-native/example/seed.db
RN_CONSUMER_SEED ?= clients/react-native/example/verification/seed.db
RN_ANDROID_SEED_ASSET ?= clients/react-native/example/android/app/src/main/assets/seed.db
GO_TEST_ARGS ?= -v -count=1 -p 1
GO_TEST_PKGS ?= ./...
GRADLE_TEST_ARGS ?= --rerun-tasks
CLIENT_ARTIFACT_DIR ?= $(CURDIR)/dist/local-consumer
LOCAL_CONSUMER_DIR ?= $(CLIENT_ARTIFACT_DIR)
CURRENT_VERSION := $(shell cat VERSION 2>/dev/null)
PHASE_5_EVIDENCE ?= $(CURDIR)/dist/verification/phase-5-summary.json
PHASE_5_INPUT ?= $(CURDIR)/dist/verification/phase-5-input.json
PACKAGED_SMOKE_EVIDENCE ?= $(CURDIR)/dist/verification/packaged-smoke-summary.json

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
	@echo "  test-conformance-module - Test standalone conformance module policy"
	@echo "  test-conformance-imports - Test standalone conformance import policy"
	@echo "  test-conformance-contract - Test strict contract loading and snapshots"
	@echo "  test-conformance-drivers - Test the plain Swift and Kotlin process drivers"
	@echo "  update-conformance-catalog - Write the deterministic scenario catalog"
	@echo "  check-conformance-catalog - Check the deterministic scenario catalog"
	@echo "  test-conformance-scenarios - Test strict scenario loading and catalog generation"
	@echo "  test-vectors          - Test canonical protocol 3 vectors"
	@echo "  test-reference        - Test the independent protocol 3 reference model"
	@echo "  test-conformance      - Run the independent protocol conformance suite"
	@echo "  test-inventory        - Test generated evidence inventory"
	@echo "  test-blackbox         - Run the packaged server black-box suite"
	@echo "  test-blackbox-configured-bounds - Run the real configured-limit measurement proof"
	@echo "  test-blackbox-mutation-control - Run one structured real mutation control"
	@echo "  record-r1-benchmark   - Record one R1 benchmark candidate"
	@echo "  test-r1-benchmark     - Compare R1 benchmark results with the tracked baseline"
	@echo "  rc-check-pg18         - Verify the packaged PostgreSQL 18 candidate"
	@echo "  evidence              - Generate and validate the Phase 5 CI summary"
	@echo "  lint-go               - Run Go formatting checks and go vet"
	@echo "  lint-rn               - Run React Native typecheck and ESLint"
	@echo "  lint-rust-core        - Run Rust fmt and clippy for the shared core"
	@echo "  lint-rust-pg          - Run Rust fmt and clippy for the PostgreSQL extension"
	@echo "  lint-rust             - Run all Rust fmt and clippy checks"
	@echo "  test                  - Run the default local validation set"
	@echo "  test-rust-core        - Run synchro-core unit tests"
	@echo "  test-rust-mutants     - Run targeted synchro-core mutation tests"
	@echo "  test-integration-mutants - Run curated production integration mutants"
	@echo "  test-rust-pg          - Run pgrx integration tests on PG 18"
	@echo "  test-rust-pg-all      - Run pgrx tests on PG 14 through PG 18"
	@echo "  test-adapter          - Run Go adapter integration tests (override GO_TEST_PKGS to focus)"
	@echo "                         Set ADAPTER_TEST_URL for an external PostgreSQL database"
	@echo "  build-swift-native-runner - Build the macOS native conformance process"
	@echo "  build-kotlin-conformance-app - Build the Android native conformance test APK"
	@echo "  test-swift-unit       - Run Swift unit tests"
	@echo "  test-swift-warm-connect - Run the direct Swift warm-connect scenario"
	@echo "  test-swift            - Run Swift integration tests against the local adapter"
	@echo "  test-kotlin-unit      - Run Kotlin unit tests"
	@echo "  test-kotlin-instrumentation - Run Android instrumentation on the selected device"
	@echo "  test-kotlin           - Run Kotlin integration tests against the local adapter"
	@echo "  test-rn-unit          - Run React Native Jest tests"
	@echo "  test-rn-android-parity - Regenerate the TurboModule spec and compile the Android implementation"
	@echo "  test-rn-ios-parity     - Compile the iOS implementation against the generated TurboModule spec"
	@echo "  test-rn-native-parity  - Compile both native implementations against one TurboModule spec"
	@echo "  test-rn-warm-connect-control - Run the exact React Native warm-connect negative control"
	@echo "  test-rn-warm-connect-ios - Run direct React Native warm-connect through the iOS bridge"
	@echo "  test-rn-warm-connect-android - Run direct React Native warm-connect through the Android bridge"
	@echo "  verify-rn-seed        - Verify the pinned React Native seed digest"
	@echo "  refresh-rn-seed       - Regenerate and pin the React Native seed"
	@echo "  test-rn-e2e-ios       - Run React Native Detox tests on iOS"
	@echo "  test-rn-e2e-android   - Run React Native Detox tests on Android ($(RN_ANDROID_DETOX_CONFIG))"
	@echo "  test-rn               - Run React Native Detox tests on both platforms"
	@echo "  rn-android-emulator-reset - Stop any running Pixel_7_API_34 emulator before Detox"
	@echo "  synchrod-pg-test-start   - Start the extension-backed test adapter"
	@echo "  synchrod-pg-test-stop    - Stop the extension-backed test adapter"
	@echo "  synchrod-pg-test-restart - Restart the extension-backed test adapter"
	@echo "  release-pods-check    - Validate Apple package metadata surfaces"
	@echo "  validation-check      - Combine JavaScript and Go contract gates with full validation"
	@echo "  release-check         - Run the full release validation matrix"
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
	@echo "  test-packaged-consumers - Run all packaged consumer checks"
	@echo "  phase-5-check         - Validate terminal support-cell and gate evidence"
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
	cd api/go && GOWORK=off go build -o ../../$(BINARY) ./cmd/synchrod-pg

build-seed:
	@mkdir -p "$(dir $(SEED_BINARY))"
	cd api/go && GOWORK=off go build -o ../../$(SEED_BINARY) ./cmd/synchro-seed

build-check:
	cd api/go && GOWORK=off go build ./...

run:
	cd api/go && GOWORK=off go run ./cmd/synchrod-pg

docs-build: verify-contract
	cd docs && npm run build

docs-dev:
	cd docs && npm run dev

# This target validates the JavaScript-authored contract. validation-check also
# runs the Go contract and scenario validators through test-conformance.
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

test-conformance-module:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./internal/importguard -count=1 -run '^TestModulePolicy$$'

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
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./scenarios/... ./nativeexecution ./nativeharness ./modelrunner ./cmd/synchro-conformance -count=1

test-vectors:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./vectors -count=1

test-reference:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./reference -count=1

test-conformance-faults:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./barriers ./faults -count=1

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
	@case "$(MUTATION_CONTROL_TEST)" in \
		TestRealMutationControlCursorAdvancement|TestRealMutationControlWALAcknowledgement|TestRealMutationControlMutationConservation|TestRealMutationControlChecksumCorrectness|TestRealMutationControlScopeIsolation|TestRealMutationControlProgressOrder) ;; \
		*) echo "MUTATION_CONTROL_TEST is not a supported mutation control" >&2; exit 1 ;; \
	esac
	@case "$(MUTATION_CONTROL_EXPECT)" in \
		target_pass|target_semantic_test_failure) ;; \
		*) echo "MUTATION_CONTROL_EXPECT is invalid" >&2; exit 1 ;; \
	esac
	@cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test "$(MUTATION_CONTROL_TEST)" \
		-expect "$(MUTATION_CONTROL_EXPECT)" \
		-- go test -json ./blackbox/integration -count=1 -run "^$(MUTATION_CONTROL_TEST)$$" -args --provision --install

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
		$(MAKE) --no-print-directory conformance-pg18-extension-artifact CONFORMANCE_EXTENSION_ARTIFACT="$$extension_bundle" PGRX_TARGET_DIR="$$artifact_root/cargo-target"; \
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

conformance-pg18-extension-artifact:
	@test -n "$(PGRX_PG_CONFIG)" || (echo "PGRX_PG_CONFIG is required" >&2; exit 1)
	@test "$$($(PGRX_PG_CONFIG) --version)" = "PostgreSQL 18.3" || (echo "PGRX_PG_CONFIG must select PostgreSQL 18.3" >&2; exit 1)
	@set -eu; \
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
		sql="$$out$$sharedir/extension/synchro_pg--0.3.0.sql"; \
		test -f "$$library" && test -f "$$control" && test -f "$$sql"; \
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
			'  "postgresql_version": "18.3",' \
			'  "files": [' \
			"    {\"path\": \"$$library_path\", \"destination\": \"pkglibdir/synchro_pg.$$suffix\", \"sha256\": \"$$library_hash\"}," \
			"    {\"path\": \"$$control_path\", \"destination\": \"sharedir/extension/synchro_pg.control\", \"sha256\": \"$$control_hash\"}," \
			"    {\"path\": \"$$sql_path\", \"destination\": \"sharedir/extension/synchro_pg--0.3.0.sql\", \"sha256\": \"$$sql_hash\"}" \
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

test-evidence:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./evidence ./cmd/synchro-evidence -count=1

test-inventory:
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./inventory -count=1

test-blackbox: conformance-mod-download test-blackbox-harness test-blackbox-components
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult suite -- go test -json ./blackbox/integration -count=$(BLACKBOX_TEST_COUNT) -args --provision --install

test-conformance: conformance-mod-download test-conformance-testresult test-conformance-module test-conformance-imports test-conformance-contract test-conformance-drivers test-conformance-scenarios check-conformance-catalog test-vectors test-reference test-conformance-faults test-blackbox-harness test-evidence test-inventory

rc-check-pg18:
	@echo "$@ is unavailable until its required verification phase is implemented; release promotion is blocked." >&2
	@exit 1

evidence:
	@test -f "$(PHASE_5_INPUT)" || (echo "PHASE_5_INPUT is required: $(PHASE_5_INPUT)" >&2; exit 1)
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/synchro-evidence generate --repo-root .. --input "$(PHASE_5_INPUT)" --output "$(PHASE_5_EVIDENCE)"
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/synchro-evidence validate --repo-root .. --summary "$(PHASE_5_EVIDENCE)"

lint-rn:
	cd clients/react-native && yarn typecheck
	cd clients/react-native && yarn lint

test: test-rust-core test-adapter test-swift-unit test-kotlin-unit test-rn-unit verify-contract docs-build

build-swift-native-runner:
	cd clients/swift && swift build --product synchro-native-runner

build-kotlin-conformance-app:
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew $(GRADLE_TEST_ARGS) :conformance-app:assembleDebug :conformance-app:assembleDebugAndroidTest

test-swift-unit:
	rm -rf clients/swift/.build/test-results/unit.xcresult
	mkdir -p clients/swift/.build/test-results
	cd clients/swift && xcodebuild test -quiet -scheme Synchro-Package -destination 'platform=macOS' -skip-testing:SynchroTests/IntegrationTests -skip-testing:SynchroTests/SchemaIntegrationTests -skip-testing:SynchroTests/ClientSchemaIdentityTests -resultBundlePath .build/test-results/unit.xcresult
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult xcresult -path ../clients/swift/.build/test-results/unit.xcresult

test-client-schema-identity: conformance-mod-download test-adapter-setup
	@set -e; \
		status=0; \
		if $(MAKE) --no-print-directory _test-client-schema-identity; then status=0; else status=$$?; fi; \
		$(MAKE) --no-print-directory test-adapter-teardown; \
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
	cd clients/swift && xcodebuild test -quiet -scheme Synchro-Package -destination 'platform=macOS' \
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
		runner_dir="$$(cd clients/swift && swift build --show-bin-path)"; \
		test -x "$$runner_dir/synchro-native-runner"; \
		cd conformance; \
		SYNCHRO_SWIFT_NATIVE_RUNNER="$$runner_dir/synchro-native-runner" \
			GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
			-test TestRealSwiftWarmConnect \
			-expect target_pass \
			-- go test -tags swiftintegration -json ./swift -count=1 -timeout=10m \
			-run '^TestRealSwiftWarmConnect$$' -args --provision --install

test-swift: synchrod-pg-test-restart test-swift-warm-connect
	rm -rf clients/swift/.build/integration-derived-data clients/swift/.build/test-results/integration.xcresult
	mkdir -p clients/swift/.build/test-results
	cd clients/swift && xcodebuild build-for-testing -quiet -scheme Synchro-Package -destination 'platform=macOS' -derivedDataPath .build/integration-derived-data
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
		plutil -insert "$$environment_path.SYNCHRO_TEST_SEED_PATH" -string "$(CURDIR)/clients/react-native/example/seed.db" "$$xctestrun"; \
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

test-kotlin-instrumentation: build-kotlin-conformance-app
	@test -x "$(ANDROID_HOME)/platform-tools/adb" || (echo "adb not found at $(ANDROID_HOME)/platform-tools/adb"; exit 1)
	@test -n "$(KOTLIN_ANDROID_SERIAL)" || (echo "Set KOTLIN_ANDROID_SERIAL to one booted Android device."; exit 1)
	rm -rf clients/kotlin/conformance-app/build/outputs/androidTest-results/connected
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" ./gradlew $(GRADLE_TEST_ARGS) -Pandroid.injected.device.serial="$(KOTLIN_ANDROID_SERIAL)" :conformance-app:connectedDebugAndroidTest
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult junit -path ../clients/kotlin/conformance-app/build/outputs/androidTest-results/connected

test-kotlin: synchrod-pg-test-restart test-kotlin-warm-connect
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	rm -rf clients/kotlin/synchro/build/test-results
	cd clients/kotlin && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" $(TEST_ENV) ./gradlew $(GRADLE_TEST_ARGS) -PsynchroTestSuite=integration :synchro:test
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
	cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION=ios.sim.debug GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeWarmConnectIOS \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=15m \
		-run '^TestRealReactNativeWarmConnectIOS$$' -args --provision --install

test-rn-warm-connect-android: conformance-mod-download test-blackbox-harness test-rn-warm-connect-control test-rn-android-parity rn-watchman-reset rn-android-emulator-reset
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	cd clients/react-native/example && ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" npx detox build --configuration $(RN_ANDROID_DETOX_CONFIG)
	cd conformance && SYNCHRO_RN_DETOX_CONFIGURATION="$(RN_ANDROID_DETOX_CONFIG)" GOFLAGS= GOWORK=off go run ./cmd/testresult exact \
		-test TestRealReactNativeWarmConnectAndroid \
		-expect target_pass \
		-- go test -tags reactnativeintegration -json ./reactnative -count=1 -timeout=15m \
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
	cd clients/react-native/example/ios && \
		STAMP=.synchro-pods.stamp; \
		SOURCE_DIGEST="$$( ( \
			shasum -a 256 ../../package.json; \
			find ../../src -type f \( -name '*.ts' -o -name '*.tsx' \) -exec shasum -a 256 {} +; \
			find ../../../../clients/swift/Sources/Synchro -type f -name '*.swift' -exec shasum -a 256 {} +; \
			find ../../ios -type f \( -name '*.swift' -o -name '*.m' -o -name '*.mm' -o -name '*.h' -o -name '*.cpp' \) -exec shasum -a 256 {} + \
		) | LC_ALL=C sort | shasum -a 256 | cut -d ' ' -f 1)"; \
		if [ ! -f "$$STAMP" ] || [ ! -f Pods/Manifest.lock ] || [ ! -f SynchroReactNativeExample.xcworkspace/contents.xcworkspacedata ] || ! cmp -s Podfile.lock Pods/Manifest.lock || [ Podfile -nt "$$STAMP" ] || [ Podfile.lock -nt "$$STAMP" ] || [ ../../SynchroReactNative.podspec -nt "$$STAMP" ] || [ ../../../../Synchro.podspec -nt "$$STAMP" ] || ! grep -qx "$$SOURCE_DIGEST" "$$STAMP"; then \
			pod install && printf '%s\n' "$$SOURCE_DIGEST" > "$$STAMP"; \
		else \
			echo "React Native iOS pods already match Podfile.lock"; \
		fi

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

test-rn-e2e-android-run:
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android Detox requires JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	rm -f clients/react-native/example/artifacts/android-test-results.json
	mkdir -p clients/react-native/example/artifacts
	cd clients/react-native/example && \
		ANDROID_HOME="$(ANDROID_HOME)" ANDROID_SDK_ROOT="$(ANDROID_HOME)" \
		JAVA_HOME="$(ANDROID_JAVA_HOME)" PATH="$(ANDROID_JAVA_HOME)/bin:$$PATH" \
		$(TEST_ENV) npx detox test --configuration $(RN_ANDROID_DETOX_CONFIG) $(DETOX_ARGS) --json --outputFile artifacts/android-test-results.json
	cd conformance && GOFLAGS= GOWORK=off go run ./cmd/testresult jest -path ../clients/react-native/example/artifacts/android-test-results.json

test-rn-e2e-android:
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-android-build
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-android-run

test-rn:
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-ios
	@$(MAKE) DETOX_ARGS="$(DETOX_ARGS)" test-rn-e2e-android

release-pods-check: version-check
	@command -v pod >/dev/null 2>&1 || (echo "CocoaPods CLI is required for release-pods-check."; exit 1)
	pod ipc spec Synchro.podspec >/dev/null
	swift package dump-package >/dev/null
	@echo "Apple package metadata validated."

# This is the combined gate for JavaScript and Go contract validation.
validation-check: override GO_TEST_ARGS := -v -count=1 -p 1
validation-check: override GO_TEST_PKGS := ./...
validation-check: override GRADLE_TEST_ARGS := --rerun-tasks
validation-check: override DETOX_ARGS :=
validation-check: build-conformance test-conformance test-blackbox version-check release-pods-check build build-seed build-check release-kotlin-local release-npm-dry-run lint-go lint-rust-core lint-rust-pg lint-rn test-rust-core test-rust-mutants test-integration-mutants test-rust-pg test-adapter test-client-schema-identity test-swift test-kotlin-unit test-kotlin-instrumentation test-kotlin test-rn-unit test-rn-native-parity test-rn test-packaged-consumers phase-5-check verify-contract check-pg-sql docs-build
	@echo "Validation suite passed."

release-check: override GO_TEST_ARGS := -v -count=1 -p 1
release-check: override GO_TEST_PKGS := ./...
release-check: override GRADLE_TEST_ARGS := --rerun-tasks
release-check: override DETOX_ARGS :=
# test-r1-benchmark is a separate required release gate. Its baseline is
# fingerprint-bound to the pinned benchmark host, so it runs there, not
# inside release-check. Release evidence requires both results.
release-check: validation-check evidence rc-check-pg18
	@echo "Release validation passed."
	@echo "Reminder: run make test-r1-benchmark on the pinned benchmark host. It is a separate required release gate."

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
		final="$(abspath $(CLIENT_ARTIFACT_DIR))/apple"; \
		stage="$$final.tmp.$$$$"; \
		cleanup() { rm -rf "$$stage"; }; \
		trap cleanup EXIT HUP INT TERM; \
		rm -rf "$$stage"; \
		mkdir -p "$$stage/Synchro/clients/swift"; \
		cp Package.swift Package.resolved Synchro.podspec LICENSE "$$stage/Synchro/"; \
		cp -R clients/swift/Sources "$$stage/Synchro/clients/swift/"; \
		COPYFILE_DISABLE=1 tar -czf "$$stage/synchro-spm-$(CURRENT_VERSION).tar.gz" -C "$$stage" Synchro; \
		mkdir -p "$$(dirname "$$final")"; \
		rm -rf "$$final"; \
		mv "$$stage" "$$final"; \
		trap - EXIT HUP INT TERM

client-consumer-kotlin-artifact: version-check
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	@set -eu; \
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
		tmp="$$(mktemp -d "$${TMPDIR:-/tmp}/synchro-swift-consumer.XXXXXX")"; \
		trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
		SYNCHRO_SWIFT_PACKAGE_PATH="$$artifact" swift package \
			--package-path verification/consumers/swift \
			--scratch-path "$$tmp/build" \
			show-dependencies --format json > "$$tmp/dependencies.json"; \
		grep -F "$$artifact" "$$tmp/dependencies.json" >/dev/null; \
		if grep -F "$(CURDIR)/clients/swift" "$$tmp/dependencies.json" >/dev/null; then \
			echo "Swift consumer resolved workspace client sources" >&2; \
			exit 1; \
		fi; \
		SYNCHRO_SWIFT_PACKAGE_PATH="$$artifact" swift run \
			--package-path verification/consumers/swift \
			--scratch-path "$$tmp/build" \
			SynchroConsumer

test-consumer-swift-ios: client-consumer-apple-artifact
	SUPPORT_PLATFORM_VERSION="$(SUPPORT_PLATFORM_VERSION)" \
		sh verification/consumers/swift-ios/test-consumer.sh "$(abspath $(CLIENT_ARTIFACT_DIR))"

test-consumer-kotlin: client-consumer-kotlin-artifact
	@test -n "$(ANDROID_JAVA_HOME)" || (echo "Android builds require JDK 17. Set ANDROID_JAVA_HOME to a JDK 17 install."; exit 1)
	@test -d "$(ANDROID_HOME)" || (echo "Android SDK not found at $(ANDROID_HOME). Set ANDROID_HOME to a valid SDK install."; exit 1)
	@set -eu; \
		tmp="$$(mktemp -d "$${TMPDIR:-/tmp}/synchro-kotlin-consumer.XXXXXX")"; \
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

test-consumer-rn-ios: client-consumer-apple-artifact client-consumer-rn-artifact
	SUPPORT_PLATFORM_VERSION="$(SUPPORT_PLATFORM_VERSION)" \
		sh verification/consumers/react-native/test-consumer.sh ios "$(abspath $(CLIENT_ARTIFACT_DIR))" "$(CURRENT_VERSION)"

test-consumer-rn-android: client-consumer-kotlin-artifact client-consumer-rn-artifact
	ANDROID_HOME="$(ANDROID_HOME)" ANDROID_JAVA_HOME="$(ANDROID_JAVA_HOME)" \
		sh verification/consumers/react-native/test-consumer.sh android "$(abspath $(CLIENT_ARTIFACT_DIR))" "$(CURRENT_VERSION)"

test-client-platforms:
	@test -n "$(SUPPORT_CELL_ID)" || (echo "SUPPORT_CELL_ID is required" >&2; exit 1)
	@case "$(SUPPORT_CELL_ID)" in \
		SUP-IOS-MIN-001) \
			test "$(SUPPORT_PLATFORM_VERSION)" = "16" || { echo "SUPPORT_PLATFORM_VERSION must be 16" >&2; exit 1; }; \
			$(MAKE) test-consumer-swift-ios ;; \
		SUP-IOS-CURRENT-001) \
			test -n "$(SUPPORT_PLATFORM_VERSION)" || { echo "SUPPORT_PLATFORM_VERSION is required" >&2; exit 1; }; \
			$(MAKE) test-consumer-swift-ios ;; \
		SUP-ANDROID-MIN-001) \
			test "$(SUPPORT_PLATFORM_VERSION)" = "24" || { echo "SUPPORT_PLATFORM_VERSION must be 24" >&2; exit 1; }; \
			test "$$($(ANDROID_HOME)/platform-tools/adb shell getprop ro.build.version.sdk | tr -d '\r')" = "24" || { echo "Android API 24 is required" >&2; exit 1; }; \
			$(MAKE) test-consumer-kotlin-device ;; \
		SUP-ANDROID-CURRENT-001|SUP-RN-ANDROID-CURRENT-001) \
			test -n "$(SUPPORT_PLATFORM_VERSION)" || { echo "SUPPORT_PLATFORM_VERSION is required" >&2; exit 1; }; \
			test "$$($(ANDROID_HOME)/platform-tools/adb shell getprop ro.build.version.sdk | tr -d '\r')" = "$(SUPPORT_PLATFORM_VERSION)" || { echo "Android runtime does not match SUPPORT_PLATFORM_VERSION" >&2; exit 1; }; \
			if [ "$(SUPPORT_CELL_ID)" = "SUP-ANDROID-CURRENT-001" ]; then $(MAKE) test-consumer-kotlin-device; else $(MAKE) test-consumer-rn-android; fi ;; \
		SUP-RN-IOS-CURRENT-001) \
			test -n "$(SUPPORT_PLATFORM_VERSION)" || { echo "SUPPORT_PLATFORM_VERSION is required" >&2; exit 1; }; \
			$(MAKE) test-consumer-rn-ios ;; \
		*) echo "unknown client support cell: $(SUPPORT_CELL_ID)" >&2; exit 1 ;; \
	esac

test-packaged-smoke:
	@python3 scripts/release-support-check.py --repo-root "$(CURDIR)" --evidence "$(PACKAGED_SMOKE_EVIDENCE)" --kind smoke

test-packaged-consumers: test-consumer-swift test-consumer-swift-ios test-consumer-kotlin test-consumer-kotlin-device test-consumer-rn-ios test-consumer-rn-android test-packaged-smoke

phase-5-check: test-conformance test-blackbox test-adapter test-rust-core test-rust-pg test-swift-unit test-kotlin-unit test-kotlin-instrumentation test-rn-unit test-swift test-kotlin test-rn-e2e-ios test-rn-e2e-android test-rn-warm-connect-ios test-rn-warm-connect-android test-packaged-consumers
	@python3 scripts/release-support-check.py --repo-root "$(CURDIR)" --evidence "$(PHASE_5_EVIDENCE)"

ext-build:
	cd extensions/synchro-pg && cargo build

generate-pg-sql:
	cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx schema pg18 --pg-config "$(PGRX_PG_CONFIG)" --out sql/synchro_pg--0.3.0.sql
	perl -pi -e 's/[ \t]+$$//' extensions/synchro-pg/sql/synchro_pg--0.3.0.sql
	perl -0pi -e 's/\n+\z/\n/' extensions/synchro-pg/sql/synchro_pg--0.3.0.sql

check-pg-sql:
	@set -eu; \
		tmp="$$(mktemp -d "$${TMPDIR:-/tmp}/synchro-pg-sql.XXXXXX")"; \
		trap 'rm -rf "$$tmp"' EXIT HUP INT TERM; \
		generated="$$tmp/synchro_pg--0.3.0.sql"; \
		cd extensions/synchro-pg; \
		CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx schema pg18 --pg-config "$(PGRX_PG_CONFIG)" --out "$$generated"; \
		perl -pi -e 's/[ \t]+$$//' "$$generated"; \
		perl -0pi -e 's/\n+\z/\n/' "$$generated"; \
		if ! cmp -s sql/synchro_pg--0.3.0.sql "$$generated"; then \
			diff -u sql/synchro_pg--0.3.0.sql "$$generated" || true; \
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

test-integration-mutants: test-conformance-testresult
	sh conformance/mutants/integration_gate.sh "$(CURDIR)"

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

lint-rust: lint-rust-core lint-rust-pg

test-adapter-setup: $(ADAPTER_DB_SETUP)

adapter-db-external-probe:
	@set -eu; \
		command -v psql >/dev/null 2>&1 || { echo "psql is required for an external ADAPTER_TEST_URL" >&2; exit 1; }; \
		echo "Probing external adapter test database..."; \
		psql "$(ADAPTER_TEST_URL)" -v ON_ERROR_STOP=1 -Atqc "SELECT 1" >/dev/null || { echo "external ADAPTER_TEST_URL is unreachable" >&2; exit 1; }; \
		available="$$(psql "$(ADAPTER_TEST_URL)" -v ON_ERROR_STOP=1 -Atqc "SELECT default_version FROM pg_available_extensions WHERE name = 'synchro_pg'")"; \
		test "$$available" = "$(CURRENT_VERSION)" || { echo "external database offers synchro_pg '$$available', expected '$(CURRENT_VERSION)'" >&2; exit 1; }; \
		echo "External adapter test database is reachable with synchro_pg $(CURRENT_VERSION)."

adapter-db-external-teardown:
	@echo "Leaving external adapter test database unchanged."

adapter-db-local-teardown:
	@echo "Tearing down adapter test database..."
	@$(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -c "DROP DATABASE IF EXISTS $(ADAPTER_TEST_DB)" 2>/dev/null || true
	@cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx stop $(PGRX_PG) 2>/dev/null || true
	@echo "Done."

adapter-db-local-setup: ext-install
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
	@cd extensions/synchro-pg && CARGO_TARGET_DIR="$(PGRX_TARGET_DIR)" cargo pgrx stop $(PGRX_PG) 2>/dev/null || true
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
	@$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -c "DROP ROLE IF EXISTS $(PGRX_WORKER_LOGIN)"
	@$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -c "CREATE ROLE $(PGRX_WORKER_LOGIN) LOGIN REPLICATION NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS"
	@$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d postgres -c "GRANT synchro_worker TO $(PGRX_WORKER_LOGIN)"
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
	@if grep -q "^synchro.worker_login" "$(PGRX_DATA_DIR)/postgresql.conf"; then \
		perl -0pi -e "s/^synchro\.worker_login\s*=.*$$/synchro.worker_login = '$(PGRX_WORKER_LOGIN)'/m" "$(PGRX_DATA_DIR)/postgresql.conf"; \
	else \
		printf "\nsynchro.worker_login = '$(PGRX_WORKER_LOGIN)'\n" >> "$(PGRX_DATA_DIR)/postgresql.conf"; \
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

test-adapter-teardown: $(ADAPTER_DB_TEARDOWN)

test-adapter: test-adapter-setup
	@echo "Running adapter integration tests..."
	@set -e; \
	status=0; \
	if (cd conformance && GOFLAGS= GOWORK=off TEST_DATABASE_URL="$(ADAPTER_TEST_URL)" go run ./cmd/testresult suite -dir ../api/go -- go test -json $(GO_TEST_ARGS) $(GO_TEST_PKGS)); then \
		status=0; \
	else \
		status=$$?; \
	fi; \
	$(MAKE) test-adapter-teardown; \
	exit $$status

synchrod-pg-test-start: build-seed verify-rn-seed
	@set -e; \
	for PID in $$(lsof -tiTCP:$(SYNCHROD_PG_PORT) -sTCP:LISTEN 2>/dev/null); do \
		kill "$$PID" 2>/dev/null || true; \
		sleep 1; \
		if kill -0 "$$PID" 2>/dev/null; then \
			kill -9 "$$PID" 2>/dev/null || true; \
		fi; \
	done; \
	if [ -f "$(SYNCHROD_PG_PID_FILE)" ] && kill -0 "$$(cat "$(SYNCHROD_PG_PID_FILE)")" 2>/dev/null; then \
		echo "synchrod-pg already running"; \
		exit 0; \
	fi; \
	$(MAKE) test-adapter-setup; \
	echo "Loading schema and registering tables..."; \
	$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -f extensions/testdata/schema.sql || { if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; exit 1; }; \
	$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -f extensions/testdata/register.sql || { if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; exit 1; }; \
	REGISTRY_READY=0; \
	for attempt in $$(seq 1 $(PGRX_READY_TIMEOUT)); do \
		REGISTRY_OUTPUT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -Atqc "SELECT CASE WHEN EXISTS (SELECT 1 FROM synchro.sync_registry_generations generation WHERE generation.state = 'active' AND (SELECT count(*) FROM synchro.sync_registry registry WHERE registry.registry_generation = generation.generation) = 13 AND EXISTS (SELECT 1 FROM synchro.sync_registry registry WHERE registry.registry_generation = generation.generation AND registry.table_name = 'line_items' AND registry.membership_function_name = 'test_line_items_membership') AND EXISTS (SELECT 1 FROM synchro.sync_registry registry WHERE registry.registry_generation = generation.generation AND registry.table_name = 'document_comments' AND registry.membership_function_name = 'test_document_comments_membership')) AND NOT EXISTS (SELECT 1 FROM synchro.sync_registry_generations generation WHERE generation.state = 'pending' AND generation.validated) THEN '1' ELSE '0' END" 2>&1 || true); \
		if [ "$$REGISTRY_OUTPUT" = "1" ]; then REGISTRY_READY=1; break; fi; \
		sleep 1; \
	done; \
	if [ "$$REGISTRY_READY" -ne 1 ]; then echo "synchro registry did not activate"; exit 1; fi; \
	echo "Loading canonical seed data..."; \
	$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -f extensions/testdata/canonical-seed.sql || { if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; exit 1; }; \
	echo "Waiting for bgworker to observe seeded rows..."; \
	for attempt in $$(seq 1 60); do \
		EDGE_COUNT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -Atqc "SELECT count(*) FROM synchro.sync_bucket_edges" 2>/dev/null || echo 0); \
		if [ "$$EDGE_COUNT" -ge 6 ] 2>/dev/null; then \
			break; \
		fi; \
		sleep 1; \
	done; \
	if [ "$$EDGE_COUNT" -lt 6 ] 2>/dev/null; then \
		echo "synchro worker did not materialize all canonical seed rows"; \
		exit 1; \
	fi; \
	JSON_OUTPUT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -Atqc "SELECT CASE WHEN count(*) = 5 THEN '1' ELSE '0' END FROM (VALUES ('nations', 'metadata', '{\"source\":\"seed\"}'), ('suppliers', 'tags', '[\"seed\"]'), ('parts', 'specifications', '{\"color\":\"blue\"}'), ('parts', 'tags', '[\"seed\"]'), ('categories', 'metadata', '{\"source\":\"seed\"}')) expected(table_name, column_name, wire_value) JOIN synchro.sync_registry_generations generation ON generation.state = 'active' JOIN synchro.sync_registry registry ON registry.registry_generation = generation.generation AND registry.table_name = expected.table_name JOIN synchro.sync_registry_fields field ON field.registry_generation = registry.registry_generation AND field.relation_id = registry.relation_id AND field.physical_column = expected.column_name JOIN synchro.sync_captured_rows captured ON captured.relation_id = registry.relation_id WHERE captured.row_data -> field.field_id::text = to_jsonb(expected.wire_value)" 2>&1 || true); \
	if [ "$$JSON_OUTPUT" != "1" ]; then \
		echo "synchro worker did not preserve canonical seed JSON values"; \
		exit 1; \
	fi; \
	echo "Backfilling scope edges..."; \
	$(PGRX_PSQL) -v ON_ERROR_STOP=1 -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -c "SELECT synchro.synchro_backfill_bucket_edges()" || { if [ -f "$(PGRX_LOG_FILE)" ]; then tail -n 200 "$(PGRX_LOG_FILE)"; fi; exit 1; }; \
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
		MANIFEST_READY=0; \
		for attempt in $$(seq 1 $(PGRX_READY_TIMEOUT)); do \
			MANIFEST_OUTPUT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -Atqc "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_catalog.jsonb_array_elements((synchro.synchro_schema_manifest()->'manifest'->'tables')) table_def WHERE table_def->>'name' = 'orders') THEN '1' ELSE '0' END" 2>&1 || true); \
			CONNECT_OUTPUT=$$($(PGRX_PSQL) -h "$(PGRX_ADMIN_HOST)" -p $(PGRX_PORT) -U "$(PGRX_ADMIN_USER)" -d $(ADAPTER_TEST_DB) -Atqc "SELECT CASE WHEN synchro.synchro_connect('readiness-user', '{\"client_id\":\"readiness-client\",\"platform\":\"android\",\"app_version\":\"1.0.0\",\"protocol_version\":3,\"schema\":{\"version\":0,\"hash\":\"\"},\"scope_set_version\":0,\"known_scopes\":{}}'::pg_catalog.jsonb)->'schema'->>'action' = 'replace' THEN '1' ELSE '0' END" 2>&1 || true); \
			if [ "$$MANIFEST_OUTPUT" = "1" ] && [ "$$CONNECT_OUTPUT" = "1" ]; then \
				MANIFEST_READY=1; \
				break; \
			fi; \
			sleep 1; \
		done; \
		if [ "$$MANIFEST_READY" -ne 1 ]; then \
			echo "synchro schema/connect readiness did not converge in $(PGRX_READY_TIMEOUT)s"; \
			echo "manifest readiness: $$MANIFEST_OUTPUT"; \
			echo "connect readiness: $$CONNECT_OUTPUT"; \
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
		HTTP_READY=0; \
		for attempt in $$(seq 1 30); do \
			if curl -fsS -o /dev/null "http://localhost:$(SYNCHROD_PG_PORT)/sync/schema" 2>/dev/null; then \
				HTTP_READY=1; \
				break; \
			fi; \
			sleep 1; \
		done; \
		if [ "$$HTTP_READY" -ne 1 ]; then \
			echo "synchrod-pg HTTP schema endpoint did not become ready"; \
			if [ -f "$(SYNCHROD_PG_LOG_FILE)" ]; then cat "$(SYNCHROD_PG_LOG_FILE)"; fi; \
			rm -f "$(SYNCHROD_PG_PID_FILE)"; \
			exit 1; \
		fi; \
		if [ "$(REFRESH_RN_SEED)" = "1" ]; then \
			echo "Refreshing canonical seed asset..."; \
			if lsof "$(CURDIR)/clients/react-native/example/seed.db" "$(CURDIR)/clients/react-native/example/seed.db-wal" "$(CURDIR)/clients/react-native/example/seed.db-shm" >/dev/null 2>&1; then \
				echo "canonical seed asset is in use"; \
				exit 1; \
			fi; \
			rm -f "$(CURDIR)/clients/react-native/example/seed.db-wal" "$(CURDIR)/clients/react-native/example/seed.db-shm"; \
			DATABASE_URL="$(ADAPTER_TEST_URL)" "$(CURDIR)/$(SEED_BINARY)" --output "$(CURDIR)/clients/react-native/example/seed.db" --overwrite || { \
				if [ -f "$(SYNCHROD_PG_LOG_FILE)" ]; then cat "$(SYNCHROD_PG_LOG_FILE)"; fi; \
				rm -f "$(SYNCHROD_PG_PID_FILE)"; \
				exit 1; \
			}; \
			cd "$(CURDIR)/clients/react-native/example"; \
			shasum -a 256 seed.db > seed.db.sha256; \
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
	@$(MAKE) test-adapter-teardown

synchrod-pg-test-restart: synchrod-pg-test-stop
	@$(MAKE) synchrod-pg-test-start

clean:
	rm -rf bin/ "$(SYNCHROD_PG_PID_FILE)" "$(SYNCHROD_PG_LOG_FILE)"

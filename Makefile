.PHONY: \
	help \
	build \
	run \
	db-init \
	db-prepare-server \
	db-reset \
	db-clean-slots \
	test \
	test-go-unit \
	test-go-integration \
	test-go-wal \
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
	clean

# Local infrastructure defaults.
DB_HOST ?= 10.0.0.86
DB_PORT ?= 5432
DB_NAME ?= synchro_test
DB_USER ?= postgres
DB_PASSWORD ?= postgres

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
	@echo "  run                   - Run synchrod locally with current env"
	@echo "  db-init               - Create the test database if missing"
	@echo "  db-prepare-server     - Reset the shared synchrod test DB and install test schema"
	@echo "  db-reset              - Drop and recreate the test database"
	@echo "  db-clean-slots        - Drop inactive replication slots in the test DB"
	@echo "  test                  - Run local default validation (Go unit + Swift + Kotlin unit tests)"
	@echo "  test-go-unit          - Run Go tests without integration env"
	@echo "  test-go-integration   - Run Go integration tests against the configured test DB"
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
	@echo "  clean                 - Remove local build/test artifacts"

build:
	go build -o bin/synchrod ./cmd/synchrod

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

test: test-go-unit test-swift-unit test-kotlin-unit test-rn-unit

test-go-unit:
	go test ./...

test-go-integration:
	$(TEST_ENV) go test -count=1 -p 1 ./...

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
	cd clients/kotlin && ./gradlew :synchro:test

test-kotlin: synchrod-test-restart
	cd clients/kotlin && $(TEST_ENV) ./gradlew :synchro:test

test-kotlin-integration: test-kotlin

test-rn-unit:
	cd clients/react-native && npm test -- --testPathIgnorePatterns=e2e

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

clean:
	rm -rf bin/ "$(SYNCHROD_PID_FILE)" "$(SYNCHROD_LOG_FILE)"

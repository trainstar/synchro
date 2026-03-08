.PHONY: help build run db-init db-reset db-clean-slots test test-unit test-integration test-wal synchrod-test-start synchrod-test-stop test-swift test-swift-unit clean

# Database host (dev-infra on desktop)
DB_HOST ?= 10.0.0.86

# Default target
help:
	@echo "Available targets:"
	@echo "  build             - Build the synchrod binary"
	@echo "  run               - Run synchrod locally (requires env vars)"
	@echo "  db-init           - Create synchro_test database if missing"
	@echo "  db-reset          - Drop and recreate synchro_test database"
	@echo "  db-clean-slots    - Drop all inactive replication slots (cleanup)"
	@echo "  test              - Run all tests (unit + integration)"
	@echo "  test-unit         - Run unit tests only (no DB required)"
	@echo "  test-integration  - Run integration tests (requires DB on DB_HOST)"
	@echo "  test-wal          - Run WAL integration tests (requires DB + replication)"
	@echo "  synchrod-test-start - Start synchrod test server (background, with JWT)"
	@echo "  synchrod-test-stop  - Stop synchrod test server"
	@echo "  test-swift        - Run Swift integration tests (requires synchrod running)"
	@echo "  test-swift-unit   - Run Swift unit tests only (no server required)"
	@echo "  clean             - Remove build artifacts"

# Build
build:
	go build -o bin/synchrod ./cmd/synchrod

# Run
run:
	go run ./cmd/synchrod

# Database initialization
db-init:
	@echo "Checking/creating synchro_test on $(DB_HOST)..."
	@psql "postgres://postgres:postgres@$(DB_HOST):5432/postgres" -tc \
		"SELECT 1 FROM pg_database WHERE datname = 'synchro_test'" | grep -q 1 || \
		psql "postgres://postgres:postgres@$(DB_HOST):5432/postgres" -c \
		"CREATE DATABASE synchro_test;"
	@echo "Database ready: postgres://postgres:postgres@$(DB_HOST):5432/synchro_test"

db-reset:
	@echo "Resetting synchro_test on $(DB_HOST)..."
	@psql "postgres://postgres:postgres@$(DB_HOST):5432/postgres" -c \
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'synchro_test' AND pid <> pg_backend_pid();" >/dev/null 2>&1 || true
	@psql "postgres://postgres:postgres@$(DB_HOST):5432/postgres" -c "DROP DATABASE IF EXISTS synchro_test;"
	@psql "postgres://postgres:postgres@$(DB_HOST):5432/postgres" -c "CREATE DATABASE synchro_test;"
	@echo "Database reset: postgres://postgres:postgres@$(DB_HOST):5432/synchro_test"

db-clean-slots:
	@echo "Dropping inactive replication slots on $(DB_HOST)..."
	@psql "postgres://postgres:postgres@$(DB_HOST):5432/synchro_test" -c \
		"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE active = false;" 2>/dev/null || true
	@echo "Done."

# Tests
test: test-unit test-integration

test-unit:
	go test ./...

test-integration:
	TEST_DATABASE_URL="postgres://postgres:postgres@$(DB_HOST):5432/synchro_test?sslmode=disable" \
		go test -count=1 ./...

test-wal:
	TEST_DATABASE_URL="postgres://postgres:postgres@$(DB_HOST):5432/synchro_test?sslmode=disable" \
	TEST_REPLICATION_URL="postgres://postgres:postgres@$(DB_HOST):5432/synchro_test?replication=database&sslmode=disable" \
		go test -count=1 -v ./...

# Synchrod test server (for Swift integration tests)
synchrod-test-start:
	JWT_SECRET=test-secret-for-integration-tests \
	MIN_CLIENT_VERSION=1.0.0 \
	DATABASE_URL="postgres://postgres:postgres@$(DB_HOST):5432/synchro_test?sslmode=disable" \
	REPLICATION_URL="postgres://postgres:postgres@$(DB_HOST):5432/synchro_test?replication=database&sslmode=disable" \
		go run ./cmd/synchrod &
	@sleep 2
	@echo "synchrod test server running on :8080"

synchrod-test-stop:
	@pkill -f "cmd/synchrod" || true
	@echo "synchrod stopped"

# Swift tests
test-swift:
	cd clients/swift && \
	SYNCHRO_TEST_URL=http://localhost:8080 \
	SYNCHRO_TEST_JWT_SECRET=test-secret-for-integration-tests \
	TEST_DATABASE_URL="postgres://postgres:postgres@$(DB_HOST):5432/synchro_test?sslmode=disable" \
		swift test

test-swift-unit:
	cd clients/swift && \
		swift test --filter "^(?!.*Integration).*$$"

# Clean
clean:
	rm -rf bin/

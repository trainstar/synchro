# React Native Client SDK — Verification Notes

## Current State

- New Architecture only
- iOS bridge uses the Codegen-generated ObjC++ wrapper plus Swift implementation
- JS package builds and publishes as an npm package

## Verified

| Step | Status | Notes |
|------|--------|-------|
| TypeScript typecheck | PASS | `npm run typecheck` |
| Package build | PASS | `npm run prepare` |
| Tarball dry-run | PASS | `npm pack --dry-run` |
| Unit tests | PASS | Jest suite |
| iOS E2E | PASS | Detox against live `synchrod` |
| Android build | PASS | `npm run build:android` with JDK 17 + Android SDK |

## Outstanding

| Area | Status | Notes |
|------|--------|-------|
| Android E2E verification | IN PROGRESS | Emulator + API 34 image provisioning in progress in the audit environment |

## Guidance

- Do not use this file as the source of truth for package docs.
- Public package docs live in `README.md`.
- Shipability requires both iOS and Android verification, not just iOS green runs.

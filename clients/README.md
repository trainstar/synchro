# Synchro Client SDKs

Mobile and cross-platform SDKs for the Synchro sync library. Each SDK wraps a platform-native SQLite library and adds sync orchestration via HTTP calls to the Synchro server.

## SDKs

| SDK | Platform | Wraps | Status |
|-----|----------|-------|--------|
| **Swift** | iOS / macOS | GRDB | In Progress |
| **Kotlin** | Android | Room | Planned |
| **React Native** | iOS + Android | Swift + Kotlin | Planned |

## Architecture

All SDKs implement the same interface contract. See [ARCHITECTURE.md](./ARCHITECTURE.md) for the shared design.

### Layer Model

- **Layer 1 (Native)**: Swift wraps GRDB, Kotlin wraps Room. SQLite triggers on synced tables auto-track all writes (CDC). HTTP sync orchestration talks to the Synchro server.
- **Layer 2 (React Native)**: Native module wraps both Layer 1 SDKs. SQL strings down, JSON rows up, events via emitter.

### Change Detection

Client-side CDC mirrors the server's WAL approach using SQLite triggers:

- Triggers are auto-created by the SDK when synced tables are built from server schema
- A `sync_lock` flag prevents tracking during pull application
- Pending queue deduplicates via `ON CONFLICT DO UPDATE`
- On push, current record data is hydrated from the local table
- `BEFORE DELETE` triggers convert hard deletes to soft deletes
- No special write API needed -- standard GRDB/Room API works

## Build

```bash
# Swift
cd swift && swift build && swift test

# Kotlin (planned)
cd kotlin && ./gradlew build

# React Native (planned)
cd react-native && npm install && npm test
```

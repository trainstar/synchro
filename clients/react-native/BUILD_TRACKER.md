# React Native Client SDK — Build & E2E Tracker

## Architecture
- **New Architecture only** (RCT_NEW_ARCH_ENABLED=1, bridgeless mode)
- **ObjC++ thin wrapper** (`SynchroModule.mm`) conforms to Codegen `NativeSynchroSpec`
- **Swift implementation** (`SynchroModule.swift` as `SynchroModuleImpl`) — all logic lives here
- **Event delegation**: Swift → ObjC++ via `SynchroEventEmitting` protocol → Codegen `emitOnXxx:` methods

## Issues Found & Fixed

### 1. StatementArguments optional not unwrapped
- **File**: `SynchroModule.swift` (transaction block, 3 occurrences)
- **Error**: `StatementArguments(...)` returns optional in GRDB 7.0
- **Fix**: Added `?? StatementArguments()` fallback
- **Status**: FIXED

### 2. Swift types not visible to ObjC++
- **Files**: `SynchroModule.swift`
- **Error**: `cannot find protocol declaration for 'SynchroEventEmitting'`, `unknown type name 'SynchroModuleImpl'`
- **Root cause**: Protocol and class lacked `public` visibility for cross-module access
- **Fix**: Added `@objc public protocol`, `public class`, `@objc public weak var`, `@objc public func` on all methods
- **Status**: FIXED

### 3. Missing `import React` in Swift
- **File**: `SynchroModule.swift`
- **Error**: `cannot find type 'RCTPromiseResolveBlock'` / `RCTPromiseRejectBlock` in scope
- **Fix**: Added `import React`
- **Status**: FIXED

### 4. Duplicate `@objc` attribute
- **File**: `SynchroModule.swift`
- **Error**: `duplicate attribute` on `@objc(SynchroModuleImpl) @objc public class`
- **Fix**: Removed second `@objc`, kept `@objc(SynchroModuleImpl)` for ObjC name mapping
- **Status**: FIXED

### 5. Debug console.log statements
- **Files**: `SynchroClient.ts`, `App.tsx`
- **Error**: Debug logging left from troubleshooting
- **Fix**: Removed all `console.log` debug statements
- **Status**: FIXED

### 6. Detox buttons not hittable (off-screen)
- **Files**: `sync.test.ts`, `App.tsx`
- **Error**: Buttons below the fold on iPhone SE cause "View is not hittable"
- **Fix**: Added `scrollToAndTap()` helper using `waitFor().toBeVisible().whileElement().scroll()`, added `testID="test-scroll"` to ScrollView
- **Status**: FIXED

### 7. iPhone 16 simulator not available
- **Error**: Build destination `iPhone 16` not found
- **Fix**: Using `iPhone SE (3rd generation)` (available simulator)
- **Status**: FIXED

## Build Status

| Step | Status | Notes |
|------|--------|-------|
| pod install (NEW_ARCH=1) | PASS | Codegen ran, 82 deps installed |
| xcodebuild | PASS | BUILD SUCCEEDED |
| Unit tests (Jest) | PENDING | 54 tests |
| E2E tests (Detox) | PENDING | 16 tests |

## E2E Test Matrix

| Test | Status | Notes |
|------|--------|-------|
| shows the test harness | PENDING | |
| initializes successfully | PENDING | |
| executes a query | PENDING | |
| executes a write | PENDING | |
| write transaction commit | PENDING | |
| write transaction rollback | PENDING | |
| read transaction | PENDING | |
| transaction timeout | PENDING | |
| transaction error recovery | PENDING | |
| starts sync | PENDING | |
| push/pull round trip | PENDING | |
| conflict resolution | PENDING | |
| multi-user isolation | PENDING | |
| shows sync status | PENDING | |
| stops sync | PENDING | |
| maps native errors | PENDING | |

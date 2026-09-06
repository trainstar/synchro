#!/usr/bin/env bash
# Run the React Native Android e2e gate inside the CI emulator session.
# The emulator runner executes each script line in its own shell, so the
# whole sequence lives here as one command.
set -euo pipefail

adb shell wm dismiss-keyguard || true
adb shell input keyevent 82 || true

if ! scripts/ci/capture-gate-result.py \
  --gate test-rn-e2e-android \
  --output dist/verification/gate-results/test-rn-e2e-android.json \
  -- make test-rn-e2e-android-run; then
  echo "=== device log after the failed bridge run ==="
  timeout 60 adb logcat -d AndroidRuntime:E ActivityManager:I ReactNativeJS:V "*:S" | tail -n 200 || true
  exit 1
fi

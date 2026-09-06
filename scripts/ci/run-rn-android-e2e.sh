#!/usr/bin/env bash
# Run the React Native Android e2e gate inside the CI emulator session.
# The emulator runner executes each script line in its own shell, so the
# whole sequence lives here as one command.
set -euo pipefail

adb shell wm dismiss-keyguard || true
adb shell input keyevent 82 || true
# A crash or ANR dialog on the slow software-rendered emulator takes window
# focus, and every later Espresso interaction then fails on focus.
adb shell settings put global hide_error_dialogs 1 || true
adb shell settings put global window_animation_scale 0.0 || true
adb shell settings put global transition_animation_scale 0.0 || true
adb shell settings put global animator_duration_scale 0.0 || true
adb shell input keyevent KEYCODE_HOME || true

if ! scripts/ci/capture-gate-result.py \
  --gate test-rn-e2e-android \
  --output dist/verification/gate-results/test-rn-e2e-android.json \
  -- make test-rn-e2e-android-run; then
  echo "=== window focus after the failed bridge run ==="
  timeout 30 adb shell dumpsys window windows | grep -E "mCurrentFocus|mFocusedApp|mObscuringWindow" || true
  echo "=== device log after the failed bridge run ==="
  timeout 60 adb logcat -d AndroidRuntime:E ActivityManager:I ReactNativeJS:V "*:S" | tail -n 200 || true
  exit 1
fi

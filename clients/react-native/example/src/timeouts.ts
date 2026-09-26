// The Detox driver and the harness app share this budget.
// A wait detects a hang. It is not a speed check. The slowest measured CI test
// other than the relaunch test took 48.6 s, so one wait has a wide margin.
export const WAIT_TIMEOUT_MS = 120000;

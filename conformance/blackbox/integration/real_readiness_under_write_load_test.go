package integration

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"testing"
	"time"
)

const (
	realReadinessLoadDuration       = 30 * time.Second
	realReadinessLoadCommitInterval = 125 * time.Millisecond
	realReadinessLoadSampleInterval = 200 * time.Millisecond
	// The load must average at least 6 commits each second. A short host stall can delay one second of writes.
	realReadinessLoadMinimumRate = 6
)

type realReadinessLimits struct {
	heartbeatSeconds float64
	walLagBytes      float64
	walLagSeconds    float64
}

type realReadinessSample struct {
	at                 time.Duration
	ready              bool
	unhealthy          []string
	heartbeatSeconds   float64
	heartbeatKnown     bool
	walLagBytes        float64
	walLagBytesKnown   bool
	walLagSeconds      float64
	walLagSecondsKnown bool
}

func (sample realReadinessSample) withinLimits(limits realReadinessLimits) bool {
	return sample.heartbeatKnown && sample.heartbeatSeconds >= 0 && sample.heartbeatSeconds <= limits.heartbeatSeconds &&
		sample.walLagBytesKnown && sample.walLagBytes >= 0 && sample.walLagBytes <= limits.walLagBytes &&
		sample.walLagSecondsKnown && sample.walLagSeconds >= 0 && sample.walLagSeconds <= limits.walLagSeconds
}

type realReadinessWriteLoad struct {
	commits                       []time.Duration
	err                           error
	maximumScheduledStartDelay    time.Duration
	totalSuccessfulExecDuration   time.Duration
	maximumSuccessfulExecDuration time.Duration
}

// TestRealReadinessStaysReadyUnderWriteLoad proves that readiness stays ready while a healthy worker captures writes.
// Each readiness sample during a steady write load is ready when its observations are within the configured limits.
func TestRealReadinessStaysReadyUnderWriteLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	harness, _ := provisionRealProofHarness(t, ctx)
	writer := openIssue49Admin(t, ctx, harness)
	admin := openIssue49Admin(t, ctx, harness)
	waitForIssue49CanonicalHealth(t, ctx, admin, true)
	limits := loadRealReadinessLimits(t, ctx, admin)

	writeContext, stopWrites := context.WithCancel(ctx)
	defer stopWrites()
	started := time.Now()
	written := make(chan realReadinessWriteLoad, 1)
	go func() {
		written <- runRealReadinessWriteLoad(writeContext, writer, started)
	}()
	sampleCount := int(realReadinessLoadDuration / realReadinessLoadSampleInterval)
	samples := make([]realReadinessSample, 0, sampleCount)
	for sample := 0; sample < sampleCount; sample++ {
		time.Sleep(time.Until(started.Add(time.Duration(sample) * realReadinessLoadSampleInterval)))
		samples = append(samples, loadRealReadinessSample(t, ctx, admin, time.Since(started)))
	}
	load := <-written

	if load.err != nil {
		t.Fatalf("readiness write load failed after %d commits: %v", len(load.commits), load.err)
	}
	if commits, want := realReadinessLoadCommitsInWindow(load.commits), realReadinessLoadMinimumRate*int(realReadinessLoadDuration/time.Second); commits < want {
		t.Fatalf("write load committed %d rows in %s, want at least %d: max_scheduled_start_delay=%s total_successful_exec_duration=%s max_successful_exec_duration=%s",
			commits, realReadinessLoadDuration, want, load.maximumScheduledStartDelay, load.totalSuccessfulExecDuration, load.maximumSuccessfulExecDuration)
	}
	if last := samples[len(samples)-1].at; last > load.commits[len(load.commits)-1] {
		t.Fatalf("readiness sample at %s started after the last write at %s", last, load.commits[len(load.commits)-1])
	}
	exceeded := 0
	var firstExceeded realReadinessSample
	var maximum realReadinessSample
	for _, sample := range samples {
		if !sample.withinLimits(limits) {
			if exceeded == 0 {
				firstExceeded = sample
			}
			exceeded++
		}
		maximum.heartbeatSeconds = max(maximum.heartbeatSeconds, sample.heartbeatSeconds)
		maximum.walLagBytes = max(maximum.walLagBytes, sample.walLagBytes)
		maximum.walLagSeconds = max(maximum.walLagSeconds, sample.walLagSeconds)
	}
	// A sample outside a limit is legitimately unready, so the load cannot prove readiness.
	if exceeded > 0 {
		t.Fatalf(
			"%d of %d readiness samples were outside a configured limit: first=%+v limits=%+v",
			exceeded, len(samples), firstExceeded, limits,
		)
	}
	// The mutation gate treats output of the parent test as a setup failure, so the summary is in the assertion.
	t.Run("assertion", func(t *testing.T) {
		t.Logf(
			"readiness write load commits=%d samples=%d max_heartbeat_age=%.3fs max_wal_lag_bytes=%.0f max_commit_lag=%.3fs max_scheduled_start_delay=%s total_successful_exec_duration=%s max_successful_exec_duration=%s",
			len(load.commits), len(samples), maximum.heartbeatSeconds, maximum.walLagBytes, maximum.walLagSeconds,
			load.maximumScheduledStartDelay, load.totalSuccessfulExecDuration, load.maximumSuccessfulExecDuration,
		)
		notReady := 0
		unhealthy := make(map[string]int)
		var firstNotReady realReadinessSample
		for _, sample := range samples {
			if sample.ready {
				continue
			}
			if notReady == 0 {
				firstNotReady = sample
			}
			notReady++
			for _, check := range sample.unhealthy {
				unhealthy[check]++
			}
		}
		if notReady > 0 {
			t.Fatalf(
				"readiness was false in %d of %d samples within the configured limits: checks=%v first=%+v",
				notReady, len(samples), unhealthy, firstNotReady,
			)
		}
	})
}

func loadRealReadinessLimits(t *testing.T, ctx context.Context, database *sql.DB) realReadinessLimits {
	t.Helper()
	var limits realReadinessLimits
	if err := database.QueryRowContext(ctx, `
		SELECT current_setting('synchro.max_worker_heartbeat_age_seconds')::double precision,
		       current_setting('synchro.max_wal_lag_bytes')::double precision,
		       current_setting('synchro.max_wal_lag_seconds')::double precision`).Scan(
		&limits.heartbeatSeconds,
		&limits.walLagBytes,
		&limits.walLagSeconds,
	); err != nil {
		t.Fatalf("load configured readiness limits: %v", err)
	}
	if limits.heartbeatSeconds <= 0 || limits.walLagBytes <= 0 || limits.walLagSeconds <= 0 {
		t.Fatalf("configured readiness limits are not positive: %+v", limits)
	}
	return limits
}

// runRealReadinessWriteLoad commits one registered row in each transaction on a fixed schedule.
// It records the commit offsets from started and returns the first failure.
func runRealReadinessWriteLoad(ctx context.Context, database *sql.DB, started time.Time) realReadinessWriteLoad {
	var load realReadinessWriteLoad
	commitCount := int(realReadinessLoadDuration / realReadinessLoadCommitInterval)
	for commit := 0; commit < commitCount; commit++ {
		scheduled := started.Add(time.Duration(commit) * realReadinessLoadCommitInterval)
		select {
		case <-ctx.Done():
			load.err = ctx.Err()
			return load
		case <-time.After(time.Until(scheduled)):
		}
		recordID := fmt.Sprintf("00000000-0000-4000-8239-%012d", commit+1)
		queryStart := time.Now()
		_, err := database.ExecContext(
			ctx,
			"INSERT INTO public.cf_items (id, owner_id, value) VALUES ($1, 'diagnostic-user', 'readiness-write-load')",
			recordID,
		)
		queryEnd := time.Now()
		if err != nil {
			load.err = fmt.Errorf("insert write-load row %s: %w", recordID, err)
			return load
		}
		load.commits = append(load.commits, time.Since(started))
		executionDuration := queryEnd.Sub(queryStart)
		load.maximumScheduledStartDelay = max(load.maximumScheduledStartDelay, queryStart.Sub(scheduled))
		load.totalSuccessfulExecDuration += executionDuration
		load.maximumSuccessfulExecDuration = max(load.maximumSuccessfulExecDuration, executionDuration)
	}
	return load
}

// slowestRealReadinessLoadSecond returns the write-load second with the fewest commits.
func realReadinessLoadCommitsInWindow(commits []time.Duration) int {
	count := 0
	for _, at := range commits {
		if at < realReadinessLoadDuration {
			count++
		}
	}
	return count
}

// loadRealReadinessSample reads one canonical readiness result in its own transaction.
// The ready value is the value that synchro_readiness returns to the adapter for GET /ready.
func loadRealReadinessSample(t *testing.T, ctx context.Context, database *sql.DB, at time.Duration) realReadinessSample {
	t.Helper()
	detail := loadIssue49Health(t, ctx, database)
	ready, readyOK := detail["ready"].(bool)
	checks, checksOK := detail["checks"].(map[string]any)
	observations, observationsOK := detail["observations"].(map[string]any)
	if !readyOK || !checksOK || !observationsOK {
		t.Fatalf("canonical readiness result is invalid: %#v", detail)
	}
	sample := realReadinessSample{at: at, ready: ready}
	for name, raw := range checks {
		check, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("canonical readiness check %q is invalid: %#v", name, raw)
		}
		if check["state"] != "ok" {
			sample.unhealthy = append(sample.unhealthy, fmt.Sprintf("%s=%v", name, check["reason"]))
		}
	}
	sort.Strings(sample.unhealthy)
	sample.heartbeatSeconds, sample.heartbeatKnown = observations["heartbeat_age_seconds"].(float64)
	sample.walLagBytes, sample.walLagBytesKnown = observations["wal_lag_bytes"].(float64)
	sample.walLagSeconds, sample.walLagSecondsKnown = observations["wal_lag_seconds"].(float64)
	return sample
}

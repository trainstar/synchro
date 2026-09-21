package integration

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func readRealProcessRSSBytes(ctx context.Context, pid int) (int64, error) {
	if ctx == nil || pid <= 0 {
		return 0, fmt.Errorf("RSS process observation is invalid")
	}
	output, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		return 0, fmt.Errorf("read process RSS: %w", err)
	}
	fields := strings.Fields(string(output))
	if len(fields) != 1 {
		return 0, fmt.Errorf("process RSS output is invalid")
	}
	kibibytes, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || kibibytes <= 0 || kibibytes > math.MaxInt64/1024 {
		return 0, fmt.Errorf("process RSS value is invalid")
	}
	return kibibytes * 1024, nil
}

func startRealProcessRSSSampler(
	ctx context.Context,
	pid int,
	maximumRSSBytes int64,
) (int64, func() (int64, error), error) {
	if ctx == nil {
		return 0, nil, fmt.Errorf("RSS process context is invalid")
	}
	if maximumRSSBytes < 0 {
		return 0, nil, fmt.Errorf("RSS process limit is invalid")
	}
	samplingContext, cancel := context.WithCancel(ctx)
	first, err := readRealProcessRSSBytes(samplingContext, pid)
	if err != nil {
		cancel()
		return 0, nil, err
	}
	if maximumRSSBytes > 0 && first > maximumRSSBytes {
		cancel()
		return 0, nil, fmt.Errorf("process RSS value is invalid")
	}

	type samplingResult struct {
		peak int64
		err  error
	}
	result := make(chan samplingResult, 1)
	go func() {
		peak := first
		for {
			timer := time.NewTimer(time.Millisecond)
			select {
			case <-samplingContext.Done():
				if !timer.Stop() {
					<-timer.C
				}
				result <- samplingResult{peak: peak}
				return
			case <-timer.C:
			}
			rss, sampleErr := readRealProcessRSSBytes(samplingContext, pid)
			if samplingContext.Err() != nil {
				result <- samplingResult{peak: peak}
				return
			}
			if sampleErr != nil {
				result <- samplingResult{peak: peak, err: sampleErr}
				return
			}
			if maximumRSSBytes > 0 && rss > maximumRSSBytes {
				result <- samplingResult{peak: peak, err: fmt.Errorf("process RSS value is invalid")}
				return
			}
			if rss > peak {
				peak = rss
			}
		}
	}()

	var once sync.Once
	var observation samplingResult
	stop := func() (int64, error) {
		once.Do(func() {
			cancel()
			observation = <-result
		})
		return observation.peak, observation.err
	}
	return first, stop, nil
}

func TestProcessRSSSamplerCanceledStartReturnsError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := make(chan error, 1)
	go func() {
		_, stop, err := startRealProcessRSSSampler(ctx, os.Getpid(), 0)
		if stop != nil {
			_, _ = stop()
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("canceled RSS sampler start succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled RSS sampler start did not return")
	}
}

func TestProcessRSSSamplerStopIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, stop, err := startRealProcessRSSSampler(ctx, os.Getpid(), 0)
	if err != nil {
		t.Fatalf("start current-process sampler: %v", err)
	}
	defer func() { _, _ = stop() }()
	peak, err := stop()
	if err != nil || peak < first || first <= 0 {
		t.Fatalf("stop current-process sampler: first=%d peak=%d err=%v", first, peak, err)
	}
	repeated, err := stop()
	if err != nil || repeated != peak {
		t.Fatalf("repeated sampler stop changed the result: peak=%d repeated=%d err=%v", peak, repeated, err)
	}
}

package reactnative

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

const maximumNodeTimerMilliseconds int64 = 1<<31 - 1

func newCorpusDetoxCommand(ctx context.Context, arguments ...string) (*exec.Cmd, error) {
	if ctx == nil {
		return nil, errors.New("React Native corpus Detox context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("React Native corpus Detox context is unavailable: %w", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return nil, errors.New("React Native corpus Detox context requires a deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return nil, errors.New("React Native corpus Detox context deadline has expired")
	}
	milliseconds := int64(remaining / time.Millisecond)
	if remaining%time.Millisecond != 0 {
		milliseconds++
	}
	if milliseconds <= 0 || milliseconds > maximumNodeTimerMilliseconds {
		return nil, errors.New("React Native corpus Detox timeout exceeds the Node timer bound")
	}
	commandArguments := make([]string, 0, len(arguments)+3)
	commandArguments = append(commandArguments, "detox")
	commandArguments = append(commandArguments, arguments...)
	commandArguments = append(commandArguments, "--testTimeout", strconv.FormatInt(milliseconds, 10))
	return exec.CommandContext(ctx, "npx", commandArguments...), nil
}

func TestNewCorpusDetoxCommandRequiresLiveFiniteContext(t *testing.T) {
	if _, err := newCorpusDetoxCommand(nil, "test"); err == nil {
		t.Fatal("nil context was accepted")
	}
	if _, err := newCorpusDetoxCommand(context.Background(), "test"); err == nil {
		t.Fatal("context without a deadline was accepted")
	}
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()
	if _, err := newCorpusDetoxCommand(expired, "test"); err == nil {
		t.Fatal("expired context was accepted")
	}
	canceled, cancel := context.WithTimeout(context.Background(), time.Minute)
	cancel()
	if _, err := newCorpusDetoxCommand(canceled, "test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	tooLong, cancelTooLong := context.WithDeadline(
		context.Background(),
		time.Now().Add(time.Duration(maximumNodeTimerMilliseconds+1000)*time.Millisecond),
	)
	defer cancelTooLong()
	if _, err := newCorpusDetoxCommand(tooLong, "test"); err == nil {
		t.Fatal("context beyond the Node timer bound was accepted")
	}
}

func TestNewCorpusDetoxCommandPropagatesRemainingDeadline(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	before := time.Until(deadline)
	command, err := newCorpusDetoxCommand(ctx, "test", "e2e/example.test.ts", "--json")
	if err != nil {
		t.Fatalf("create corpus Detox command: %v", err)
	}
	after := time.Until(deadline)
	if len(command.Args) != 7 ||
		command.Args[1] != "detox" ||
		command.Args[2] != "test" ||
		command.Args[3] != "e2e/example.test.ts" ||
		command.Args[4] != "--json" ||
		command.Args[5] != "--testTimeout" {
		t.Fatalf("corpus Detox arguments = %#v", command.Args)
	}
	milliseconds, err := strconv.ParseInt(command.Args[6], 10, 64)
	if err != nil {
		t.Fatalf("parse corpus Detox timeout: %v", err)
	}
	maximum := int64(before / time.Millisecond)
	if before%time.Millisecond != 0 {
		maximum++
	}
	if milliseconds < after.Milliseconds() || milliseconds > maximum {
		t.Fatalf("corpus Detox timeout = %dms, remaining before=%s after=%s", milliseconds, before, after)
	}
}

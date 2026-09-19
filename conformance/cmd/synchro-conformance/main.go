package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/trainstar/synchro/conformance/scenarios"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "synchro-conformance: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("operation canceled: %w", err)
	}
	if len(args) == 0 {
		return errors.New("command is required")
	}
	switch args[0] {
	case "catalog":
		return runCatalog(ctx, args[1:])
	default:
		return errors.New("unknown command")
	}
}

func runCatalog(ctx context.Context, args []string) error {
	flags := newFlagSet("catalog")
	repoRoot := flags.String("repo-root", "", "repository root")
	write := flags.Bool("write", false, "write the catalog")
	check := flags.Bool("check", false, "check the catalog")
	if err := flags.Parse(args); err != nil {
		return errors.New("catalog flags are invalid")
	}
	if flags.NArg() != 0 {
		return errors.New("catalog does not accept positional arguments")
	}
	if *repoRoot == "" {
		return errors.New("catalog requires --repo-root PATH")
	}
	if *write == *check {
		return errors.New("catalog requires exactly one of --write or --check")
	}
	if *write {
		if err := scenarios.WriteGeneratedCatalog(ctx, *repoRoot); err != nil {
			return operationError(ctx, "catalog write", err)
		}
		return nil
	}
	if err := scenarios.CheckGeneratedCatalog(ctx, *repoRoot); err != nil {
		return operationError(ctx, "catalog check", err)
	}
	return nil
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func operationError(ctx context.Context, operation string, cause error) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(cause, context.DeadlineExceeded) {
		return operationFailure{operation: operation, outcome: "canceled", cause: context.DeadlineExceeded}
	}
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(cause, context.Canceled) {
		return operationFailure{operation: operation, outcome: "canceled", cause: context.Canceled}
	}
	return operationFailure{operation: operation, outcome: "failed", cause: cause}
}

type operationFailure struct {
	operation string
	outcome   string
	cause     error
}

func (failure operationFailure) Error() string {
	message := failure.operation + " " + failure.outcome
	if cause := boundedCauseText(failure.cause); cause != "" {
		message += ": " + cause
	}
	return message
}

func (failure operationFailure) Unwrap() error {
	return failure.cause
}

const maximumCauseTextBytes = 160

func boundedCauseText(cause error) string {
	if cause == nil {
		return ""
	}
	text := cause.Error()
	if index := strings.IndexAny(text, "\r\n"); index >= 0 {
		text = text[:index]
	}
	if index := strings.Index(text, ": "); index >= 0 {
		text = text[:index]
	}
	text = strings.TrimSpace(text)
	if len(text) <= maximumCauseTextBytes {
		return text
	}
	const suffix = "..."
	text = text[:maximumCauseTextBytes-len(suffix)]
	for !utf8.ValidString(text) {
		text = text[:len(text)-1]
	}
	return text + suffix
}

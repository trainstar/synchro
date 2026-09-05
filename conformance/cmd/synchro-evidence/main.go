package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/trainstar/synchro/conformance/evidence"
	"github.com/trainstar/synchro/conformance/inventory"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "synchro-evidence: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if ctx == nil {
		return errors.New("context is nil")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(args) == 0 {
		return errors.New("command is required")
	}
	switch args[0] {
	case "generate":
		return runGenerate(ctx, args[1:])
	case "validate":
		return runValidate(ctx, args[1:])
	case "coverage":
		return runCoverage(ctx, args[1:])
	case "coverage-report":
		return runCoverageReport(ctx, args[1:])
	default:
		return errors.New("unknown command")
	}
}

func runGenerate(ctx context.Context, args []string) error {
	flags := newFlagSet("generate")
	repoRoot := flags.String("repo-root", "", "repository root")
	inputPath := flags.String("input", "", "terminal CI input")
	outputPath := flags.String("output", "", "generated CI summary")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *repoRoot == "" || *inputPath == "" || *outputPath == "" {
		return errors.New("generate requires --repo-root, --input, and --output")
	}
	data, err := os.ReadFile(*inputPath)
	if err != nil {
		return fmt.Errorf("read CI input: %w", err)
	}
	input, err := evidence.DecodeInput(data)
	if err != nil {
		return err
	}
	summary, err := evidence.Generate(ctx, *repoRoot, input)
	if err != nil {
		return fmt.Errorf("generate CI summary: %w", err)
	}
	var output bytes.Buffer
	if err := evidence.Encode(&output, summary); err != nil {
		return err
	}
	return publish(*outputPath, output.Bytes())
}

func runValidate(ctx context.Context, args []string) error {
	flags := newFlagSet("validate")
	repoRoot := flags.String("repo-root", "", "repository root")
	summaryPath := flags.String("summary", "", "generated CI summary")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *repoRoot == "" || *summaryPath == "" {
		return errors.New("validate requires --repo-root and --summary")
	}
	summary, err := readSummary(*summaryPath)
	if err != nil {
		return err
	}
	if err := evidence.Validate(ctx, *repoRoot, summary); err != nil {
		return fmt.Errorf("validate CI summary: %w", err)
	}
	return nil
}

func runCoverage(ctx context.Context, args []string) error {
	flags := newFlagSet("coverage")
	repoRoot := flags.String("repo-root", "", "repository root")
	summaryPath := flags.String("summary", "", "generated CI summary")
	jsonPath := flags.String("json", "", "coverage JSON output")
	markdownPath := flags.String("markdown", "", "coverage Markdown output")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *repoRoot == "" || *summaryPath == "" || *jsonPath == "" || *markdownPath == "" {
		return errors.New("coverage requires --repo-root, --summary, --json, and --markdown")
	}
	summary, err := readSummary(*summaryPath)
	if err != nil {
		return err
	}
	if err := evidence.Validate(ctx, *repoRoot, summary); err != nil {
		return fmt.Errorf("validate CI summary: %w", err)
	}
	report, err := inventory.Project(summary)
	if err != nil {
		return fmt.Errorf("project CI coverage: %w", err)
	}
	var jsonOutput, markdownOutput bytes.Buffer
	if err := inventory.WriteJSON(&jsonOutput, report); err != nil {
		return err
	}
	if err := inventory.WriteMarkdown(&markdownOutput, report); err != nil {
		return err
	}
	if err := publish(*jsonPath, jsonOutput.Bytes()); err != nil {
		return err
	}
	return publish(*markdownPath, markdownOutput.Bytes())
}

func runCoverageReport(ctx context.Context, args []string) error {
	flags := newFlagSet("coverage-report")
	repoRoot := flags.String("repo-root", "", "repository root")
	summaryPath := flags.String("summary", "", "phase-5 CI summary")
	jsonPath := flags.String("json", "", "requirement coverage JSON output")
	markdownPath := flags.String("markdown", "", "requirement coverage Markdown output")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 || *repoRoot == "" || *summaryPath == "" || *jsonPath == "" || *markdownPath == "" {
		return errors.New("coverage-report requires --repo-root, --summary, --json, and --markdown")
	}
	summary, err := readSummary(*summaryPath)
	if err != nil {
		return err
	}
	report, err := evidence.GenerateRequirementCoverage(ctx, *repoRoot, summary)
	if err != nil {
		return fmt.Errorf("generate requirement coverage: %w", err)
	}
	var jsonOutput, markdownOutput bytes.Buffer
	if err := evidence.WriteRequirementCoverageJSON(&jsonOutput, report); err != nil {
		return err
	}
	if err := evidence.WriteRequirementCoverageMarkdown(&markdownOutput, report); err != nil {
		return err
	}
	if err := publish(*jsonPath, jsonOutput.Bytes()); err != nil {
		return err
	}
	return publish(*markdownPath, markdownOutput.Bytes())
}

func readSummary(path string) (evidence.Summary, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return evidence.Summary{}, fmt.Errorf("read CI summary: %w", err)
	}
	return evidence.DecodeSummary(data)
}

func publish(path string, data []byte) error {
	if path == "" {
		return errors.New("output path is empty")
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".synchro-evidence-*")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect temporary output: %w", err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary output: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary output: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish output: %w", err)
	}
	return nil
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

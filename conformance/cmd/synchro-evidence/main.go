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
	"github.com/trainstar/synchro/conformance/internal/contract"
	"github.com/trainstar/synchro/conformance/inventory"
	"github.com/trainstar/synchro/conformance/scenarios"
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
	case "validate":
		return runValidate(ctx, args[1:])
	case "inventory":
		return runInventory(ctx, args[1:])
	default:
		return errors.New("unknown command")
	}
}

func runValidate(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("validate", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repoRoot := flags.String("repo-root", "", "repository root")
	candidateDir := flags.String("candidate-dir", "", "candidate directory")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("validate flags are invalid")
	}
	if *repoRoot == "" || *candidateDir == "" {
		return errors.New("validate requires --repo-root and --candidate-dir")
	}
	return evidence.ValidateCandidate(ctx, *repoRoot, *candidateDir)
}

func runInventory(ctx context.Context, args []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	flags := flag.NewFlagSet("inventory", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	repoRoot := flags.String("repo-root", "", "repository root")
	candidateDir := flags.String("candidate-dir", "", "candidate directory")
	if err := flags.Parse(args); err != nil || flags.NArg() != 0 {
		return errors.New("inventory flags are invalid")
	}
	if *repoRoot == "" || *candidateDir == "" {
		return errors.New("inventory requires --repo-root and --candidate-dir")
	}
	root, err := filepath.Abs(*repoRoot)
	if err != nil {
		return errors.New("inventory repository root is invalid")
	}
	candidateRoot, err := filepath.Abs(*candidateDir)
	if err != nil {
		return errors.New("inventory candidate directory is invalid")
	}
	candidateRoot = filepath.Clean(candidateRoot)
	base := filepath.Base(candidateRoot)
	expectedRoot := filepath.Join(filepath.Clean(root), "dist", "verification", base)
	if base == "." || candidateRoot != expectedRoot {
		return errors.New("inventory candidate directory must be dist/verification/<candidate-id>")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	bundle, err := contract.Load(ctx, root)
	if err != nil {
		return fmt.Errorf("inventory load contract: %w", err)
	}
	allScenarios, err := scenarios.LoadAll(ctx, root)
	if err != nil {
		return fmt.Errorf("inventory load scenarios: %w", err)
	}
	candidate, err := evidence.LoadCandidate(ctx, root, candidateRoot)
	if err != nil {
		return fmt.Errorf("inventory load candidate: %w", err)
	}
	if candidate.ID != base {
		return errors.New("inventory candidate directory does not match candidate ID")
	}
	report, err := inventory.Generate(ctx, inventory.Inputs{Contract: bundle, Scenarios: allScenarios, EvidenceRoot: "evidence", Candidate: candidate})
	if err != nil {
		return fmt.Errorf("inventory generate report: %w", err)
	}
	var jsonData, markdownData bytes.Buffer
	if err := inventory.WriteJSON(&jsonData, report); err != nil {
		return fmt.Errorf("inventory write JSON: %w", err)
	}
	if err := inventory.WriteMarkdown(&markdownData, report); err != nil {
		return fmt.Errorf("inventory write Markdown: %w", err)
	}
	if err := publishInventory(candidateRoot, "inventory.json", jsonData.Bytes()); err != nil {
		return err
	}
	if err := publishInventory(candidateRoot, "inventory.md", markdownData.Bytes()); err != nil {
		return err
	}
	return nil
}

func publishInventory(root, name string, data []byte) error {
	temporary, err := os.CreateTemp(root, ".inventory-*")
	if err != nil {
		return fmt.Errorf("inventory create temporary %s: %w", name, err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("inventory protect temporary %s: %w", name, err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("inventory write temporary %s: %w", name, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("inventory sync temporary %s: %w", name, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("inventory close temporary %s: %w", name, err)
	}
	finalName := filepath.Join(root, name)
	if err := os.Link(temporaryName, finalName); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("inventory output already exists: %s", name)
		}
		return fmt.Errorf("inventory publish %s: %w", name, err)
	}
	directory, err := os.Open(root)
	if err != nil {
		return fmt.Errorf("inventory open candidate directory: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("inventory sync candidate directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("inventory close candidate directory: %w", err)
	}
	return nil
}

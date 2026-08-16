package main

import (
	"encoding/json"
	"errors"
	"io"
	"strings"
	"time"
)

type result string

const (
	resultTargetPass                result = "target_pass"
	resultTargetSemanticTestFailure result = "target_semantic_test_failure"
	resultPackageSetupFailure       result = "package_setup_failure"
	resultSkip                      result = "skip"
	resultMissingTest               result = "missing_test"
	resultMalformedOutput           result = "malformed_output"
)

type testEvent struct {
	Action  string
	Package string
	Test    string
	Output  string
}

type eventState struct {
	packageName    string
	packageStarted bool
	packageFinal   string
	targetRun      bool
	targetFinal    string
	assertionRun   bool
	assertionFinal string
	outerFailure   bool
	eventCount     int
}

func classifyTestResult(input io.Reader, target string) result {
	if input == nil || !validTargetName(target) {
		return resultMalformedOutput
	}

	decoder := json.NewDecoder(input)
	state := eventState{}
	for {
		var event testEvent
		err := decoder.Decode(&event)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil || !state.accepts(event, target) {
			return resultMalformedOutput
		}
	}
	return state.result()
}

func validTargetName(target string) bool {
	return target != "" && !strings.Contains(target, "/")
}

func (state *eventState) accepts(event testEvent, target string) bool {
	if event.Action == "" || event.Package == "" {
		return false
	}
	if state.packageFinal != "" {
		return false
	}
	if state.packageName == "" {
		state.packageName = event.Package
	} else if state.packageName != event.Package {
		return false
	}
	state.eventCount++

	assertion := target + "/assertion"
	switch event.Action {
	case "start":
		if event.Test != "" || state.packageStarted || state.eventCount != 1 {
			return false
		}
		state.packageStarted = true
		return true
	case "run":
		if !state.packageStarted {
			return false
		}
		return state.acceptRun(event.Test, target, assertion)
	case "pass", "fail", "skip":
		if !state.packageStarted {
			return false
		}
		return state.acceptFinal(event.Test, event.Action, target, assertion)
	case "output", "pause", "cont", "bench":
		if !state.acceptsScopedEvent(event.Test, target, assertion) {
			return false
		}
		if event.Action == "output" && state.isOuterFailureOutput(event, target) {
			state.outerFailure = true
		}
		return true
	default:
		return false
	}
}

func (state eventState) acceptsScopedEvent(name, target, assertion string) bool {
	switch name {
	case "":
		return state.packageStarted
	case target:
		return state.targetRun && state.targetFinal == ""
	case assertion:
		return state.assertionRun && state.assertionFinal == ""
	default:
		return false
	}
}

func (state eventState) isOuterFailureOutput(event testEvent, target string) bool {
	if !state.targetRun || event.Test == target+"/assertion" {
		return false
	}
	if event.Test == "" {
		return !isPackageFrameworkOutput(event.Output, state.packageName)
	}
	return !isTestFrameworkOutput(event.Output, target)
}

func isPackageFrameworkOutput(output, packageName string) bool {
	if output == "PASS\n" || output == "FAIL\n" {
		return true
	}
	if packageName == "" || !strings.HasSuffix(output, "\n") {
		return false
	}
	fields := strings.Split(strings.TrimSuffix(output, "\n"), "\t")
	if len(fields) != 3 || fields[0] != "FAIL" || fields[1] != packageName {
		return false
	}
	duration, err := time.ParseDuration(fields[2])
	return err == nil && duration >= 0
}

func isTestFrameworkOutput(output, target string) bool {
	for _, prefix := range []string{
		"=== RUN   ",
		"=== PAUSE ",
		"=== CONT  ",
		"=== NAME  ",
		"--- PASS: ",
		"--- FAIL: ",
		"--- SKIP: ",
	} {
		if strings.HasPrefix(output, prefix+target) {
			return true
		}
	}
	return false
}

func (state *eventState) acceptRun(name, target, assertion string) bool {
	switch name {
	case target:
		if state.targetRun {
			return false
		}
		state.targetRun = true
		return true
	case assertion:
		if !state.targetRun || state.targetFinal != "" || state.assertionRun {
			return false
		}
		state.assertionRun = true
		return true
	default:
		return false
	}
}

func (state *eventState) acceptFinal(name, action, target, assertion string) bool {
	switch name {
	case "":
		if !state.packageStarted || state.packageFinal != "" {
			return false
		}
		if state.targetRun && state.targetFinal == "" {
			return false
		}
		state.packageFinal = action
		return true
	case target:
		if !state.targetRun || state.targetFinal != "" {
			return false
		}
		if state.assertionRun && state.assertionFinal == "" {
			return false
		}
		state.targetFinal = action
		return true
	case assertion:
		if !state.assertionRun || state.assertionFinal != "" {
			return false
		}
		state.assertionFinal = action
		return true
	default:
		return false
	}
}

func (state eventState) result() result {
	if state.eventCount == 0 || !state.packageStarted || state.packageFinal == "" {
		return resultMalformedOutput
	}
	if !state.targetRun {
		switch state.packageFinal {
		case "fail":
			return resultPackageSetupFailure
		case "skip":
			return resultSkip
		case "pass":
			return resultMissingTest
		default:
			return resultMalformedOutput
		}
	}
	if state.targetFinal == "skip" || state.assertionFinal == "skip" {
		if state.packageFinal == "fail" {
			return resultPackageSetupFailure
		}
		return resultSkip
	}
	if !state.assertionRun || state.assertionFinal == "" {
		return resultPackageSetupFailure
	}
	switch state.assertionFinal {
	case "fail":
		if state.outerFailure {
			return resultPackageSetupFailure
		}
		if state.targetFinal == "fail" && state.packageFinal == "fail" {
			return resultTargetSemanticTestFailure
		}
		return resultMalformedOutput
	case "pass":
		if state.targetFinal == "pass" && state.packageFinal == "pass" {
			return resultTargetPass
		}
		return resultPackageSetupFailure
	default:
		return resultMalformedOutput
	}
}

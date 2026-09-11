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
	packageName     string
	packageStarted  bool
	packageFinal    string
	targetRun       bool
	targetFinal     string
	targetPaused    bool
	assertionRun    bool
	assertionFinal  string
	assertionPaused bool
	descendants     map[string]string
	descendantFail  bool
	outerFailure    bool
	eventCount      int
}

func classifyTestResult(input io.Reader, target string) result {
	testName, assertionName, valid := exactTestNames(target)
	if input == nil || !valid {
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
		if err != nil || !state.accepts(event, testName, assertionName) {
			return resultMalformedOutput
		}
	}
	return state.result()
}

func validTargetName(target string) bool {
	_, _, valid := exactTestNames(target)
	return valid
}

func exactTestNames(target string) (string, string, bool) {
	parts := strings.Split(target, "/")
	switch len(parts) {
	case 1:
		if parts[0] == "" {
			return "", "", false
		}
		return parts[0], parts[0] + "/assertion", true
	case 2:
		if parts[0] == "" || !validAssertionName(parts[1]) {
			return "", "", false
		}
		return parts[0], target, true
	default:
		return "", "", false
	}
}

func validAssertionName(name string) bool {
	if name == "assertion" {
		return true
	}
	if len(name) != len("assertion#00") || !strings.HasPrefix(name, "assertion#") {
		return false
	}
	tens, ones := name[len(name)-2], name[len(name)-1]
	return tens >= '0' && tens <= '9' && ones >= '0' && ones <= '9' && (tens != '0' || ones != '0')
}

func (state *eventState) accepts(event testEvent, target, assertion string) bool {
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
	case "output", "bench":
		if !state.acceptsScopedEvent(event.Test, target, assertion) {
			return false
		}
		if event.Action == "output" && state.isOuterFailureOutput(event, target, assertion) {
			state.outerFailure = true
		}
		return true
	case "pause":
		return state.acceptPause(event.Test, target, assertion)
	case "cont":
		return state.acceptContinue(event.Test, target, assertion)
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
		return isAssertionDescendant(name, assertion) && (state.descendants[name] == "run" || state.descendants[name] == "pause")
	}
}

func (state eventState) isOuterFailureOutput(event testEvent, target, assertion string) bool {
	if !state.targetRun || event.Test == assertion || isAssertionDescendant(event.Test, assertion) {
		return false
	}
	if event.Test == "" {
		return !isPackageFrameworkOutput(event.Output, state.packageName, target, assertion)
	}
	return !isTestFrameworkOutput(event.Output, target)
}

func isPackageFrameworkOutput(output, packageName, target, assertion string) bool {
	if output == "PASS\n" || output == "FAIL\n" {
		return true
	}
	for _, prefix := range []string{"--- PASS: ", "--- FAIL: ", "--- SKIP: "} {
		if !strings.HasPrefix(output, prefix) {
			continue
		}
		name, _, found := strings.Cut(strings.TrimPrefix(output, prefix), " (")
		return found && (name == target || name == assertion || isAssertionDescendant(name, assertion))
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
		if !state.targetRun || state.targetFinal != "" || state.targetPaused || state.assertionRun {
			return false
		}
		state.assertionRun = true
		return true
	default:
		if !state.targetRun || state.targetFinal != "" || !state.assertionRun || state.assertionFinal != "" || state.targetPaused || state.assertionPaused || !isAssertionDescendant(name, assertion) {
			return false
		}
		parent := name[:strings.LastIndex(name, "/")]
		if parent != assertion && state.descendants[parent] != "run" {
			return false
		}
		if state.descendants == nil {
			state.descendants = make(map[string]string)
		}
		if _, found := state.descendants[name]; found {
			return false
		}
		state.descendants[name] = "run"
		return true
	}
}

func (state *eventState) acceptPause(name, target, assertion string) bool {
	switch name {
	case target:
		if !state.targetRun || state.targetFinal != "" || state.targetPaused || (state.assertionRun && state.assertionFinal == "") {
			return false
		}
		state.targetPaused = true
		return true
	case assertion:
		if !state.assertionRun || state.assertionFinal != "" || state.assertionPaused {
			return false
		}
		for _, descendantState := range state.descendants {
			if descendantState == "run" || descendantState == "pause" {
				return false
			}
		}
		state.assertionPaused = true
		return true
	default:
		if !isAssertionDescendant(name, assertion) || state.descendants[name] != "run" {
			return false
		}
		for descendant, descendantState := range state.descendants {
			if (descendantState == "run" || descendantState == "pause") && strings.HasPrefix(descendant, name+"/") {
				return false
			}
		}
		state.descendants[name] = "pause"
		return true
	}
}

func (state *eventState) acceptContinue(name, target, assertion string) bool {
	switch name {
	case target:
		if !state.targetPaused {
			return false
		}
		state.targetPaused = false
		return true
	case assertion:
		if !state.assertionPaused {
			return false
		}
		state.assertionPaused = false
		return true
	default:
		if !isAssertionDescendant(name, assertion) || state.descendants[name] != "pause" {
			return false
		}
		state.descendants[name] = "run"
		return true
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
		if !state.targetRun || state.targetFinal != "" || state.targetPaused {
			return false
		}
		if state.assertionRun && state.assertionFinal == "" {
			return false
		}
		state.targetFinal = action
		return true
	case assertion:
		if !state.assertionRun || state.assertionFinal != "" || state.assertionPaused {
			return false
		}
		for _, descendantState := range state.descendants {
			if descendantState == "run" || descendantState == "pause" {
				return false
			}
		}
		if state.descendantFail && action != "fail" {
			return false
		}
		state.assertionFinal = action
		return true
	default:
		if !isAssertionDescendant(name, assertion) || state.descendants[name] != "run" {
			return false
		}
		for descendant, descendantState := range state.descendants {
			if (descendantState == "run" || descendantState == "pause") && strings.HasPrefix(descendant, name+"/") {
				return false
			}
		}
		state.descendants[name] = action
		state.descendantFail = state.descendantFail || action == "fail"
		return true
	}
}

func isAssertionDescendant(name, assertion string) bool {
	return strings.HasPrefix(name, assertion+"/") && len(name) > len(assertion)+1
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

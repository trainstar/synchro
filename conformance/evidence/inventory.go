package evidence

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/trainstar/synchro/conformance/internal/jsonstrict"
)

// LoadTerminalEvidence loads fully validated terminal attempts from one
// candidate-relative evidence directory. It does not require release closure.
func LoadTerminalEvidence(ctx context.Context, candidate Candidate, evidenceRoot string) ([]Evidence, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if candidate.RepoRoot == "" || candidate.Root == "" || candidate.ID == "" || !validCandidatePath(evidenceRoot) {
		return nil, fmt.Errorf("%w: inventory evidence root", ErrInvalidEvidence)
	}
	if err := verifyCandidateRoot(candidate); err != nil {
		return nil, fmt.Errorf("%w: inventory candidate root: %v", ErrInvalidEvidence, err)
	}
	root := filepath.Join(candidate.Root, filepath.FromSlash(evidenceRoot))
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: inventory evidence directory", ErrInvalidEvidence)
	}
	paths := make([]string, 0)
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("evidence directory contains a symbolic link")
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() || filepath.Ext(entry.Name()) != ".json" {
			return fmt.Errorf("evidence directory contains a non-JSON file")
		}
		relative, err := filepath.Rel(candidate.Root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if !validCandidatePath(relative) {
			return fmt.Errorf("evidence file path is invalid")
		}
		paths = append(paths, relative)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("%w: walk inventory evidence: %v", ErrInvalidEvidence, err)
	}
	if err := verifyCandidateRoot(candidate); err != nil {
		return nil, fmt.Errorf("%w: inventory candidate root: %v", ErrInvalidEvidence, err)
	}
	if len(paths) == 0 {
		return nil, fmt.Errorf("%w: inventory evidence directory is empty", ErrInvalidEvidence)
	}
	sort.Strings(paths)

	values := make([]Evidence, 0, len(paths))
	seenEvidence := make(map[string]struct{}, len(paths))
	seenReceipts := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if err := contextError(ctx); err != nil {
			return nil, err
		}
		data, _, err := readLockedCandidateFile(candidate, path)
		if err != nil {
			return nil, fmt.Errorf("%w: read inventory evidence: %v", ErrInvalidEvidence, err)
		}
		if err := rejectInventoryClaims(data); err != nil {
			return nil, err
		}
		var item Evidence
		if err := decodeClosedEvidence(data, &item); err != nil {
			return nil, err
		}
		if err := validateInventoryEvidence(ctx, candidate, item, path); err != nil {
			return nil, err
		}
		if _, exists := seenEvidence[item.EvidenceID]; exists {
			return nil, fmt.Errorf("%w: duplicate inventory evidence ID", ErrInvalidEvidence)
		}
		if _, exists := seenReceipts[item.ReceiptID]; exists {
			return nil, fmt.Errorf("%w: replayed receipt", ErrInvalidEvidence)
		}
		seenEvidence[item.EvidenceID] = struct{}{}
		seenReceipts[item.ReceiptID] = struct{}{}
		values = append(values, item)
	}
	if err := validateTerminalLineage(values); err != nil {
		return nil, err
	}
	return terminalEvidence(values), nil
}

func decodeClosedEvidence(data []byte, destination *Evidence) error {
	var members map[string]json.RawMessage
	if err := jsonstrict.Decode(data, &members); err != nil {
		return fmt.Errorf("%w: decode inventory evidence: %v", ErrInvalidEvidence, err)
	}
	allowed := map[string]struct{}{
		"$schema": {}, "schema_version": {}, "evidence_id": {}, "receipt_id": {}, "evidence_class": {}, "candidate_id": {},
		"release_version": {}, "protocol_version": {}, "contract_snapshot_sha256": {}, "support_cell_id": {},
		"scenario_id": {}, "proof_obligation_id": {}, "requirement_ids": {}, "proof_type": {}, "source_commit": {},
		"generator": {}, "run": {}, "environment": {}, "assertions": {}, "attachments": {}, "attachment_ids": {},
		"execution_artifacts": {}, "replay": {}, "fault_execution": {}, "performance_results": {},
		"required_measurement_results": {}, "vector_results": {}, "artifact_bindings": {}, "http_observations": {},
		"counters": {}, "observations": {}, "negative_control": {}, "seed": {}, "runner_digest": {}, "receipt": {},
	}
	for name := range members {
		if _, found := allowed[name]; !found {
			return fmt.Errorf("%w: unknown inventory evidence field %q", ErrInvalidEvidence, name)
		}
	}
	if len(members) != len(allowed) {
		return fmt.Errorf("%w: incomplete inventory evidence", ErrInvalidEvidence)
	}
	if err := jsonstrict.Decode(data, destination); err != nil {
		return fmt.Errorf("%w: decode inventory evidence: %v", ErrInvalidEvidence, err)
	}
	return nil
}

func validateInventoryEvidence(ctx context.Context, candidate Candidate, item Evidence, path string) error {
	if err := validateEvidence(ctx, candidate.RepoRoot, candidate, item, path, &FinalManifest{}); err != nil {
		return fmt.Errorf("%w: inventory semantic validation: %v", ErrInvalidEvidence, err)
	}
	return nil
}

func terminalEvidence(values []Evidence) []Evidence {
	byKey := make(map[string]Evidence)
	for _, item := range values {
		key := item.CandidateID + "\x00" + item.ScenarioID + "\x00" + item.ProofObligationID + "\x00" + nullableString(item.SupportCellID)
		if prior, found := byKey[key]; !found || prior.Run.Attempt < item.Run.Attempt {
			byKey[key] = item
		}
	}
	result := make([]Evidence, 0, len(byKey))
	for _, item := range byKey {
		result = append(result, item)
	}
	sort.Slice(result, func(left, right int) bool {
		return terminalEvidenceKey(result[left]) < terminalEvidenceKey(result[right])
	})
	return result
}

func terminalEvidenceKey(item Evidence) string {
	return strings.Join([]string{item.ScenarioID, item.ProofObligationID, nullableString(item.SupportCellID), item.EvidenceID}, "\x00")
}

func rejectInventoryClaims(data []byte) error {
	var value any
	if err := jsonstrict.Decode(data, &value); err != nil {
		return fmt.Errorf("%w: decode inventory claim: %v", ErrInvalidEvidence, err)
	}
	if hasInventoryClaim(value) {
		return fmt.Errorf("%w: inventory contains a prohibited claim", ErrInvalidEvidence)
	}
	return nil
}

func hasInventoryClaim(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalized := strings.ReplaceAll(strings.ReplaceAll(strings.ToLower(key), "_", "-"), " ", "-")
			if normalized == "readiness" || normalized == "covered" || normalized == "verified" || normalized == "certified" || normalized == "promotion" || normalized == "promotion-status" {
				return true
			}
			if hasInventoryClaim(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if hasInventoryClaim(child) {
				return true
			}
		}
	}
	return false
}

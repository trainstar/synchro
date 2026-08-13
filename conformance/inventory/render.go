package inventory

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// WriteJSON writes a deterministic JSON projection after validation.
func WriteJSON(w io.Writer, r Report) error {
	if w == nil {
		return fmt.Errorf("inventory JSON writer is nil")
	}
	if err := r.Validate(); err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(r)
}

// WriteMarkdown writes an evidence projection after validation.
func WriteMarkdown(w io.Writer, r Report) error {
	if w == nil {
		return fmt.Errorf("inventory Markdown writer is nil")
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "# Generated evidence inventory\n\nCandidate: `%s`  \nProtocol: `%d`\n\n", r.CandidateID, r.ProtocolVersion); err != nil {
		return err
	}
	if _, err := io.WriteString(w, "| Requirement | Scenario | Obligation | Assertion | Support cell | Proof type | Result | Lineage | Artifacts | Environment | Seed | Attachments | Negative control |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n"); err != nil {
		return err
	}
	for _, row := range r.Rows {
		lineage := row.Lineage.EvidenceID + ", " + row.Lineage.ReceiptID + ", " + row.Lineage.ExecutionLineageID + ", attempt " + fmt.Sprint(row.Lineage.Attempt)
		control := ""
		if row.NegativeControl != nil {
			control = row.NegativeControl.ControlID + ", " + row.NegativeControl.FaultID + ", " + row.NegativeControl.Outcome
		}
		line := []string{row.RequirementID, row.ScenarioID, row.ProofObligationID, row.AssertionID, nullable(row.SupportCellID), row.ProofType, string(row.Result), lineage, markdownArtifacts(row), markdownEnvironment(row), nullable(row.Seed), markdownAttachments(row), control}
		for index := range line {
			line[index] = markdownCell(line[index])
		}
		if _, err := fmt.Fprintf(w, "| %s |\n", strings.Join(line, " | ")); err != nil {
			return err
		}
	}
	return nil
}

func markdownArtifacts(row Row) string {
	values := make([]string, 0, len(row.Artifacts))
	for _, item := range row.Artifacts {
		values = append(values, item.InventoryID+"/"+item.ArtifactID+"/"+item.Path+"/"+item.SHA256)
	}
	return strings.Join(values, "; ")
}

func markdownEnvironment(row Row) string {
	values := make([]string, 0, len(row.Environment))
	for _, item := range row.Environment {
		values = append(values, item.Name+"="+item.Value)
	}
	return strings.Join(values, "; ")
}

func markdownAttachments(row Row) string {
	values := make([]string, 0, len(row.Attachments))
	for _, item := range row.Attachments {
		values = append(values, item.ID+"/"+item.Path+"/"+item.SHA256)
	}
	return strings.Join(values, "; ")
}

func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "|", "\\|")
	value = strings.ReplaceAll(value, "\r", " ")
	return strings.ReplaceAll(value, "\n", "<br>")
}

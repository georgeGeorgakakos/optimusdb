package contextualmetadata

import (
	"fmt"
	"strings"
)

type EnrichmentRequest struct {
	DB       string
	Table    string
	Profile  *DatasetProfile
	UseGreek bool // optional: bilingual outputs
}

// Build a concise prompt for a 1.1B chat model. Keep it deterministic & structured.
func BuildPrompt(req EnrichmentRequest) string {
	var b strings.Builder
	fmt.Fprintf(&b, "You are a data catalog assistant. Summarize the dataset meaningfully from its columns and example values.\n")
	fmt.Fprintf(&b, "Return JSON with fields: description (<=120 words), tags (3-10 keywords), columns (array of {name, role, notes}).\n")
	if req.UseGreek {
		fmt.Fprintf(&b, "Write the description in Greek, but keep tags in English.\n")
	}
	fmt.Fprintf(&b, "\nDATASET: %s.%s\n", req.DB, req.Table)
	for _, c := range req.Profile.Profiles {
		fmt.Fprintf(&b, "\n# Column: %s\n", c.Name)
		fmt.Fprintf(&b, "- inferred_type: %s\n", c.InferredType)
		fmt.Fprintf(&b, "- null_ratio: %.2f\n", c.NullRatio)
		fmt.Fprintf(&b, "- cardinality: %d\n", c.Cardinality)
		if len(c.ExampleValues) > 0 {
			fmt.Fprintf(&b, "- examples: %s\n", strings.Join(quoteSlice(c.ExampleValues[:min(5, len(c.ExampleValues))]), ", "))
		}
		if c.IsIdentifier {
			fmt.Fprintf(&b, "- hint: identifier\n")
		}
		if c.IsTimestamp {
			fmt.Fprintf(&b, "- hint: timestamp\n")
		}
		if c.IsGeo {
			fmt.Fprintf(&b, "- hint: geo\n")
		}
		if c.IsCodeLike {
			fmt.Fprintf(&b, "- hint: code-like\n")
		}
	}
	fmt.Fprintf(&b, "\nRespond only with valid JSON.\n")
	return b.String()
}

func quoteSlice(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = fmt.Sprintf("%q", s)
	}
	return out
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

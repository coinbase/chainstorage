package retirement

import (
	"encoding/json"
	"io"
)

func WriteReportJSON(w io.Writer, report *Report) error {
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(report)
}

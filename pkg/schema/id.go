package schema

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
)

func (e LogEvent) DeterministicID() string {
	h := sha256.New()

	io.WriteString(h, e.Service)
	io.WriteString(h, "\x1f")
	io.WriteString(h, e.Severity)
	io.WriteString(h, "\x1f")
	io.WriteString(h, e.Message)
	io.WriteString(h, "\x1f")
	io.WriteString(h, fmt.Sprintf("%d", e.EventTime.UnixNano()))
	io.WriteString(h, "\x1f")
	io.WriteString(h, e.TraceID)

	return hex.EncodeToString(h.Sum(nil))
}

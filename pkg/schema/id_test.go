package schema

import (
	"testing"
	"time"
)

func TestDeterministicIDStable(t *testing.T) {
	event := LogEvent{
		Service:   "my-service",
		Severity:  "ERROR",
		Message:   "Something went wrong",
		EventTime: time.Date(2026, 6, 12, 14, 30, 0, 0, time.UTC),
		TraceID:   "abc123",
	}

	id1 := event.DeterministicID()
	id2 := event.DeterministicID()

	if id1 != id2 {
		t.Errorf("Expected deterministic IDs to be the same, got %s and %s", id1, id2)
	}
}

func TestDeterministicIDDistinct(t *testing.T) {
	event1 := LogEvent{
		Service:   "my-service",
		Severity:  "ERROR",
		Message:   "Something went wrong",
		EventTime: time.Date(2026, 6, 12, 14, 30, 0, 0, time.UTC),
		TraceID:   "abc123",
	}

	event2 := LogEvent{
		Service:   "my-service",
		Severity:  "ERROR",
		Message:   "Something went wrong",
		EventTime: time.Date(2026, 6, 12, 14, 30, 0, 0, time.UTC),
		TraceID:   "def456", // Different TraceID
	}

	id1 := event1.DeterministicID()
	id2 := event2.DeterministicID()

	if id1 == id2 {
		t.Errorf("Expected deterministic IDs to be different, got %s and %s", id1, id2)
	}
}

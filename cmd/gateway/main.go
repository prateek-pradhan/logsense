package main

import (
	"encoding/json"
	"fmt"
	"github.com/prateek-pradhan/logsense/pkg/schema"
	"log"
	"net/http"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.HandleFunc("POST /v1/logs", handleIngest)
	addr := ":8080"
	log.Printf("gateway listening on %s", addr)

	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatalf("failed to start gateway: %v", err)
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

const (
	maxBodyBytes = 5 << 20
	maxBatchSize = 1000
)

var validSeverities = map[string]bool{
	"DEBUG": true,
	"INFO":  true,
	"WARN":  true,
	"ERROR": true,
	"FATAL": true,
}

func handleIngest(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)

	var events []schema.LogEvent
	if err := json.NewDecoder(r.Body).Decode(&events); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	if len(events) == 0 {
		http.Error(w, "no events provided", http.StatusBadRequest)
		return
	}

	if len(events) > maxBatchSize {
		http.Error(w, fmt.Sprintf("batch size exceeds limit of %d", maxBatchSize), http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()

	for i := range events {
		if err := validateEvent(events[i]); err != nil {
			http.Error(w, fmt.Sprintf("invalid event at index %d: %v", i, err), http.StatusBadRequest)
			return
		}
		events[i].IngestedAt = now
		if events[i].ID == "" {
			events[i].ID = events[i].DeterministicID()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]int{"accepted": len(events)})
}

func validateEvent(e schema.LogEvent) error {
	if e.Service == "" {
		return fmt.Errorf("service is required")
	}
	if e.Message == "" {
		return fmt.Errorf("message is required")
	}
	if !validSeverities[e.Severity] {
		return fmt.Errorf("invalid severity: %s", e.Severity)
	}
	if e.EventTime.IsZero() {
		return fmt.Errorf("event_time is required")
	}
	return nil
}

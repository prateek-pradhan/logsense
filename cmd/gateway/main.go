package main

import (
	"encoding/json"
	"fmt"
	"github.com/prateek-pradhan/logsense/pkg/schema"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"net/http"
	"os"
	"time"
)

var kafkaClient *kgo.Client

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

func main() {

	var err error
	kafkaClient, err = kgo.NewClient(
		kgo.SeedBrokers(env0r("KAFKA_BROKERS", "localhost:19092")),
	)
	if err != nil {
		log.Fatal("failed to create Kafka client: %v", err)
	}

	defer kafkaClient.Close()

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

	records := make([]*kgo.Record, 0, len(events))

	for i := range events {
		data, err := json.Marshal(events[i])
		if err != nil {
			http.Error(w, "encode: "+err.Error(), http.StatusInternalServerError)
			return
		}
		records = append(records, &kgo.Record{
			Topic: "logs.raw",
			Key:   []byte(events[i].Service),
			Value: data,
		})
	}

	if err := kafkaClient.ProduceSync(r.Context(), records...).FirstErr(); err != nil {
		http.Error(w, "failed to produce to Kafka: "+err.Error(), http.StatusInternalServerError)
		return
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

func env0r(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

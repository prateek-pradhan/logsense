package storage

import (
	"context"
	"github.com/prateek-pradhan/logsense/pkg/schema"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
	"time"
)

func TestConnectPing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := Connect(ctx, "mongodb://localhost:27017")
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer store.Close(ctx)
}

func TestBulkUpsertIdempotency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := Connect(ctx, "mongodb://localhost:27017")
	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}
	defer store.Close(ctx)

	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)

	events := []schema.LogEvent{
		{Service: "test-svc", Severity: "ERROR", Message: "one", EventTime: base},
		{Service: "test-svc", Severity: "ERROR", Message: "two", EventTime: base},
		{Service: "test-svc", Severity: "ERROR", Message: "three", EventTime: base},
	}

	ids := make([]string, len(events))
	for i := range events {
		events[i].ID = events[i].DeterministicID()
		ids[i] = events[i].ID
	}

	filter := bson.M{"_id": bson.M{"$in": ids}}
	if _, err := store.logs.DeleteMany(ctx, filter); err != nil {
		t.Fatalf("Failed to clean up test documents: %v", err)
	}

	if err := store.BulkInsert(ctx, events); err != nil {
		t.Fatalf("First BulkInsert failed: %v", err)
	}

	if err := store.BulkInsert(ctx, events); err != nil {
		t.Fatalf("Second BulkInsert failed: %v", err)
	}

	count, err := store.logs.CountDocuments(ctx, filter)
	if err != nil {
		t.Fatalf("Failed to count documents: %v", err)
	}
	if count != int64(len(events)) {
		t.Fatalf("Expected %d documents after upsert, got %d", len(events), count)
	}

	store.logs.DeleteMany(ctx, filter)
}

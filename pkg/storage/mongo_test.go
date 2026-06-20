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

func TestSearchReturnNewestFirst(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := Connect(ctx, "mongodb://localhost:27017")
	if err != nil {
		t.Fatalf("connect %v", err)
	}
	defer store.Close(ctx)

	base := time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC)

	events := []schema.LogEvent{
		{Service: "search-test", Severity: "INFO", Message: "old", EventTime: base},
		{Service: "search-test", Severity: "INFO", Message: "mid", EventTime: base.Add(time.Minute)},
		{Service: "search-test", Severity: "INFO", Message: "new", EventTime: base.Add(2 * time.Minute)},
	}

	for i := range events {
		events[i].ID = events[i].DeterministicID()
	}

	ids := bson.M{"service": "search-test"}
	store.logs.DeleteMany(ctx, ids)
	if err := store.BulkInsert(ctx, events); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := store.Search(ctx, SearchParams{Service: "search-test", Limit: 10, MaxTimeMS: 2000})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 results, got %d", len(got))
	}

	if got[0].Message != "new" || got[2].Message != "old" {
		t.Errorf("wrong order: got %s ... %s (want new ... old)", got[0].Message, got[2].Message)
	}
	store.logs.DeleteMany(ctx, ids)
}

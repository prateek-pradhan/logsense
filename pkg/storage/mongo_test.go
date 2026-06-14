package storage

import (
	"context"
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

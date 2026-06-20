package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/prateek-pradhan/logsense/pkg/storage"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	store, err := storage.Connect(ctx, envOr("MONGO_URI", "mongodb://localhost:27017"))
	cancel()
	if err != nil {
		log.Fatalf("mongo connect: %v", err)
	}
	defer store.Close(context.Background())

	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", func(w http.ResponseWriter, req *http.Request) {
		w.Write([]byte("ok"))
	})

	addr := ":8081"
	log.Println("api listening on", addr)
	if err := http.ListenAndServe(addr, r); err != nil {
		log.Fatal(err)
	}
}

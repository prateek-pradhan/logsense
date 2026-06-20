package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: dlqctl <inspect|replay> [-limit N]")
	}

	switch os.Args[1] {
	case "inspect":
		fs := flag.NewFlagSet("inspect", flag.ExitOnError)
		limit := fs.Int("limit", 10, "max messages to show")
		fs.Parse(os.Args[2:])
		inspect(*limit)
	case "replay":
		fs := flag.NewFlagSet("replay", flag.ExitOnError)
		limit := fs.Int("limit", 10, "max messages to replay")
		fs.Parse(os.Args[2:])
		replay(*limit)

	default:
		log.Fatalf("unknown command %q (want inspect|replay)", os.Args[1])
	}
}

func newDirectReader() *kgo.Client {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(envOr("KAFKA_BROKERS", "localhost:19092")),
		kgo.ConsumeTopics("logs.dlq"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		log.Fatal("kafka connect: ", err)
	}
	return cl
}

func inspect(limit int) {
	cl := newDirectReader()
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	shown := 0
	for shown < limit {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if shown >= limit {
				return
			}

			shown++

			fmt.Printf("--- dead letter #%d (partition %d, offset %d) ---\n", shown, r.Partition, r.Offset)
			fmt.Printf("  key:   %s\n", string(r.Key))
			fmt.Printf("  value: %s\n", string(r.Value))
			for _, h := range r.Headers {
				fmt.Printf("   headers %s = %s\n", h.Key, string(h.Value))
			}
		})
	}
	fmt.Printf("\nshown %d dead letter(s)\n", shown)
}

func replay(limit int) {
	cl := newDirectReader()
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	replayed := 0
	for replayed < limit {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if replayed >= limit {
				return
			}
			topic := "logs.raw"
			for _, h := range r.Headers {
				if h.Key == "original_topic" {
					topic = string(h.Value)
				}
			}
			cl.Produce(ctx, &kgo.Record{Topic: topic, Key: r.Key, Value: r.Value}, nil)
			replayed++
		})
	}
	if err := cl.Flush(ctx); err != nil {
		log.Printf("flush: %v", err)
	}
	fmt.Printf("replayed %d dead letter(s)\n", replayed)
}

package main

import (
	"context"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func env0r(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(env0r("KAFKA_BROKERS", "localhost:19092")),
		kgo.ConsumerGroup("logsense-writers"),
		kgo.ConsumeTopics("logs.raw"),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	if err != nil {
		log.Fatalf("failed to create Kafka client: %v", err)
	}
	defer client.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Println("consumer started; reading logs.raw (Ctrl+C to stop)")
	count := 0

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(topic string, parition int32, err error) {
			log.Printf("fetch error on topic %s partition %d: %v", topic, parition, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			count++
		})

		log.Printf("consumed so far: %d", count)
	}

	log.Printf("Shutting down consumer, total messages consumed: %d", count)
}

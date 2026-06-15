package main

import (
	"context"
	"encoding/json"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/prateek-pradhan/logsense/pkg/schema"
	"github.com/prateek-pradhan/logsense/pkg/storage"
)

func env0r(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	connCtx, connCancel := context.WithTimeout(context.Background(), 10*time.Second)

	store, err := storage.Connect(connCtx, env0r("MONGO_URI", "mongodb://localhost:27017"))

	connCancel()

	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer store.Close(context.Background())

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

	log.Println("consumer started; reading logs.raw (Ctrl+C to stop)")

	var written, parseFailures, dlqProduced int

	for {
		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			break
		}
		fetches.EachError(func(topic string, parition int32, err error) {
			log.Printf("fetch error on topic %s partition %d: %v", topic, parition, err)
		})

		var records []*kgo.Record
		var events []schema.LogEvent
		var deadletters []*kgo.Record

		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
			var ev schema.LogEvent

			if err := json.Unmarshal(r.Value, &ev); err != nil {
				parseFailures++
				log.Printf("failed to parse record at %s/%d offset %d: %v", r.Topic, r.Partition, r.Offset, err)
				// Permanent failure: preserve original bytes + context
				// in the DLQ for a human to inspect. Headers are key/value
				// metadata attached to the record.
				deadletters = append(deadletters, &kgo.Record{
					Topic: "logs.dlq",
					Key:   r.Key,
					Value: r.Value,
					Headers: []kgo.RecordHeader{
						{Key: "original_topic", Value: []byte(r.Topic)},
						{Key: "original_partition", Value: []byte(strconv.Itoa(int(r.Partition)))},
						{Key: "original_offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
						{Key: "parse_error", Value: []byte(err.Error())},
					},
				})
				return
			}
			events = append(events, ev)
		})

		if len(records) == 0 {
			continue
		}

		writeCtx, writeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := store.BulkInsert(writeCtx, events)
		writeCancel()

		if err != nil {
			log.Printf("failed to write batch of %d events: %v", len(events), err)
			continue
		}

		// Poison messages -> DLQ, BEFORE committing past them, so a crash
		// here re-reads and re-DLQs rather than losing them. ProduceSync
		// waits for the broker ACK, so success means they're safely stored.
		if len(deadletters) > 0 {
			if err := client.ProduceSync(ctx, deadletters...).FirstErr(); err != nil {
				log.Printf("DLQ produce failed (will retry): %v", err)
				continue
			}
			dlqProduced += len(deadletters)
		}

		if err := client.CommitRecords(ctx, records...); err != nil {
			log.Printf("commit failed: %v", err)
			continue
		}

		written += len(events)

		log.Printf("written=%d parseFailures=%d dlqProduced=%d", written, parseFailures, dlqProduced)
	}

	log.Printf("Shutting down consumer, written=%d parseFailures=%d dlqProduced=%d", written, parseFailures, dlqProduced)
}

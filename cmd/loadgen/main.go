package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/prateek-pradhan/logsense/pkg/schema"
	"golang.org/x/time/rate"
)

func main() {
	gateway := flag.String("gateway", "http://localhost:8080", "gateway base URL")
	total := flag.Int("n", 1000, "total events to send")
	ratePerSec := flag.Int("rate", 500, "target events per second")
	workers := flag.Int("workers", 8, "number of concurrent senders goroutines")
	flag.Parse()

	services := []string{"checkout", "auth", "payments", "search", "cart"}
	client := &http.Client{Timeout: 5 * time.Second}

	limiter := rate.NewLimiter(rate.Limit(*ratePerSec), *ratePerSec)

	jobs := make(chan schema.LogEvent, 1000)

	var latencies []time.Duration
	var mu sync.Mutex
	var sent, failed int

	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ev := range jobs {
				if err := limiter.Wait(context.Background()); err != nil {
					return
				}
				t0 := time.Now()
				err := sendOne(client, *gateway, ev)
				elapsed := time.Since(t0)

				mu.Lock()
				if err != nil {
					failed++
				} else {
					sent++
					latencies = append(latencies, elapsed)
				}
				mu.Unlock()
			}
		}()
	}

	for i := 0; i < *total; i++ {
		jobs <- schema.LogEvent{
			ID:        uuid.Must(uuid.NewV7()).String(),
			Service:   services[rand.Intn(len(services))],
			Severity:  "INFO",
			Message:   "synthetic event",
			EventTime: time.Now().UTC(),
		}
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	achieved := float64(sent) / elapsed.Seconds()
	fmt.Print("\n --- loadgen report ---\n")
	fmt.Printf("send = %d failed = %d in %.2fs\n", sent, failed, elapsed.Seconds())
	fmt.Printf("achieved rate: %.0f events/sec (target %d)\n", achieved, *ratePerSec)
	printPercentiles(latencies)

}

func printPercentiles(lat []time.Duration) {
	if len(lat) == 0 {
		fmt.Println("No successful events to report latencies")
		return
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	p := func(q float64) time.Duration {
		idx := int(q * float64(len(lat)))
		if idx >= len(lat) {
			idx = len(lat) - 1
		}
		return lat[idx]
	}
	fmt.Printf("latency p50=%v p95=%v p99=%v\n", p(0.5), p(0.95), p(0.99))
}

func sendOne(client *http.Client, gateway string, ev schema.LogEvent) error {
	body, err := json.Marshal([]schema.LogEvent{ev})
	if err != nil {
		return err
	}

	resp, err := client.Post(gateway+"/v1/logs", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}

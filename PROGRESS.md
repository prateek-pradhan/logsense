# LogSense — Progress

## Current phase: Phase 1 — Gateway, topics, load generator (wrapping up)

## Completed

### Phase −1 — Environment (2026-06-12)
- Go 1.25.0, Docker 28.3.2 + Compose v2.39, kind v0.32, kubectl v1.32, Helm v4.2
- Git repo initialized, conventional commits adopted
- Go survival kit: modules/packages, structs/slices/maps, `if err != nil`, `go test`

### Phase 0 — Foundations (2026-06-12)
- Repo layout: `cmd/{gateway,consumer,api,aggregator,mcp,loadgen}`, `pkg/{schema,storage,kafkautil}`, `deploy/`
- Module: `github.com/prateek-pradhan/logsense`
- `pkg/schema`: `LogEvent` struct (json + bson tags, two timestamps), `DeterministicID()` (SHA-256 content hash)
- Unit tests prove ID stability + distinctness (`go test ./pkg/schema/`)
- `docker-compose.yml`: Redpanda v24.2.4, Mongo 7, pgvector/pg16 — all healthy with healthchecks
- Secrets: `.env` (gitignored) + `.env.example` pattern established

### Phase 1 — Gateway, topics, load generator (2026-06-14)
- Kafka topics: `logs.raw` (24 partitions), `logs.dlq` (6 partitions), r=1 (single local broker)
- Gateway (`cmd/gateway`): `GET /healthz`, `POST /v1/logs` — validates batch,
  caps body/batch size, stamps `ingested_at` + deterministic ID, produces to
  `logs.raw` keyed by service via franz-go (acks=all + idempotent producing by default)
- Load generator (`cmd/loadgen`): worker pool + `rate.Limiter` (token bucket),
  UUIDv7 client IDs, latency p50/p95/p99 reporting
- **Honest throughput baseline (2026-06-14):** ~7,760 events/sec, 0 failures,
  50 workers, **single-event HTTP requests**, target 100k. Bottleneck = one
  HTTP round-trip per event. Fix for later = batch N events per request
  (gateway already accepts batches up to 1000).

## Key decisions
See DECISIONS.md.

## Open issues
- `omitempty` not dropping empty `trace_id`/`attrs` in produced JSON — cosmetic,
  investigate later.
- loadgen sends 1 event/request; batching needed to approach 10k/sec target.

## Next step
Phase 1 close-out: view messages in Redpanda Console, then comprehension check.
Then **Phase 2** — the consumer (at-least-once + idempotent writes, crash test).

## How to resume cold
```
docker compose up -d        # bring up infra
go test ./...               # everything should pass
```
Then read CLAUDE.md + this file.

# LogSense — Decision Log

Each entry: what we chose, what we rejected, and why. These are interview
answers — every entry must be defensible out loud, without notes.

## D1: Content-hash deterministic event IDs (Phase 0)
We derive each event's ID from a SHA-256 hash of its identity fields
(service, severity, message, event_time, trace_id), so the same logical
event always yields the same ID. Combined with Mongo upserts keyed on
`_id`, retries are *absorbed* rather than detected: a duplicate write
lands on the same `_id` and overwrites itself, with no "seen before?"
lookup needed. The alternative — random IDs assigned on arrival — would
turn every network retry into a stored duplicate. Trade-off: `attrs` is
excluded from the hash (Go map iteration order is random; including it
would require key-sorting), so two events differing *only* in attrs
would collapse into one — acceptable for log data.

## D2: Two timestamps per event (Phase 0)
`event_time` (stamped by the source) answers "when did it happen";
`ingested_at` (stamped by our gateway) answers "when did we receive it."
One field can't do both jobs: clients have skewed clocks and delayed
uploads, so `event_time` is useful but untrusted, while `ingested_at` is
ours and trustworthy. Their difference is the pipeline's end-to-end lag
(a key Phase 4 metric), and data expiry (TTL) keys on `ingested_at`
because we control it.

## D3: kind over k3d for local Kubernetes (Phase −1)
Both run a local cluster in Docker. kind is what the Kubernetes project
itself tests with, so its behavior and docs track upstream K8s most
closely — less unlearning when concepts transfer to real clusters. k3d
boots faster, but startup speed isn't our bottleneck; learning fidelity
is. kind's config also makes multi-node clusters easy, which we'll want
for Phase 7 chaos drills.

## D4: Pinned image versions in docker-compose (Phase 0)
`redpanda:v24.2.4`, `mongo:7`, `pgvector:pg16` — never `:latest`.
`:latest` means the stack can silently change under us between pulls,
turning "it worked yesterday" into an archaeology project. Pinning makes
the environment reproducible on any machine, any day. Cost: we must
bump versions deliberately — which is a feature, not a bug.

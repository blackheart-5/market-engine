# Real-Time Market-Data Engine

A streaming engine in Go that ingests live crypto trades over WebSocket, routes them through Kafka partitioned by symbol, computes microstructure analytics in real time (VWAP, volatility, order-flow imbalance, anomaly alerts), and serves them on a live web dashboard. A record/replay harness benchmarks the pipeline reproducibly and verifies the analytics are deterministic.

![dashboard](docs/dashboard.png)

## Architecture

```
Finnhub WebSocket ──► Producer (Go) ──► Kafka topic "ticks" ──► Analytics consumer (Go) ──► Web dashboard
                                         (6 partitions,            (rolling windows,          (localhost:8090)
                                          keyed by symbol)          alerts, latency)
                                              │
                                              ▼
                                         Recorder ──► ticks.jsonl ──► Replayer ──► topic "ticks-replay" ──► same consumer
```

| Component | Path | Role |
|---|---|---|
| Producer | `cmd/producer` | Connects to Finnhub, stamps each trade on arrival, publishes to Kafka keyed by symbol |
| Consumer | `cmd/consumer` | Per-symbol rolling analytics, z-score alerts, latency stats, HTTP dashboard |
| Analytics | `internal/analytics` | O(1) sliding window over trade time |
| Recorder | `cmd/recorder` | Taps the raw `ticks` topic to a JSONL file |
| Replayer | `cmd/replayer` | Feeds a recording back through Kafka at max speed or a fixed rate |
| Dashboard | `web/index.html` | Live table and latency tiles, polling `/stats` every second |

## Benchmark

128,679 recorded live trades, replayed 5× (643,395 ticks, 14 symbols) on a Windows laptop with Redpanda running in Docker.

| Run | Replay rate | Throughput | p50 | p99 | Output digest |
|---|---|---|---|---|---|
| 1 | max | 13,602 ticks/s | 107 ms* | 398 ms* | `09cc6b025019391f` |
| 2 | max | 13,656 ticks/s | 86 ms* | 265 ms* | `09cc6b025019391f` |
| 3 | 6,000 ticks/s | 5,997 ticks/s | **11.9 ms** | **224 ms** | `09cc6b025019391f` |

\* At max speed the replayer outruns the consumer, so these latencies include time spent queued in Kafka. That is consumer lag, not processing latency. Run 3 is the sustained-rate measurement.

- **Peak throughput:** about 13.6k ticks/sec on a single consumer.
- **Sustained:** 6,000 ticks/sec at 12 ms p50 and 224 ms p99, measured end to end from when a tick enters the pipeline to when its analytics are computed.
- **Determinism:** all three runs produced an identical output digest, at two different replay speeds.

Latency is measured as `consumer time − ingest time`, and both timestamps come from the same machine's clock. Exchange timestamps are never used for latency, because clocks on different machines disagree. The live feed showed this machine running about 600 ms behind the exchange.

## Running it

Prerequisites: Go 1.22+, Docker Desktop, and a free [Finnhub](https://finnhub.io) API key.

```bash
docker compose up -d
docker exec -it redpanda rpk topic create ticks -p 6
docker exec -it redpanda rpk topic create ticks-replay -p 6
go mod tidy
```

**Live mode** (two terminals):

```powershell
# terminal 1
$env:FINNHUB_TOKEN="your_key"
go run ./cmd/producer

# terminal 2 (run from the repo root so web/ is found)
go run ./cmd/consumer
```

Then open http://localhost:8090. The Redpanda console is at http://localhost:8080.

On macOS or Linux, use `export FINNHUB_TOKEN=your_key` instead of `$env:`.

## Reproducing the benchmark

Stop the live producer and consumer first so they don't compete for CPU.

```powershell
# 1. record everything in the live topic
go run ./cmd/recorder                       # -> ticks.jsonl

# 2. terminal 2: start a consumer on the replay topic with a fresh group
$env:TOPIC="ticks-replay"; $env:GROUP="bench-1"; go run ./cmd/consumer

# 3. terminal 1: replay
$env:LOOPS="5"; $env:RATE="0"; go run ./cmd/replayer      # RATE=0 means max speed
```

About 5 seconds after the replay finishes, the consumer prints:

```
=== RUN SUMMARY: ticks=643395 symbols=14 time=47.12s throughput=13656 ticks/s p50=85.7ms p99=264.9ms alerts=1062 digest=09cc6b025019391f
```

Before each run, reset the topic (`rpk topic delete ticks-replay`, then `create`) and use a new `GROUP`. For a sustained-rate run, set `RATE` to about half the max throughput.

| Variable | Used by | Default |
|---|---|---|
| `FINNHUB_TOKEN` | producer | required |
| `TOPIC` | consumer, replayer | `ticks` (consumer), `ticks-replay` (replayer) |
| `GROUP` | consumer | `analytics` |
| `RATE` | replayer | `0` (max) |
| `LOOPS` | replayer | `1` |
| `FILE` | replayer | `ticks.jsonl` |

## Analytics

Each symbol has its own window covering the last **60 seconds of trade time**.

- **VWAP:** Σ(price × volume) / Σ(volume).
- **Volatility:** the standard deviation of log returns, in basis points, sampled once per second.
- **Order-flow imbalance:** (buy volume − sell volume) / total volume, from −1 (all selling) to +1 (all buying). Each trade's side is inferred with the **tick rule**: an uptick is a buy, a downtick is a sell, and an unchanged price inherits the previous side.
- **Alerts:** fire when a 1-second return has a z-score of 4 or more against the window's history, after at least 30 samples.

The window keeps running sums and updates them as ticks enter and leave, so each update costs O(1) regardless of window size.

## Design decisions

**Partitioning by symbol.** The symbol is the message key, so Kafka's hash partitioner sends every trade for a symbol to the same partition. That guarantees per-symbol ordering, which the rolling windows depend on, and lets consumers scale horizontally: a second consumer in the same group automatically takes over half the partitions.

**Backpressure: drop and count, never block.** The WebSocket reader hands ticks to the Kafka writer through a bounded channel of 50,000 slots. If the channel fills up, new ticks are dropped and counted rather than blocking the reader. A stalled socket would eventually be disconnected by the feed and lose everything, while a dropped tick loses one data point and shows up in the `dropped=` counter.

**Windows on trade time, not wall-clock time.** The analytics depend only on the trades themselves, never on when they're processed. That's what makes replay deterministic: replaying at 1× or 100× gives identical results. A late tick never moves a window backwards, because windows advance on the maximum trade time seen.

**1-second return sampling.** Computing volatility from returns between individual trades produced absurd z-scores, as high as −289σ. Most consecutive trades print at the same price or bounce between the bid and ask, which makes the measured volatility tiny and every real move look extreme. This is known as microstructure noise, and sampling the price once per second removes it. Order-flow imbalance still uses every trade.

**Async offset commits (at-least-once).** By default the consumer committed its offset synchronously after every message. Switching to a background commit every second raised throughput about 10×, from ~1.4k to ~13.6k ticks/sec. The trade-off is that after a crash, up to one second of ticks is processed twice. For analytics that's harmless.

**Per-symbol determinism digests.** Kafka orders messages within a partition, not across partitions, so BTC and ETH can interleave differently on every run. The consumer therefore hashes each symbol's output stream separately, then combines the hashes in sorted symbol order. The digest is stable across runs and changes if any computed value changes.

## Known limitations and next steps

- **The p99 tail.** p99 is about 19× p50. The 5-second windows show periodic stalls rather than a uniformly slow pipeline. The likely causes are Go garbage collection and the Docker-on-Windows network hop to the broker. Next steps are profiling with `pprof`, tuning `GOGC`, and benchmarking against a native broker.
- **Producer reconnect.** The producer exits on a WebSocket error. It should reconnect with exponential backoff.
- **Symbol coverage.** 20 symbols are subscribed, but Finnhub sent no trades for 6 of them (FIL, NEAR, SOL, TRX, UNI, XRP), so the benchmark covers 14.
- **Persistence.** Analytics live in memory. A time-series store such as TimescaleDB or QuestDB would add history behind the dashboard.
- **Better anomaly models.** The z-score rule could be compared against EWMA volatility or rolling quantiles.
- **Horizontal scale test.** Run two consumers in one group and measure the partition rebalance.

## Deploying to Fly.io

The `Dockerfile` bundles Redpanda, the producer and the consumer into one container (`deploy/start.sh`), so no external Kafka is needed. Kafka data lives only inside the container and resets on every redeploy.

```bash
fly launch --no-deploy --copy-config   # pick a unique app name if "market-engine" is taken
fly secrets set FINNHUB_TOKEN=your_key  # no quotes around the key
fly deploy
```

`KAFKA_BROKERS` (default `localhost:19092`) and `PORT` (default `8090`) can be overridden in any environment.

# Order Processing System

A fault-tolerant, event-driven order processing backend built with **Erlang/OTP** and **Apache Kafka**.

Orders arrive over a REST API, are published to Kafka, and are processed by supervised consumer workers with retries, a dead-letter topic, and idempotency. The goal: **no order is ever lost or charged twice, even when workers crash mid-flight.**

There is no frontend. You drive it with `curl`, Postman, or a producer script.

> **Status:** all 9 phases are built, and `docker compose up` starts the whole system:
>
> - Orders flow end to end from `POST /orders` through Kafka into PostgreSQL.
> - A killed consumer recovers without losing orders.
> - Failed payments are retried with backoff before landing on the dead-letter topic.
> - A duplicate or redelivered order is never charged twice.
> - A contract test fails the build if the producer changes the event shape.
> - Prometheus metrics and JSON logs show what is happening.
>
> See [Roadmap](#roadmap).

---

## Contents

- [How it works](#how-it-works)
- [Tech stack](#tech-stack)
- [Prerequisites](#prerequisites)
- [Quick start](#quick-start)
- [Using the API](#using-the-api)
- [Running the tests](#running-the-tests)
- [Load test](#load-test)
- [Project layout](#project-layout)
- [Design principles](#design-principles)
- [Roadmap](#roadmap)
- [Troubleshooting](#troubleshooting)

---

## How it works

```mermaid
flowchart LR
    C[Client] -->|POST /orders| API[REST API<br/>cowboy]
    API -->|order.created<br/>keyed by order ID| O[(orders<br/>3 partitions)]
    O --> W[Consumer workers<br/>one per partition]
    W <-->|check ID, save result| S[(Mnesia / PostgreSQL)]
    W -->|success| D[(orders.completed)]
    W -->|retryable failure| R[(orders.retry)]
    R --> RM[Retry manager] -->|after backoff| O
    W -->|fatal or retries exhausted| DLQ[(orders.dlq)]
```

1. `POST /orders` validates the body and publishes an `order.created` event to the `orders` topic, keyed by order ID so one order always lands on the same partition.
2. A consumer worker reads the event and checks the idempotency store. An order it has already seen is skipped.
3. The worker validates the order and calls a simulated payment gateway.
4. **Success:** the result is saved, the ID is marked as processed, and the event goes to `orders.completed`.
5. **Retryable failure** (such as a gateway timeout): the event goes to `orders.retry` with backoff of 1 s, then 5 s, then 30 s.
6. **Fatal failure** (validation error or declined payment) or retries used up: the event goes to `orders.dlq`.
7. `GET /orders/:id` returns the order's current status at any time.

### Delivery guarantees

- **At-least-once delivery** from Kafka, with **manual offset commits** only after an order is saved, retried, or dead-lettered.
- **Idempotent processing** on top, so a redelivered message does not create a duplicate. The result is effectively exactly once, without Kafka transactions.
- **One worker process per partition**, so partitions are processed in parallel. If a worker crashes, the consumer group restarts under its supervisor and every partition resumes from its last committed offset. No order is lost, at the cost of a pause of a few seconds.
- **No worker ever sleeps on a failed order.** It hands the order to the retry topic and moves on, so one bad order cannot block a partition.

### Kafka topics

| Topic | Written by | Read by | Purpose |
| --- | --- | --- | --- |
| `orders` | REST API, retry manager | Consumer workers | New orders and orders whose backoff has expired |
| `orders.retry` | Consumer workers | Retry manager | Failed orders waiting for backoff |
| `orders.completed` | Consumer workers | Downstream services | Successful orders |
| `orders.dlq` | Consumer workers | Operators | Orders that failed fatally or used up their retries |

### Order event

Every topic carries this JSON shape:

```json
{
  "id": "o-1001",
  "customer_id": "c-42",
  "amount": 1500,
  "currency": "INR",
  "items": [{"sku": "SKU-1", "qty": 2}],
  "attempt": 0,
  "created_at": "2026-11-14T10:00:00Z"
}
```

`amount` is an integer in the smallest currency unit (paise, cents), **never a float**. The server sets `attempt` and `created_at`.

Messages on `orders.retry` also carry `not_before` (Unix milliseconds). Messages on `orders.dlq` also carry `reason` (for example `"timeout"`, `"declined"`, `"invalid:amount"`) and `failed_at`.

---

## Tech stack

| Area | Choice |
| --- | --- |
| Language and build | Erlang/OTP 28, `rebar3` |
| Kafka client | [`brod`](https://github.com/kafka4beam/brod) |
| HTTP | `cowboy` |
| JSON | `jsx` |
| Fast state | Mnesia (idempotency keys, the simulated gateway's memory; optionally order records) |
| System of record | PostgreSQL through `epgsql`: orders and their status history |
| Metrics | `prometheus.erl` (phase 9) |
| Tests | EUnit, PropEr, Common Test, `meck`, Pact |
| Infrastructure | Docker Compose: Kafka 3.9 (KRaft, no ZooKeeper), PostgreSQL 16, Prometheus, Redpanda Console (Kafka web UI) |
| Release | `relx` release in a slim Debian image; GitHub Actions CI |

---

## Prerequisites

You need only:

- **Docker** with Docker Compose v2 (Docker Desktop on Windows and macOS)
- **Git**, plus a POSIX shell for the helper scripts (Git Bash works on Windows)
- **`curl`** to try the API

You do **not** need Erlang or `rebar3` on your machine. All Erlang code builds and runs inside a Linux container, because `brod`'s CRC32C native extension (`crc32cer`) does not build on Windows. Your checkout is mounted into the container, so you edit files locally as usual.

---

## Quick start

```bash
git clone https://github.com/ShivamGupta322/order-processing
cd order-processing
docker compose up -d
```

That one command does everything:

- builds the release image
- starts Kafka and PostgreSQL
- creates the four topics (`kafka-init`, a one-shot service)
- starts the app release (`orders`) once its dependencies are healthy
- starts Prometheus
- starts Redpanda Console, a web UI for Kafka

The first build takes a few minutes. Then:

```bash
curl localhost:8080/health
# {"status":"ok"}
docker compose logs -f orders     # JSON log lines
```

Prometheus is at <http://localhost:9090> (try the query `orders_processed_total`).
The Kafka UI is at <http://localhost:8081>.

### Stopping

```bash
docker compose down               # add --profile dev if the dev container is running
```

`docker compose stop` sends SIGTERM. The app stops its HTTP listener first, lets each partition worker finish its in-flight order, commits offsets, then exits.

> Kafka has no data volume, so topics are lost when its container is removed. `kafka-init` recreates them on the next `up`.
>
> Orders (in the `pg_data` volume) and idempotency keys (in the `mnesia_data` volume) survive `down`. To wipe both, run `docker compose --profile dev down -v`. That also deletes the build cache volumes. Wipe them together: keeping the idempotency keys while losing the orders would make re-sent orders look like duplicates of orders that no longer exist.

### Development workflow

For working on the code, use the dev container instead of the release. It mounts your checkout and runs `rebar3 shell`:

```bash
docker compose up -d kafka postgres kafka-init     # infrastructure and topics only
docker compose run --rm --service-ports app rebar3 shell --sname order_processing
```

Do not run the dev container and the `orders` release at the same time. They share port 8080, the node name, and the Mnesia volume. `docker compose stop orders` first.

The `--sname` matters: Mnesia stores data on disk under the node name (`order_processing@app`), so the name must stay the same between runs. Build output and the package cache live in Docker volumes, so only the first run compiles everything.

---

## Using the API

| Method and path | Purpose | Responses |
| --- | --- | --- |
| `POST /orders` | Validate and publish an order | `202` with order ID; `400` invalid body; `413` body too large; `503` Kafka unavailable |
| `GET /orders/:id` | Current order status | `200` with order; `404` unknown or not yet processed |
| `GET /metrics` | Prometheus scrape endpoint | `200` |
| `GET /health` | Liveness check | `200` |

`POST /orders` returns **`202 Accepted`**, not `201 Created`, because processing happens asynchronously after the event is published.

### Place an order

```bash
curl -X POST localhost:8080/orders \
  -H 'content-type: application/json' \
  -d '{"id":"o-1001","customer_id":"c-42","amount":1500,"currency":"INR","items":[{"sku":"SKU-1","qty":2}]}'
# 202 {"id":"o-1001"}
```

### Check its status

The order is processed asynchronously, usually within a second or two:

```bash
curl localhost:8080/orders/o-1001
# 200 {"id":"o-1001","status":"completed","customer_id":"c-42","amount":1500,"currency":"INR",
#      "items":[{"sku":"SKU-1","qty":2}],"attempt":0,"created_at":"...","updated_at":1794685666388}
```

`status` is `completed`, or `failed` (payment declined, or retries used up). `updated_at` is Unix time in milliseconds. An order that has not been processed yet, or is waiting for a retry, returns `404`.

### Look at the database

Orders and their status history are in PostgreSQL:

```bash
MSYS_NO_PATHCONV=1 docker compose exec postgres psql -U orders -d orders \
  -c "SELECT id, status, amount, currency, items, attempt FROM orders ORDER BY updated_at DESC LIMIT 5;" \
  -c "SELECT order_id, status, attempt, changed_at FROM order_status_history ORDER BY id DESC LIMIT 5;"
```

### Switch the store

Storage is an OTP behaviour (`order_store`) with two implementations, selected in `config/sys.config`:

```erlang
{store_impl, order_store_pg},       % PostgreSQL, the default
%% {store_impl, order_store_mnesia}, % Mnesia only
```

Change the line and restart the app; no code changes. With Mnesia, the app starts no PostgreSQL connection. Both stores pass the same contract tests (`order_store_SUITE`).

### Validation rules

| Field | Rule |
| --- | --- |
| `id`, `customer_id` | Non-empty string |
| `amount` | Positive integer (floats are rejected) |
| `currency` | One of `INR`, `USD`, `EUR`, `GBP` |
| `items` | Non-empty list of `{"sku": non-empty string, "qty": positive integer}` |

Unknown fields are ignored. Errors name the first failing field:

```bash
curl -X POST localhost:8080/orders -H 'content-type: application/json' \
  -d '{"id":"o-1002","customer_id":"c-42","amount":-5,"currency":"INR","items":[{"sku":"SKU-1","qty":2}]}'
# 400 {"error":"invalid","field":"amount"}
```

### Watch messages arrive in Kafka

The easiest way is the web UI at <http://localhost:8081>. It is Redpanda Console, the tool formerly called Kowl.

- **Topics → `orders`** shows each message: key (the order ID), partition, offset and JSON value. Use **Start offset: Newest** and keep the tab open to see new orders arrive.
- **`orders.retry`** has messages with `not_before` and `attempt`. **`orders.dlq`** has messages with `reason`.
- **Consumer Groups → `order_consumers`** shows committed offsets and lag per partition. Lag going back to 0 means every order has been handled. `order_retry` is the retry consumer group.

From the command line instead:

```bash
# MSYS_NO_PATHCONV=1 stops Git Bash on Windows from rewriting /opt/... into a Windows path
MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.completed --from-beginning \
  --property print.key=true --property print.partition=true
```

Swap the topic for `orders` or `orders.dlq` to see those. To see the consumer group's committed offsets and lag:

```bash
MSYS_NO_PATHCONV=1 docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order_consumers
```

### Watch the metrics

```bash
curl -s localhost:8080/metrics | grep -E '^orders_|^order_processing'
```

| Metric | Type | Labels | Incremented when |
| --- | --- | --- | --- |
| `orders_received_total` | counter | none | `POST /orders` publishes an order |
| `orders_processed_total` | counter | `result`: completed, failed, duplicate | an order reaches an outcome |
| `orders_retried_total` | counter | `attempt` | an order goes to `orders.retry` |
| `orders_dlq_total` | counter | `reason` | a message goes to `orders.dlq` |
| `order_processing_duration_seconds` | histogram | none | a message from `orders` is handled |

Logs are one JSON object per line, with `order_id`, `partition`, and `offset` as fields:

```json
{"attempt":1,"delay_ms":1000,"event":"order_retry","level":"warning","mfa":"consumer_worker:retry_or_give_up/4","offset":166,"order_id":"o-rel-1","partition":0,"reason":"timeout","time":"2026-11-15T12:53:00.695074Z"}
```

### Run code in the live app

The demos below run Erlang inside the app. With the release running (`docker compose up`), use its `eval` command:

```bash
MSYS_NO_PATHCONV=1 docker compose exec orders bin/order_processing eval '<expression>.'
```

With the dev container, type the expression into its `rebar3 shell` instead.

### Crash a worker

```erlang
{ok, #{0 := Worker}} = consumer_worker:partition_workers(orders), exit(Worker, kill).
```

The consumer group restarts within a few seconds (`consumer_worker_started` in the logs), and orders posted before, during, and after the kill all reach `completed`. `e2e_SUITE` automates this with 30 orders.

### Watch a retry

The simulated gateway times out 10% of the time and declines 2% (`gateway_timeout_rate` and `gateway_decline_rate` in `config/sys.config`). A timeout is retried after 1 s, 5 s, then 30 s. After the third retry fails, the order is marked `failed` and sent to `orders.dlq`. A decline goes to `orders.dlq` at once.

To force it, make every charge time out (see [Run code in the live app](#run-code-in-the-live-app)):

```erlang
application:set_env(order_app, gateway_timeout_rate, 1.0).
```

Post an order. About 36 seconds later, `GET /orders/:id` shows `"status":"failed","attempt":3`, and the logs show `order_retry` with attempts 1, 2 and 3, then `order_dead_lettered` with `class: retries_exhausted`. Set the rate back to `0.1` afterwards.

A message that is not valid JSON goes to `orders.dlq` too, base64-encoded, so a bad message never blocks its partition.

### Send a duplicate

Post the same order twice. The second copy is skipped (`order_duplicate` in the logs), nothing extra is published, and the order is charged once. Once an order ID has a final outcome (`completed` or `failed`), it is never processed again. To retry after a decline, use a new ID.

The simulated gateway also treats the order ID as an idempotency key. Say the worker crashes after the charge but before it records the order. The message is redelivered, and the gateway returns the original receipt instead of charging again.

---

## Running the tests

Run every command inside the dev container, with Kafka and PostgreSQL up (`docker compose up -d kafka postgres kafka-init`):

```bash
docker compose run --rm app rebar3 compile
docker compose run --rm app rebar3 eunit --cover
docker compose run --rm app rebar3 ct --dir test/ct --suite order_consumer_SUITE,order_producer_SUITE --cover
docker compose run --rm app rebar3 ct --dir test/pact  # contract tests; regenerates pacts/
docker compose run --rm app rebar3 ct --dir test/ct --suite order_store_SUITE   # both stores; needs PostgreSQL
docker compose run --rm app rebar3 cover --verbose     # coverage summary
docker compose run --rm app rebar3 dialyzer

# End-to-end against real Kafka and PostgreSQL (not in the default run; skips itself
# if Kafka is down). Stop the release first: it would consume the same test orders.
docker compose stop orders
docker compose run --rm app rebar3 ct --dir test/ct --suite e2e_SUITE
```

**CI.** [`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs every push and pull request in the same `erlang:28.4.2` image, with a PostgreSQL service container. It runs compile, EUnit, the mocked Common Test suites, the store contract suite, the Pact suites, and Dialyzer. It also checks that the regenerated pact file matches the committed one. The end-to-end suite is left out, because it needs Kafka.

The end-to-end suite posts its orders to the real `orders` topic under its own consumer groups. When you next start the app, its `order_consumers` group processes those test orders too. That is harmless, but expect some extra `order_completed` and `order_retry` log lines.

The unit and Common Test suites do **not** need Kafka. The Kafka client, the consumer group, and the payment gateway are mocked at their edges, and Mnesia runs in RAM, so the suites run fast, give the same result every time, and leave no files behind.

| Layer | Tool | Covers | Location |
| --- | --- | --- | --- |
| Unit | EUnit, PropEr | Validation, JSON codec (with a property-based round trip), retry policy, processor outcomes and duplicates, Mnesia store, idempotency store, simulated gateway and its idempotency key | `test/unit/` |
| Integration | Common Test, `meck` | HTTP API, consumer outcomes, `GET /orders/:id`, retries (backoff, release after `not_before`, in-order commits, the full timeout-to-DLQ loop), consumer restart after a crash, duplicates charged once, gateway replay after a crash before marking | `test/ct/` |
| Store contract | Common Test | The same store cases against Mnesia and PostgreSQL, including 50 concurrent writers; PostgreSQL status history | `test/ct/order_store_SUITE.erl` |
| Contract | Pact v3 message pact, own verifier | Shape of the `order.created` event: the consumer's needs are written to `pacts/`, and the payload the API really publishes is checked against them | `test/pact/` |
| End-to-end | Common Test | Real Kafka and PostgreSQL: an order completes; 30 orders all complete despite a worker killed mid-batch; a failing order retries three times and lands on `orders.dlq`; an order posted twice completes once | `test/ct/e2e_SUITE.erl` |

**Current results:**
- EUnit: 158 tests pass, including 300 generated PropEr round-trip cases.
- Common Test: all 24 mocked cases pass (consumer suite 16, producer suite 8).
- Contract: all 6 Pact cases pass. A deliberate rename of `amount` to `total` in the producer makes them fail, as intended.
- Store contract: all 15 cases pass (Mnesia 7, PostgreSQL 8).
- End-to-end: all 4 cases pass against real Kafka and PostgreSQL.
- Dialyzer: clean.
- **Line coverage: 84%** (EUnit plus the mocked and store suites). The Kafka-facing code shows lower numbers, because those suites stub it and the end-to-end suite exercises it: `order_producer` 0%, `consumer_worker` 70%, `retry_sup` 55%.

---

## Load test

[`scripts/load_test.escript`](scripts/load_test.escript) posts N orders from many concurrent senders over kept-alive connections. It then waits until the app's own `orders_processed_total` metric shows every order processed. Run it inside the compose network, so it measures the app rather than Docker Desktop's port forwarding:

```bash
docker compose run --rm --no-deps app escript scripts/load_test.escript 5000 50 http://orders:8080
```

**Measured** against the `orders` release, with the gateway's failure rates set to 0, so random retry delays do not stretch the tail:

| Orders | Concurrency | API accepted | Processed end to end |
| --- | --- | --- | --- |
| 1,000 | 20 | 107 orders/s (9.3 s) | 100 orders/s (10.0 s) |
| 5,000 | 50 | 277 orders/s (18.0 s) | 110 orders/s (45.3 s) |

**Hardware:** AMD Ryzen 5 2500U laptop (4 cores, 8 threads, 6.9 GB RAM), Windows 10 Home. Docker Desktop on WSL2 had 8 CPUs and 3.3 GB. Kafka, PostgreSQL, the app, and the load generator all ran on that one machine.

**Reading the numbers:**

- Processing tops out around 110 orders/s. Each of the 3 partitions handles one order at a time. Every order does a gateway record and an idempotency mark in Mnesia, a PostgreSQL insert, and a synchronous publish to `orders.completed` before its offset is committed. More partitions would raise this.
- The API accepts faster than processing, and Kafka absorbs the backlog.
- `scripts/produce_orders.sh` (curl and xargs) is fine for demos, but on this machine it tops out near 60 requests/s from process start-up alone. Don't use it for throughput numbers.

---

## Project layout

```
.
├── .github/workflows/
│   └── ci.yml                CI: everything except the end-to-end suite
├── config/
│   ├── sys.config            app settings: ports, Kafka endpoints, timeouts, store, log format
│   ├── vm.args
│   └── prometheus.yml        Prometheus scrape config
├── docs/
│   └── IMPLEMENTATION_GUIDE.md   architecture, data shapes, semantics, milestones
├── pacts/
│   └── order_consumer-order_api_producer.json   the event contract (generated by the consumer pact suite)
├── include/
│   ├── order_topics.hrl      Kafka topic names, defined once
│   └── order_tables.hrl      Mnesia records and table names, defined once
├── priv/sql/
│   └── schema.sql            PostgreSQL schema, applied by the app at boot (idempotent)
├── scripts/
│   ├── create_topics.sh      creates the four topics by hand (safe to re-run)
│   ├── produce_orders.sh     posts N orders (demo)
│   └── load_test.escript     load test: API and end-to-end throughput
├── src/
│   ├── order_app.erl         application entry point
│   ├── order_sup.erl         top-level supervisor
│   ├── order_http_handler.erl   thin HTTP shell: POST /orders, GET /orders/:id, /health, /metrics
│   ├── order_metrics.erl     declares the Prometheus metrics
│   ├── order_log_formatter.erl  JSON log lines (OTP json module, no dependency)
│   ├── order_validator.erl   pure: field and amount checks
│   ├── order_codec.erl       pure: JSON encode and decode, DLQ messages
│   ├── order_producer.erl    the only module that publishes to Kafka
│   ├── consumer_sup.erl      supervises the brod consumer group
│   ├── consumer_worker.erl   thin Kafka shell: decode, process, publish outcome, commit
│   ├── order_processor.erl   business flow, returns retryable or fatal errors
│   ├── retry_policy.erl      pure: backoff schedule and dead-letter decision
│   ├── retry_sup.erl         supervises retry_manager and the retry consumer group
│   ├── retry_manager.erl     holds retries until not_before, republishes, commits in order
│   ├── payment_gateway.erl   simulated gateway: configurable failure rates, order ID as idempotency key
│   ├── order_store.erl       storage behaviour, implementation chosen in config
│   ├── order_store_mnesia.erl   Mnesia implementation
│   ├── order_store_pg.erl    PostgreSQL implementation, the system of record
│   ├── idempotency_store.erl order IDs with a final outcome (Mnesia, no process)
│   ├── order_mnesia.erl      creates the schema and tables at boot
│   └── order_clock.erl       the one place the app reads the clock
├── test/
│   ├── unit/                 EUnit and PropEr
│   ├── ct/                   Common Test suites, including the end-to-end suite
│   └── pact/                 contract suites and the Pact matcher
├── docker-compose.yml        Kafka, PostgreSQL, topic init, the release, Prometheus, Kafka UI, the dev container
├── Dockerfile                production image: relx release on debian:trixie-slim
├── Dockerfile.dev            dev image: Erlang 28 plus cmake (for brod's native extension)
└── rebar.config
```

### Supervision tree

```
order_app
└── order_sup (one_for_one)
    ├── producer_sup        brod client
    ├── order_pg            PostgreSQL connection (only when store_impl is order_store_pg)
    ├── consumer_sup        supervisor
    │     └── consumer_group    brod group subscriber, one worker per partition
    ├── retry_sup           supervisor (one_for_all)
    │     ├── retry_manager     holds retries until not_before
    │     └── retry_group       brod group subscriber on orders.retry
    └── http_sup            cowboy listener
```

Two pieces have no process of their own:

- **`idempotency_store`** is a plain module over a Mnesia table, so the lookup on every message is not serialised behind one process.
- **Metrics** live in `prometheus.erl`'s own registry, so there is no `metrics_server`.

Children start in the order shown and stop in reverse. On shutdown, HTTP stops first, then the consumers, then the producer they both depend on. Each partition worker finishes the order it is handling before it stops, and acked offsets are committed on the way out.

---

## Design principles

- **Pure core, thin shells.** Validation, encoding, and the retry policy are pure functions: data in, data out, with no processes, I/O, or clock reads. The HTTP handler and the consumer worker only translate between the outside world and that core.
- **One way to reach Kafka and one way to reach storage.** Only `order_producer` and `consumer_worker` call `brod`. All storage goes through the `order_store` behaviour, so Mnesia and PostgreSQL can be swapped through config.
- **Let it crash, on purpose.** No defensive `try/catch`. Failures are handled by supervisors, and only errors with a meaningful response are caught (for example, a Kafka error becomes `503`).
- **Errors are classified.** The processor returns `{error, retryable, _}` or `{error, fatal, _}`, so the caller chooses retry or DLQ without inspecting the reason.
- **No atoms from external input.** Only a fixed set of known JSON keys is converted to atoms, because atoms are never garbage collected.

For the full specification, see [`docs/IMPLEMENTATION_GUIDE.md`](docs/IMPLEMENTATION_GUIDE.md).

---

## Roadmap

| Phase | Build | Done when | Status |
| --- | --- | --- | --- |
| 1. Setup | `rebar3` app, Docker Compose, topics script | Kafka runs and the app boots | ✅ |
| 2. API and producer | cowboy handler, validator, codec, producer | `curl POST /orders` puts a message on `orders` | ✅ |
| 3. Basic consumer | `consumer_worker`, `order_processor`, Mnesia store | An order is consumed and `GET /orders/:id` shows `completed` | ✅ |
| 4. Supervision | `consumer_sup`, one worker per partition, manual commits | Killing a worker restarts it and no order is lost | ✅ |
| 5. Retry and DLQ | `retry_policy`, `retry_manager`, error classification | A failing order retries three times, then lands on `orders.dlq` | ✅ |
| 6. Idempotency | `idempotency_store`, gateway idempotency key | The same order sent twice is charged once | ✅ |
| 7. Contract tests | Pact file and suites | Renaming a field on the producer fails the build | ✅ |
| 8. PostgreSQL | `order_store_pg` behind the behaviour | Switching `store_impl` changes the store with no code change | ✅ |
| 9. Metrics and release | Prometheus, JSON logs, `relx` release, CI | `docker compose up` starts everything and CI is green | ✅ |

---

## Troubleshooting

**`curl` returns `503 {"error":"kafka_unavailable"}`.**
The app cannot reach Kafka, or the topics do not exist. Check `docker compose ps`; `kafka-init` should show `Exited (0)`. Rerun it with `docker compose up kafka-init`, or use `./scripts/create_topics.sh`.

**The app container exits right after starting.**
The app needs Kafka and, with the default store, PostgreSQL at boot: the `brod` client and the PostgreSQL connection both exit if they cannot connect. Make sure both show `(healthy)` in `docker compose ps`. The `orders` and `app` services wait for those health checks, but a restart of either afterwards can still cause it.

**`port is already allocated` for 8080 when starting `orders` or `app`.**
The release and the dev container both use port 8080. Stop one first: `docker compose stop orders`, or `docker compose stop app`.

**`create_topics.sh` fails with `no such file or directory: C:/Program Files/Git/opt/kafka/...`.**
Git Bash is rewriting the container path. The script sets `MSYS_NO_PATHCONV=1` itself; set it yourself when you run `docker compose exec` by hand.

**`GET /orders/:id` returns `404` right after a `202`.**
Processing is asynchronous, so wait a second and retry. If it stays `404`, check `docker compose logs app` for `order_dead_lettered`: an order that fails validation or times out is not stored, and goes to `orders.dlq` instead.

**The app fails at boot with a Mnesia error after you changed the node name.**
Disc tables belong to the node that created them. Start the shell with `--sname order_processing`, or wipe the data with `docker compose --profile dev down -v`.

**A port is already in use (8080, 9092, or 5432).**
Stop whatever holds the port, or change the host side of the port mapping in `docker-compose.yml`.

**Running `rebar3` directly on Windows (outside Docker).**
This is not supported. `brod`'s native extension will not build. If you have OTP 28 and `rebar3` installed anyway, set `TMPDIR` to a Windows path first: OTP 28 reports its architecture as `x86_64-pc-windows`, so `rebar3` takes its Unix temp-directory branch and builds a broken `\tmp` path.

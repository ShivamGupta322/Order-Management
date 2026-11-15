# Order Processing System: Implementation Guide

Nov 14, 2026

## Overview

This is a fault-tolerant, event-driven order processing backend in Erlang/OTP and Kafka. It accepts orders over REST, processes them through Kafka, and never loses or double-processes an order when workers crash.

It is a backend-only project. There is no frontend: you drive it with `curl`, Postman, or a producer script.

### Goals

- Accept orders through a REST API and publish them to Kafka.
- Process orders in parallel with one supervised worker per partition.
- Retry failed orders with backoff, then route them to a dead-letter topic.
- Process each order exactly once in effect, using idempotency on top of at-least-once delivery.
- Expose metrics and ship as a `rebar3` release that starts with `docker-compose up`.

### Out of scope

- A real payment gateway. Payment is simulated with random failures.
- Authentication, multi-tenancy, and a web UI.

### Tech stack

| Area | Choice |
| --- | --- |
| Language and build | Erlang/OTP, `rebar3`, `relx` release |
| Kafka client | `brod` |
| HTTP | `cowboy` |
| JSON | `jsx` |
| Fast state | Mnesia (idempotency keys, retry counters) |
| System of record | PostgreSQL via `epgsql`, added in a later phase |
| Metrics | `prometheus.erl`, optional Grafana |
| Tests | EUnit, PropEr, Common Test, `meck`, Pact |
| Infrastructure | Docker Compose: Kafka (KRaft), PostgreSQL, Prometheus |

## Architecture

An order enters through the REST API, travels through Kafka, and ends in exactly one of two places: completed, or the dead-letter topic.

```
                              Mnesia and PostgreSQL
                         (idempotency keys, order records)
                                       ^
                                       | check ID, save result
                                       v
REST API ------> orders ------> Consumer worker ---- success ----> orders.completed
POST /orders    (topic,        (one per partition)
                3 partitions)     |           |
                   ^              |           +-- exhausted or fatal --> orders.dlq
                   |              | retryable failure
             after backoff        v
                   |         orders.retry
             Retry manager <----- (topic)
```

The consumer worker is the only component that decides an order's outcome. The retry manager only delays a failed order and puts it back on `orders`.

### Processing steps

1. A client sends `POST /orders`. The API validates the body and publishes an `order.created` event to the `orders` topic, keyed by order ID.
2. A consumer worker reads the event and checks the idempotency store. An order ID seen before is skipped and its offset committed.
3. The worker validates the order and calls the simulated payment gateway.
4. On success it saves the result, marks the ID as processed, publishes to `orders.completed`, and commits the offset.
5. On a retryable failure it publishes to `orders.retry` with an incremented attempt count, then commits the offset.
6. After the last allowed attempt, or on a fatal error, it publishes to `orders.dlq` and marks the order failed.
7. `GET /orders/:id` returns the current status at any point.

### Kafka topics

| Topic | Written by | Read by | Purpose |
| --- | --- | --- | --- |
| `orders` | REST API | Consumer workers | New orders |
| `orders.retry` | Consumer workers | Retry manager | Failed orders waiting for backoff |
| `orders.completed` | Consumer workers | Downstream services | Successful orders |
| `orders.dlq` | Consumer workers | Operators | Orders that exhausted retries or failed fatally |

Use 3 partitions per topic for local development, and key every message by order ID so one order always lands on the same partition.

### Supervision tree

```
order_app
└── order_sup (one_for_one)
    ├── producer_sup        brod client, shared by producer and consumers
    ├── consumer_sup        supervisor
    │     └── consumer_group    brod_group_subscriber_v2
    │           ├── worker for partition 0   (consumer_worker callbacks)
    │           ├── worker for partition 1
    │           └── worker for partition 2
    ├── retry_sup           supervisor (one_for_all)
    │     ├── retry_manager     gen_server holding retries until not_before
    │     └── retry_group       brod_group_subscriber_v2 on orders.retry
    ├── http_sup            cowboy listener
    └── metrics_server
```

Children start top to bottom and stop in reverse. On shutdown, HTTP stops first, then the consumers, then the producer they depend on.

`idempotency_store` is not in the tree. It is a plain module over a Mnesia table and has no process: a gen_server would only serialise a lookup made on every message, and Mnesia already handles concurrent access.

Each partition has its own worker process, so partitions are processed in parallel and in order within each partition. The partition workers are owned by brod's group subscriber, not directly by `consumer_sup`.

When any partition worker crashes, the subscriber flushes the offsets already acked, stops its other workers, and exits. `consumer_sup` restarts it, the member rejoins the group, and **every** partition resumes from its last committed offset. No order is lost. The crashed partition's in-flight message is redelivered, and the other partitions pause for a few seconds during the rejoin. This was chosen over per-partition supervisors (which would need offsets kept outside Kafka) to keep Kafka consumer groups and their tooling. `consumer_worker:partition_workers/0` returns the worker pid for each partition.

### Modules

| Module | Type | Responsibility |
| --- | --- | --- |
| `order_app`, `order_sup` | application, supervisor | Boot and top-level supervision |
| `order_http_handler` | cowboy handler | `POST /orders`, `GET /orders/:id` |
| `order_validator` | pure | Field and amount checks |
| `order_codec` | pure | JSON encode and decode of events |
| `retry_policy` | pure | Backoff schedule and dead-letter decision |
| `order_producer` | wrapper over `brod` | Publish to any topic; swappable in tests |
| `consumer_worker` | `brod` subscriber callback | Thin shell: decode, call processor, commit or retry |
| `order_processor` | pure-ish | Business flow; returns `{ok, Result}` or `{error, retryable or fatal, Reason}` |
| `payment_gateway` | simulated | Random success, timeout, or decline |
| `retry_manager` | `gen_server` | Holds retry messages until their backoff expires |
| `order_store` | behaviour | Storage contract, with Mnesia and PostgreSQL implementations |
| `idempotency_store` | Mnesia | Processed order IDs |

Business logic stays out of `consumer_worker` and never calls `brod` directly. That separation is what makes the unit and contract tests cheap.

## Data and storage

Mnesia holds the fast, short-lived state and PostgreSQL holds the durable order records. Build with Mnesia alone through phase 6, then add PostgreSQL behind the same behaviour.

### Order event

Every topic carries the same JSON shape. This is also the shape the Pact contract pins down.

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

`amount` is an integer in the smallest currency unit (paise), never a float. `attempt` starts at 0 and increases on each retry.

The client sends `id`, `customer_id`, `amount`, `currency`, and `items`. The API sets `attempt` to 0 and `created_at` to the current UTC time, overwriting any client value. Accepted currencies are `INR`, `USD`, `EUR`, and `GBP`. Unknown JSON fields are dropped when the event is decoded.

### Which store holds what

| Data | Store | Why |
| --- | --- | --- |
| Processed order IDs (idempotency keys) | Mnesia | Looked up on every message, must be fast |
| Retry counters and in-flight state | Mnesia | Short-lived and tied to the node |
| Final order records and status history | PostgreSQL | Durable, queryable, the system of record |

### Mnesia tables

Records live in `include/order_tables.hrl`, the one place tables are named. `gateway_charge` is the simulated payment gateway's own memory of final charge results, keyed by order ID. It stands in for a real gateway's storage, and only `payment_gateway` touches it.

```erlang
-record(processed_order, {id :: binary(), processed_at :: integer()}).
-record(gateway_charge, {order_id :: binary(),
                         result :: {ok, map()} | {error, declined},
                         charged_at :: integer()}).
-record(order, {id :: binary(),
                status :: pending | processing | completed | failed,
                customer_id :: binary(),
                amount :: non_neg_integer(),
                currency :: binary(),
                items :: [map()],
                attempt = 0 :: non_neg_integer(),
                created_at :: binary() | undefined,
                updated_at :: integer()}).

init_tables() ->
    mnesia:create_schema([node()]),
    ok = mnesia:start(),
    mnesia:create_table(processed_order,
        [{attributes, record_info(fields, processed_order)},
         {disc_copies, [node()]}]),
    mnesia:create_table(order,
        [{attributes, record_info(fields, order)},
         {disc_copies, [node()]}]),
    ok = mnesia:wait_for_tables([processed_order, order], 5000).
```

`mnesia:create_schema/1` and `create_table/2` return an error tuple when the schema or table already exists, so match on both outcomes at boot instead of asserting `ok`.

The `order` record holds the full order, not just its status. That way `GET /orders/:id` can return `customer_id`, `currency`, and `items` before PostgreSQL arrives in phase 8.

`order_mnesia:init/0` does this setup. `order_app:start/2` calls it before starting the supervisor. Mnesia is not in the app's `applications` list: starting it before the disc schema exists would give a RAM-only schema.

- **Copy type:** `mnesia_copies` in app env selects it. It is `disc_copies` normally and `ram_copies` in tests, so tests leave no files behind.
- **Node name:** disc tables are tied to the node name. The app container uses `hostname: app` and `rebar3 shell --sname order_processing`, which gives the node `order_processing@app`.
- **Data location:** the Mnesia directory is `/var/lib/order_app/mnesia`, on the `mnesia_data` volume, so orders survive container restarts.

### PostgreSQL schema

```sql
CREATE TABLE orders (
  id          TEXT PRIMARY KEY,
  customer_id TEXT        NOT NULL,
  amount      BIGINT      NOT NULL CHECK (amount > 0),
  currency    TEXT        NOT NULL,
  status      TEXT        NOT NULL,
  attempt     INT         NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ NOT NULL,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Storage behaviour

Both stores implement one behaviour, selected through application env. This is what lets you say the storage layer is pluggable.

```erlang
-module(order_store).

-callback save(Order :: map()) -> ok | {error, term()}.
-callback get(Id :: binary()) -> {ok, map()} | {error, not_found}.
-callback update_status(Id :: binary(), Status :: atom()) -> ok | {error, term()}.

-export([save/1, get/1, update_status/2]).

impl() -> application:get_env(order_app, store_impl, order_store_mnesia).

save(Order)             -> (impl()):save(Order).
get(Id)                 -> (impl()):get(Id).
update_status(Id, S)    -> (impl()):update_status(Id, S).
```

### Consistency rule when both stores are used

Writing to two stores is not atomic, so a crash between the writes can leave them disagreeing. Handle it with a fixed order and one source of truth:

1. Write the final result to PostgreSQL first, using `INSERT ... ON CONFLICT (id) DO NOTHING`.
2. Mark the ID in Mnesia.
3. Commit the Kafka offset.

PostgreSQL is the source of truth and Mnesia is a fast cache in front of it. If the worker crashes after step 1, the message is redelivered, the insert is a no-op, and the worker carries on from step 2.

## Processing semantics

The system uses at-least-once delivery from Kafka plus idempotent processing, which gives exactly-once results without Kafka transactions.

### Offset commits

- Turn off auto-commit. Commit an offset only after the order is saved, retried, or dead-lettered.
- With `brod_group_subscriber_v2`, each partition gets its own worker process, and `handle_message/2` returns `{ok, commit, State}` once the message is fully handled. Confirm the callback shape against the `brod` version you install.
- A crash before the commit means the message is delivered again. Idempotency makes that safe.

### Retry and dead-letter

| Failed attempt | Next action | Delay |
| --- | --- | --- |
| 1st (attempt 0) | Retry | 1 s |
| 2nd (attempt 1) | Retry | 5 s |
| 3rd (attempt 2) | Retry | 30 s |
| 4th (attempt 3) | Send to `orders.dlq` | none |

```erlang
-module(retry_policy).
-export([next/1]).

-spec next(non_neg_integer()) -> {retry, pos_integer()} | dead_letter.
next(0) -> {retry, 1000};
next(1) -> {retry, 5000};
next(2) -> {retry, 30000};
next(_) -> dead_letter.
```

- The worker never sleeps on a failed order. It publishes the order to `orders.retry` with `attempt + 1` and a `not_before` timestamp, commits, and moves on, so one bad order cannot block a partition.
- `retry_manager` consumes `orders.retry`, waits until `not_before`, and republishes to `orders`. The simplest correct version handles one message at a time and commits after republishing.
- **How that is built.** `consumer_worker` runs a second consumer group, `order_retry`, on `orders.retry`. Its retry-role callback hands each message to `retry_manager:schedule/1` and does not commit. `retry_manager` is a gen_server with one FIFO queue per partition. It arms `erlang:send_after/3` for the head of each queue (it never sleeps). When the timer fires it publishes the order to `orders`, then calls the message's `commit_fun`.
- **Why one queue per partition.** Commits happen strictly in offset order, so a crash can never commit past a retry that was not yet republished. The cost is head-of-line blocking: a 1 s retry queued behind a 30 s one waits for it. That only ever lengthens a delay.
- **Supervision.** `retry_manager` holds the group's uncommitted messages in memory, so it and the retry group share a `one_for_all` supervisor, `retry_sup`. If either dies, both restart and Kafka redelivers whatever was not committed. A retry message that cannot be decoded is queued as a DLQ publish, so it too is committed in offset order.
- **Retry message shape.** A retry message is the order event plus `not_before`, in Unix milliseconds. `not_before` is stripped when the order goes back on `orders`.
- **Delays** come from `retry_delays_ms` (default `[1000, 5000, 30000]`), passed into `retry_policy:next/2`.
- **When retries run out,** `order_processor:fail/1` stores the order as `failed` before it is dead-lettered.
- **Bad `attempt` values.** `order_validator` rejects an `attempt` that is not a non-negative integer. Otherwise a malformed `attempt` would crash the retry decision on every redelivery and stall the partition.
- Errors are classified. A timeout is retryable. A validation failure or a declined payment is fatal and goes straight to the DLQ.
- A DLQ message carries the original order plus `reason`, `attempt`, and `failed_at`. `reason` is a string such as `"declined"`, `"timeout"`, or `"invalid:amount"`.
- A message that cannot be decoded has no order to carry. Its DLQ message is `{"raw_base64": ..., "reason": "malformed_json" | "not_an_object", "failed_at": ...}`. The raw bytes are base64-encoded because they may not be valid UTF-8, which JSON strings require. It is keyed by the original Kafka key.
- The simulated gateway's failure rates in `sys.config` are 0.1 for timeouts and 0.02 for declines. Set `gateway_timeout_rate` to 1.0 to watch an order retry three times and land on the DLQ.
- `consumer_worker` is a `brod_group_subscriber_v2` callback module in consumer group `order_consumers`. brod runs one callback process per assigned partition. It shares the `brod` client started by `order_producer`.

### Idempotency

```erlang
process(Order = #{id := Id}) ->
    case idempotency_store:seen(Id) of
        true  -> {ok, duplicate};
        false ->
            case payment_gateway:charge(Order) of
                {ok, Receipt} ->
                    ok = order_store:save(Order#{status => completed}),
                    ok = idempotency_store:mark(Id),
                    {ok, Receipt};
                {error, timeout}  -> {error, retryable, timeout};
                {error, declined} -> {error, fatal, declined}
            end
    end.
```

There is a real gap here to be honest about: a crash after `charge` succeeds but before `mark` leads to a second charge on redelivery. A real gateway closes it by accepting the order ID as an idempotency key. Make the simulated gateway do the same, and be ready to explain it in interviews.

How it is built:

- **Order of checks.** `order_processor` validates first, then checks `idempotency_store:seen/1`. An order without a usable ID never reaches the lookup.
- **Duplicates.** A seen order returns `{ok, duplicate}`. The worker logs `order_duplicate`, publishes nothing, and commits.
- **What counts as processed.** An ID is marked on any **final** outcome: `completed`, or `failed` (declined, or retries used up). A redelivered declined order is not given another roll of the dice. A timeout is not final and is not marked, so retries still run. Because failed IDs are marked too, a client that wants to try again after a decline must use a new order ID.
- **Write order.** The order is saved, then marked, then the offset is committed, matching the consistency rule above.
- **Gateway idempotency key.** `payment_gateway` keeps final results (success or decline) per order ID in its own `gateway_charge` table. Charging the same order again returns the stored result without a new charge. This closes the gap above. Timeouts are not stored, because a timed-out request never reached the gateway.
- **Duplicates while an order is still retrying.** Say a duplicate arrives while the original is waiting in `orders.retry`. Both copies may run, but the gateway charges at most once. The first copy to reach a final outcome marks the ID, and the other copy is skipped at its next attempt. Both copies may publish to `orders.completed`, so downstream consumers should also treat order ID as an idempotency key.
- **No expiry yet.** Marked IDs and gateway records are never deleted. A redelivery can only happen within Kafka's retention period (7 days by default), so a pruner could later remove older entries.

### Graceful shutdown

- The group subscriber traps exits. In `terminate/2` it stops each partition worker with a message queued behind the order being handled, so the in-flight order finishes, and then it flushes offset commits. `consumer_worker:terminate/2` logs `consumer_worker_stopped`.
- Child specs get a `shutdown` value (`consumer_shutdown_ms`, `producer_shutdown_ms`, both 10000 ms by default) instead of `brutal_kill`.
- On `SIGTERM` the VM stops the application in reverse start order: HTTP listener first, then consumers, then the producer.

## Project setup

The whole system starts with `docker-compose up` for infrastructure and `rebar3 shell` for the app.

The app and every `rebar3` command run inside a Linux container (the `app` service), not on the host. `brod` depends on `kafka_protocol`, which depends on the `crc32cer` NIF, and that NIF only builds on Linux and macOS; every maintained Erlang Kafka client has the same kind of native dependency. The project folder is bind-mounted into the container, so files are edited on the host as usual.

### Folder layout

```
order_processing/
├── AGENTS.md
├── CLAUDE.md
├── rebar.config
├── docker-compose.yml
├── Dockerfile.dev     dev image: erlang plus cmake for the crc32cer NIF
├── Makefile
├── docs/
│   └── IMPLEMENTATION_GUIDE.md
├── config/
│   ├── sys.config
│   └── vm.args
├── include/
│   └── order_topics.hrl   topic name macros, the one place topics are named
├── src/
│   ├── order_app.erl, order_sup.erl
│   ├── order_http_handler.erl
│   ├── order_validator.erl, order_codec.erl, retry_policy.erl
│   ├── order_producer.erl, consumer_sup.erl, consumer_worker.erl
│   ├── order_processor.erl, payment_gateway.erl, retry_manager.erl
│   ├── order_store.erl, order_store_mnesia.erl, order_store_pg.erl
│   └── idempotency_store.erl
├── test/
│   ├── unit/    retry_policy_tests.erl, order_codec_tests.erl, order_validator_tests.erl
│   ├── ct/      order_consumer_SUITE.erl, order_producer_SUITE.erl, e2e_SUITE.erl
│   └── pact/    consumer_pact_SUITE.erl, provider_verify_SUITE.erl
├── pacts/       order_consumer-order_producer.json
└── scripts/     produce_orders.sh, create_topics.sh
```

### rebar.config

```erlang
{erl_opts, [debug_info, warnings_as_errors]}.

{deps, [brod, cowboy, jsx, epgsql, prometheus]}.

{profiles, [
  {test, [{deps, [meck, proper]},
          {extra_src_dirs, ["test/unit", "test/ct", "test/pact"]}]}
]}.

{relx, [{release, {order_processing, "0.1.0"}, [order_app, sasl]},
        {sys_config, "config/sys.config"},
        {vm_args, "config/vm.args"}]}.

{dialyzer, [{warnings, [unmatched_returns, error_handling]}]}.
```

Dependencies without a version take the latest from hex.pm, and `rebar.lock` then pins them. Commit the lock file.

### docker-compose.yml

Three services, abbreviated here; see `docker-compose.yml` for the full file:

```yaml
services:
  kafka:
    image: apache/kafka:3.9.0    # KRaft mode, no ZooKeeper
    ports: ["9092:9092"]
    environment:
      # INTERNAL for containers (the app uses kafka:19092),
      # EXTERNAL for host tools (localhost:9092).
      KAFKA_LISTENERS: INTERNAL://:19092,EXTERNAL://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: INTERNAL://kafka:19092,EXTERNAL://localhost:9092
      # ...plus the KRaft settings for a single combined broker and controller
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: orders
      POSTGRES_PASSWORD: orders
      POSTGRES_DB: orders
    ports: ["5432:5432"]
  app:
    build: {context: ., dockerfile: Dockerfile.dev}
    profiles: ["dev"]            # `docker compose up -d` starts infrastructure only
    volumes: [".:/app", "app_build:/app/_build", "rebar3_cache:/root/.cache/rebar3"]
    ports: ["8080:8080"]
    depends_on: {kafka: {condition: service_healthy}}
```

Kafka needs two listeners because a broker tells clients which address to reconnect on. With only `localhost:9092`, a client in the app container would be sent to its own localhost. `_build` lives in a named volume so the Linux build never mixes with a host build.

Run the app with `docker compose run --rm --service-ports app rebar3 shell`, or in the background with `docker compose --profile dev up -d app`. `brod_client` exits at start if no broker is reachable, which is why the app waits for Kafka's health check.

Create each topic once after Kafka is up:

```
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create --topic orders --partitions 3
```

Repeat for `orders.retry`, `orders.completed`, and `orders.dlq`, and put the four commands in `scripts/create_topics.sh`.

### REST API

| Method and path | Purpose | Responses |
| --- | --- | --- |
| `POST /orders` | Validate and publish an order | `202` with order ID, `400` on invalid body |
| `GET /orders/:id` | Current status | `200` with order, `404` if unknown |
| `GET /metrics` | Prometheus scrape endpoint | `200` |
| `GET /health` | Liveness check | `200` |

`POST /orders` returns `202 Accepted`, not `201`, because processing happens asynchronously after the event is published.

```
curl -X POST localhost:8080/orders -H 'content-type: application/json' \
  -d '{"id":"o-1001","customer_id":"c-42","amount":1500,"currency":"INR","items":[{"sku":"SKU-1","qty":2}]}'
```

## Testing

Three layers cover three different risks: unit tests for logic, Common Test for failure behaviour, and Pact for the message contract between producer and consumer.

| Layer | Tool | Covers | Needs Kafka |
| --- | --- | --- | --- |
| Unit | EUnit, PropEr | Validation, backoff, JSON codec, idempotency decisions | No |
| Integration | Common Test, `meck` | Supervision, retry to DLQ, duplicates, shutdown | Mocked, plus one real end-to-end suite |
| Contract | Pact message pact | Shape of the `order.created` event | No |

### Unit tests (EUnit)

```erlang
-module(retry_policy_tests).
-include_lib("eunit/include/eunit.hrl").

backoff_test_() ->
    [?_assertEqual({retry, 1000},  retry_policy:next(0)),
     ?_assertEqual({retry, 5000},  retry_policy:next(1)),
     ?_assertEqual({retry, 30000}, retry_policy:next(2)),
     ?_assertEqual(dead_letter,    retry_policy:next(3))].

codec_roundtrip_test() ->
    Order = #{id => <<"o-1">>, amount => 1500, currency => <<"INR">>},
    ?assertEqual({ok, Order}, order_codec:decode(order_codec:encode(Order))).

invalid_amount_test() ->
    ?assertMatch({error, {invalid, amount}},
                 order_validator:validate(#{id => <<"o-2">>, amount => -5})).
```

Unit test checklist:

- [ ] `order_validator`: missing fields, negative or zero amount, unknown currency, empty items
- [ ] `order_codec`: round trip, malformed JSON, unknown extra fields ignored
- [ ] `retry_policy`: every attempt number, including past the limit
- [ ] `order_processor`: success, retryable error, fatal error, duplicate, with `payment_gateway` mocked
- [ ] PropEr property: `decode(encode(Order))` returns `{ok, Order}` for any generated order

### Common Test: consumer suite

```erlang
-module(order_consumer_SUITE).
-include_lib("common_test/include/ct.hrl").
-compile(export_all).

all() -> [worker_restarts_after_crash,
          failed_order_goes_to_dlq,
          duplicate_order_processed_once].

init_per_suite(Config) ->
    ok = meck:new(order_producer, [passthrough, no_link]),
    ok = meck:new(payment_gateway, [passthrough, no_link]),
    ok = meck:expect(order_producer, publish, fun(_Topic, _Key, _Value) -> ok end),
    {ok, _} = application:ensure_all_started(order_app),
    Config.

end_per_suite(_Config) ->
    application:stop(order_app),
    meck:unload().

worker_restarts_after_crash(_Config) ->
    [{_, Pid, _, _} | _] = supervisor:which_children(consumer_sup),
    Ref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, _, _, _} -> ok after 2000 -> ct:fail(no_down) end,
    ok = wait_until(fun() ->
            [{_, New, _, _} | _] = supervisor:which_children(consumer_sup),
            is_pid(New) andalso New =/= Pid
         end, 5000).

failed_order_goes_to_dlq(_Config) ->
    meck:expect(payment_gateway, charge, fun(_) -> {error, declined} end),
    order_test_helper:feed(#{id => <<"bad-1">>, amount => 100}),
    ok = wait_until(fun() ->
            meck:called(order_producer, publish, [<<"orders.dlq">>, '_', '_'])
         end, 5000).

duplicate_order_processed_once(_Config) ->
    meck:reset(payment_gateway),
    meck:expect(payment_gateway, charge, fun(_) -> {ok, receipt} end),
    Order = #{id => <<"dup-1">>, amount => 100},
    order_test_helper:feed(Order),
    order_test_helper:feed(Order),
    ok = wait_until(fun() -> meck:num_calls(payment_gateway, charge, '_') >= 1 end, 5000),
    timer:sleep(300),
    1 = meck:num_calls(payment_gateway, charge, '_').

wait_until(Fun, Timeout) when Timeout =< 0 ->
    case Fun() of true -> ok; false -> ct:fail(timeout) end;
wait_until(Fun, Timeout) ->
    case Fun() of
        true  -> ok;
        false -> timer:sleep(50), wait_until(Fun, Timeout - 50)
    end.
```

`order_test_helper:feed/1` hands an encoded message straight to the worker callback, so these suites run without a broker.

The guide's sample code uses `timer:sleep` (once in the duplicate case, and inside `wait_until/2` as its 50 ms poll interval). The real suites follow `CLAUDE.md`: they poll with `wait_until/2` and never sleep to wait for an outcome.

`order_test_helper:start_app/0` is shared by both suites. It starts `order_app` with Mnesia in RAM and stubs the edges: `order_producer`, `consumer_worker:start_link/0` (everything else in that module passes through to the real code), and `payment_gateway`. `brod_client` exits at init without a reachable broker, which is why its start is stubbed.

### Common Test: producer suite

- [ ] `POST /orders` with a valid body publishes one message to `orders`, keyed by order ID
- [ ] The published payload decodes back to the submitted order
- [ ] An invalid body returns `400` and publishes nothing
- [ ] A `brod` produce error surfaces as `503`, not a crash

### Common Test: end-to-end suite

One suite runs against the real Kafka from Docker Compose and is kept out of the default run:

- [x] Post an order, then poll `GET /orders/:id` until it reads `completed`
- [x] Kill a worker mid-run and assert every posted order still completes
- [x] Force failures and assert the order appears on `orders.dlq`

`e2e_SUITE` skips itself if Kafka is unreachable. Each run uses a fresh consumer group starting at `latest`, so history is not replayed. `latest` only applies to a partition with no committed offset, and a restart before a partition's first commit would skip messages that arrived meanwhile. So the suite first sends one order to each partition and waits until the group has committed on all three.

### Pact contract tests

The contract is the Kafka message. The consumer side is `consumer_worker` and the provider side is the API's producer.

```json
{
  "consumer": {"name": "order_consumer"},
  "provider": {"name": "order_api_producer"},
  "messages": [{
    "description": "an order created event",
    "contents": {
      "id": "o-1", "customer_id": "c-42", "amount": 1500,
      "currency": "INR", "attempt": 0,
      "created_at": "2026-11-14T10:00:00Z"
    },
    "matchingRules": {"body": {
      "$.id":     {"matchers": [{"match": "type"}]},
      "$.amount": {"matchers": [{"match": "integer"}]}
    }}
  }],
  "metadata": {"pactSpecification": {"version": "3.0.0"}}
}
```

| Test | Side | What it proves |
| --- | --- | --- |
| `consumer_pact_SUITE` | Consumer | Each contract message decodes and is processed by `order_processor` without error; writes the pact file |
| `provider_verify_SUITE` | Producer | `order_codec:encode/1` output for a real order satisfies every field and matching rule in the pact file |

Erlang's Pact tooling is less mature than Java's or JavaScript's. Two ways to implement it:

- **Option A, Pact library:** try the community `pact_erlang` package on hex.pm. Check that it is maintained and supports message pacts before depending on it.
- **Option B, own verifier:** keep the pact file in the standard format above and write the two CT suites yourself with a small matcher module that handles `type` and `integer` rules. This is about 100 lines and has no external dependency.

Either way the result is the same: renaming `amount` to `total` on the producer fails the build before it breaks the consumer.

### Running the tests

| Command | Runs |
| --- | --- |
| `rebar3 eunit --cover` | Unit tests |
| `rebar3 proper` | Property tests (needs the `rebar3_proper` plugin) |
| `rebar3 ct --dir test/ct --suite order_consumer_SUITE,order_producer_SUITE --cover` | Mocked integration suites |
| `rebar3 ct --dir test/pact` | Contract tests |
| `rebar3 ct --dir test/ct --suite e2e_SUITE` | End-to-end, with Docker Compose running |
| `rebar3 dialyzer` | Type checks |

Run each command inside the app container: `docker compose run --rm app <command>`.

A GitHub Actions workflow runs everything except the end-to-end suite on every push.

## Observability

Five metrics and structured logs are enough to show orders flowing, retrying, and failing in a demo.

| Metric | Type | Labels |
| --- | --- | --- |
| `orders_received_total` | counter | none |
| `orders_processed_total` | counter | `result`: completed, failed, duplicate |
| `orders_retried_total` | counter | `attempt` |
| `orders_dlq_total` | counter | `reason` |
| `order_processing_duration_seconds` | histogram | none |

- Use OTP's `logger` with a JSON formatter, and put `order_id`, `partition`, and `offset` in the metadata of every log line.
- Log at `warning` for a retry and `error` for a dead-lettered order.
- A Grafana dashboard is optional. Three panels cover it: throughput, retry rate, DLQ count.

## Milestones

Nine phases, roughly 4 to 6 weeks part-time. Each phase ends with something you can demonstrate.

| Phase | Build | Done when | Tests added |
| --- | --- | --- | --- |
| 1. Setup | `rebar3` app, Docker Compose, topics script | Kafka runs and the app boots in `rebar3 shell` | none |
| 2. API and producer | `cowboy` handler, validator, codec, `order_producer` | `curl POST /orders` puts a message on `orders` | EUnit for validator and codec; producer suite |
| 3. Basic consumer | `consumer_worker`, `order_processor`, Mnesia store | An order is consumed and `GET /orders/:id` shows `completed` | EUnit for processor |
| 4. Supervision | `consumer_sup`, one worker per partition, manual commits | Killing a worker restarts it and no order is lost | CT crash-recovery case |
| 5. Retry and DLQ | `retry_policy`, `retry_manager`, error classification | A failing order retries three times, then lands on `orders.dlq` | EUnit for policy; CT DLQ case |
| 6. Idempotency | `idempotency_store`, gateway idempotency key | The same order sent twice is charged once | CT duplicate case |
| 7. Contract tests | Pact file, consumer and provider suites | Renaming a field on the producer fails the build | Pact suites |
| 8. PostgreSQL | `order_store_pg` behind the behaviour | Switching `store_impl` in config changes the store with no code change | Store tests run against both implementations |
| 9. Metrics and release | Prometheus metrics, JSON logs, `relx` release, CI, README | `docker-compose up` starts everything and CI is green | End-to-end suite; load test |

Write each phase's tests in the same phase. Leaving all testing to the end is the most common way these projects stall.

## Deliverables

The finished project is a public GitHub repository that someone can clone, start with one command, and watch recover from a crash.

### Repository checklist

- [ ] README with a one-paragraph summary, architecture diagram, and quick start
- [ ] `docker-compose up` plus one command starts the full system
- [ ] Typespecs on exported functions and a clean `rebar3 dialyzer` run
- [ ] Coverage report from `rebar3 cover`, with the number in the README
- [ ] CI workflow running EUnit, Common Test, Pact, and Dialyzer
- [ ] Load test result measured on your own machine, stated with the hardware used

### Demo script

Record these three scenarios as a short video or GIF for the README:

1. **Happy path:** post 100 orders with the producer script and show them completing in the metrics.
2. **Crash recovery:** kill a consumer worker from the shell with `exit(Pid, kill)` while orders flow, and show that every order still completes.
3. **Retry to DLQ:** switch the gateway to always time out, post one order, and show three retries followed by a message on `orders.dlq`.

### Resume bullets

- Built a fault-tolerant order processing system in Erlang/OTP and Kafka (`brod`), with supervised per-partition consumers, manual offset commits, and at-least-once delivery.
- Implemented retry with exponential backoff, a dead-letter topic, and idempotent processing so that duplicate deliveries are handled exactly once in effect.
- Designed a pluggable storage layer as an OTP behaviour with Mnesia and PostgreSQL implementations.
- Wrote EUnit and PropEr unit tests, Common Test suites for crash recovery, DLQ, and duplicates, and consumer-driven Pact message contracts, all running in CI.

Add your measured throughput and coverage figures once you have them. Do not put numbers on the resume that you have not measured.

### Interview questions to prepare

- What happens if a consumer crashes in the middle of processing an order?
- How do you avoid processing the same message twice?
- At-least-once or exactly-once: which did you choose and why?
- Why one process per partition?
- Why both Mnesia and PostgreSQL, and what happens if they disagree?
- What does the Pact test catch that the Common Test suites do not?

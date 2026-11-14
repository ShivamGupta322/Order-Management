# AGENTS.md

Instructions for any AI coding agent working in this repository.

## Read this first

Before writing or changing any code, read the implementation guide in full:

```
docs/IMPLEMENTATION_GUIDE.md
```

It is the source of truth for architecture, data shapes, processing semantics, testing, and milestones. If the guide and the code disagree, stop and ask which one is correct. Do not silently pick one.

Then read `CLAUDE.md` for the coding rules and the pre-code checklist.

## What this project is

A fault-tolerant, event-driven order processing backend in Erlang/OTP and Kafka. Orders arrive over REST, flow through Kafka, and are processed by supervised consumer workers with retries, a dead-letter topic, and idempotency. Backend only: there is no frontend.

## Where to find things in the guide

| You need to know | Guide section |
| --- | --- |
| Goals, non-goals, tech stack | Overview |
| Flow, topics, supervision tree, module list | Architecture |
| Event JSON shape, Mnesia tables, PostgreSQL schema, `order_store` behaviour | Data and storage |
| Offset commits, retry schedule, DLQ, idempotency, shutdown | Processing semantics |
| Folder layout, `rebar.config`, Docker Compose, REST API | Project setup |
| EUnit, Common Test, Pact | Testing |
| Metric names and logging | Observability |
| Build order and done-when criteria | Milestones |

## Facts you must not get wrong

- Topics: `orders`, `orders.retry`, `orders.completed`, `orders.dlq`. Messages are keyed by order ID.
- Delivery is at-least-once. Offsets are committed manually, only after an order is saved, retried, or dead-lettered.
- Retry schedule: 1 s, 5 s, 30 s, then `orders.dlq`. Timeouts are retryable. Validation failures and declined payments are fatal.
- A worker never sleeps on a failed order. It publishes to `orders.retry` and moves on.
- `amount` is an integer in the smallest currency unit. Never a float.
- All storage goes through the `order_store` behaviour. Mnesia is the default; PostgreSQL arrives in phase 8.
- When both stores are used, write PostgreSQL first, then Mnesia, then commit the offset.
- `consumer_worker` is a thin shell. Business logic lives in `order_processor` and never calls `brod` directly.

## How to work

1. Find the current milestone phase in the guide. Work on that phase only.
2. State which modules you will touch and why before editing.
3. Write or update the tests for the phase in the same change.
4. Run the checks below and report the actual output.
5. If a task needs something the guide does not cover, propose the change to the guide first.

## Commands

All `rebar3` commands run inside the `app` container (`brod`'s NIF does not build on Windows). Prefix each one with `docker compose run --rm app`.

```
docker compose up -d                 # Kafka and PostgreSQL
./scripts/create_topics.sh           # once, after Kafka is up (Git Bash on Windows)
docker compose run --rm --service-ports app rebar3 shell --sname order_processing   # run the app on :8080
docker compose run --rm app rebar3 compile
docker compose run --rm app rebar3 eunit --cover           # unit tests
docker compose run --rm app rebar3 ct --dir test/ct --suite order_consumer_SUITE,order_producer_SUITE
docker compose run --rm app rebar3 ct --dir test/pact      # contract tests
docker compose run --rm app rebar3 dialyzer
```

## Boundaries

- Do not add a dependency without asking.
- Do not change the event JSON shape without updating the Pact file and both Pact suites in the same change.
- Do not rename topics, metrics, or public module names.
- Do not edit `rebar.lock` by hand.
- Do not commit secrets, and do not run destructive Docker or database commands without asking.
- Do not report tests as passing unless you ran them in this session.

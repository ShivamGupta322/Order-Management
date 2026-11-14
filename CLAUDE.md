# CLAUDE.md

Rules for writing code in this repository. Read `AGENTS.md` and `docs/IMPLEMENTATION_GUIDE.md` first; this file covers how to write the code, not what the system does.

## Before writing any code

Go through this list every time. If any answer is "no" or "not sure", stop and resolve it first.

- [ ] I have read the guide section for the module I am about to touch.
- [ ] I know which milestone phase this work belongs to, and earlier phases are done.
- [ ] I have read the existing module and its tests, not just the function I am changing.
- [ ] I can say in one sentence what the change does and how I will prove it works.
- [ ] I know which test file the new tests go in (`test/unit`, `test/ct`, or `test/pact`).
- [ ] The change does not alter the event JSON shape, topic names, or the `order_store` callbacks. If it does, I have said so explicitly and included the contract updates.
- [ ] I am not adding a dependency. If I need one, I have asked.

## Design rules

- **Keep logic pure.** `order_validator`, `order_codec`, and `retry_policy` take data and return data. No processes, no I/O, no `brod`, no clock reads inside them.
- **Thin shells at the edges.** `consumer_worker` and `order_http_handler` decode, call into `order_processor`, and translate the result. No business rules in them.
- **One way to reach Kafka.** Only `order_producer` and `consumer_worker` call `brod`. Everything else goes through them, so tests can swap them out.
- **One way to reach storage.** Call `order_store` and `idempotency_store`. Never call `mnesia` or `epgsql` from business code.
- **Return tagged tuples.** `{ok, Value}` or `{error, Reason}`. The processor returns `{error, retryable, Reason}` or `{error, fatal, Reason}` so the caller can choose retry or DLQ without inspecting the reason.
- **No magic numbers.** Timeouts, retry delays, partition counts, and ports come from `config/sys.config` via `application:get_env/3` with a default.

## OTP rules

- **Let it crash, on purpose.** Do not wrap code in `try ... catch` to hide failures. Catch only errors you can handle meaningfully, such as a gateway timeout that becomes `{error, retryable, timeout}`.
- **Assert what you expect.** Write `ok = order_store:save(Order)` so an unexpected result crashes right there, not three calls later.
- **Every process has a supervisor.** No bare `spawn`. Use `gen_server` under a supervisor, or `proc_lib` if there is a real reason.
- **Choose restart strategy and intensity deliberately**, and write a one-line comment in the supervisor saying why.
- **Never block a `gen_server` loop.** No `timer:sleep` and no long synchronous calls in `handle_call`. Use `erlang:send_after/3` for delays.
- **Give every `gen_server:call` an explicit timeout** and handle the timeout case.
- **Graceful shutdown:** workers trap exits, finish the in-flight order in `terminate/2`, and have a `shutdown` value in their child spec. No `brutal_kill`.
- **Name ETS and Mnesia tables in one place** and wait for tables at boot with `mnesia:wait_for_tables/2`.

## Kafka rules

- Commit an offset only after the message is fully handled: saved, published to retry, or published to the DLQ.
- Every handler must be safe to run twice on the same message. Check `idempotency_store` first.
- Always key messages by order ID.
- A poison message must never stall a partition. If it cannot be decoded, send it to `orders.dlq` with the reason and commit.
- Never sleep in a consumer to implement backoff. Publish to `orders.retry` with `not_before`.

## Erlang style

- Binaries for strings from outside (`<<"o-1">>`), atoms only for a fixed, known set. Never call `list_to_atom/1` or `binary_to_atom/2` on external input; atoms are not garbage collected.
- Maps for orders and events; records only for Mnesia rows.
- Pattern match in function heads instead of nested `case`.
- Functions stay short. If a function needs a comment to explain its middle, split it.
- `-spec` on every exported function. `-type` for shared shapes such as `order()` and `status()`.
- Export only what other modules use. No `-compile(export_all)` outside test suites.
- Module names are prefixed and descriptive: `order_*`, `consumer_*`, `retry_*`.
- Code must compile with `warnings_as_errors` and pass `rebar3 dialyzer` with no new warnings.
- Comments explain why, not what.

## Logging and metrics

- Use `logger` macros (`?LOG_INFO`, `?LOG_WARNING`, `?LOG_ERROR`) with a map, not format strings: `?LOG_WARNING(#{event => order_retry, order_id => Id, attempt => N})`.
- Always include `order_id`; include `partition` and `offset` in consumer logs.
- `warning` for a retry, `error` for a dead-lettered order. Do not log at `error` for things that are expected.
- Never log full payloads or anything that looks like personal or payment data.
- Use the metric names from the guide exactly. Increment them where the event happens, not in a wrapper far away.

## Testing rules

- New code comes with tests in the same change. Tests are not a later phase.
- Pure modules get EUnit tests for the happy path, each error path, and the boundaries.
- Anything involving processes, supervision, retries, or duplicates gets a Common Test case.
- Test behaviour through public functions. Do not export internals just to test them.
- No bare `timer:sleep` to wait for something. Poll with `wait_until/2`.
- Mock only at the edges: `order_producer`, `payment_gateway`, the clock. Always `meck:unload` in `end_per_suite` or `end_per_testcase`.
- Each test sets up and cleans its own state. Tests must pass in any order.
- A bug fix starts with a failing test that reproduces the bug.
- Changing the event shape means updating `pacts/`, `consumer_pact_SUITE`, and `provider_verify_SUITE` together.

## Before saying the work is done

Run these and report the real output, including failures:

```
rebar3 compile
rebar3 eunit --cover
rebar3 ct --dir test/ct --suite order_consumer_SUITE,order_producer_SUITE
rebar3 ct --dir test/pact
rebar3 dialyzer
```

Then confirm:

- [ ] All of the above pass, with no new warnings.
- [ ] New exported functions have specs.
- [ ] No debug output (`io:format`, `erlang:display`) is left in `src/`.
- [ ] No unrelated files or reformatting are included in the change.
- [ ] The guide is still accurate. If behaviour changed, the guide is updated in the same change.
- [ ] If a check could not be run (for example, Docker is not available for the end-to-end suite), say so plainly instead of claiming it passed.

## Commits

- One logical change per commit, with tests included.
- Message format: a short imperative subject, then why the change was needed.
- Never commit `_build/`, `Mnesia.*` directories, `.env` files, or credentials.

## When unsure

Ask. A short question costs less than code built on a wrong guess, especially around delivery guarantees, retry behaviour, and the event contract.

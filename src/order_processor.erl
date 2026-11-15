-module(order_processor).

%% Business flow for one order. Errors come back classified as retryable or
%% fatal, so the caller can choose retry or dead-letter without inspecting
%% the reason.
%%
%% An order that already reached a final outcome comes back as
%% {ok, duplicate} without being charged. The order is saved before its ID is
%% marked, and the caller commits only after both: a crash in between means
%% redelivery, and the gateway's idempotency key stops a second charge.

-export([process/1, fail/1]).
-export_type([result/0]).

-type result() :: {ok, payment_gateway:receipt() | duplicate}
                | {error, retryable | fatal, term()}.

-spec process(map()) -> result().
process(Order) ->
    dedupe(order_validator:validate(Order)).

%% Validation runs first so an order without a usable ID never reaches the
%% idempotency lookup.
dedupe({ok, #{id := Id} = Order}) ->
    charge(idempotency_store:seen(Id), Order);
dedupe({error, Reason}) ->
    {error, fatal, Reason}.

charge(true, _Order) ->
    {ok, duplicate};
charge(false, Order) ->
    settle(payment_gateway:charge(Order), Order).

settle({ok, Receipt}, #{id := Id} = Order) ->
    ok = order_store:save(Order#{status => completed}),
    ok = idempotency_store:mark(Id),
    {ok, Receipt};
settle({error, timeout}, _Order) ->
    {error, retryable, timeout};
settle({error, declined}, Order) ->
    ok = fail(Order),
    {error, fatal, declined}.

%% Records a validated order as failed, a final outcome, e.g. when it is
%% declined or its retries are used up.
-spec fail(order_codec:order()) -> ok.
fail(#{id := Id} = Order) ->
    ok = order_store:save(Order#{status => failed}),
    ok = idempotency_store:mark(Id).

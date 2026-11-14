-module(order_processor).

%% Business flow for one order. Errors come back classified as retryable or
%% fatal, so the caller can choose retry or dead-letter without inspecting
%% the reason.

-export([process/1, fail/1]).
-export_type([result/0]).

-type result() :: {ok, payment_gateway:receipt()}
                | {error, retryable | fatal, term()}.

-spec process(map()) -> result().
process(Order) ->
    charge(order_validator:validate(Order)).

charge({ok, Order}) ->
    settle(payment_gateway:charge(Order), Order);
charge({error, Reason}) ->
    {error, fatal, Reason}.

settle({ok, Receipt}, Order) ->
    ok = order_store:save(Order#{status => completed}),
    {ok, Receipt};
settle({error, timeout}, _Order) ->
    {error, retryable, timeout};
settle({error, declined}, Order) ->
    ok = fail(Order),
    {error, fatal, declined}.

%% Records a validated order as failed, e.g. when its retries are used up.
-spec fail(order_codec:order()) -> ok.
fail(Order) ->
    ok = order_store:save(Order#{status => failed}).

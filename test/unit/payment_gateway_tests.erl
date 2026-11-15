-module(payment_gateway_tests).
-include_lib("eunit/include/eunit.hrl").

%% The gateway's idempotency memory is a Mnesia table, run in RAM here.

gateway_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun no_failures_always_succeeds/1,
      fun full_timeout_rate_always_times_out/1,
      fun full_decline_rate_always_declines/1,
      fun success_is_replayed_not_recharged/1,
      fun decline_is_replayed/1,
      fun timeout_is_not_remembered/1]}.

setup() ->
    ok = application:set_env(order_app, mnesia_copies, ram_copies),
    ok = order_mnesia:init().

cleanup(_) ->
    ok = application:unset_env(order_app, gateway_timeout_rate),
    ok = application:unset_env(order_app, gateway_decline_rate),
    stopped = mnesia:stop(),
    application:unset_env(order_app, mnesia_copies).

order(N) -> #{id => <<"o-", (integer_to_binary(N))/binary>>}.

%% Distinct orders, so every charge is a fresh roll rather than a replay.
charge_many() -> [payment_gateway:charge(order(N)) || N <- lists:seq(1, 50)].

rates(Timeout, Decline) ->
    ok = application:set_env(order_app, gateway_timeout_rate, Timeout),
    ok = application:set_env(order_app, gateway_decline_rate, Decline).

no_failures_always_succeeds(_) ->
    rates(0.0, 0.0),
    Results = charge_many(),
    [?_assertMatch([{ok, #{order_id := <<"o-1">>, receipt_id := <<_/binary>>}} | _], Results),
     ?_assert(lists:all(fun({ok, _}) -> true; (_) -> false end, Results))].

full_timeout_rate_always_times_out(_) ->
    rates(1.0, 0.0),
    ?_assertEqual(lists:duplicate(50, {error, timeout}), charge_many()).

full_decline_rate_always_declines(_) ->
    rates(0.0, 1.0),
    ?_assertEqual(lists:duplicate(50, {error, declined}), charge_many()).

%% The idempotency key: a second charge of the same order returns the first
%% receipt, even when the gateway would now time out.
success_is_replayed_not_recharged(_) ->
    rates(0.0, 0.0),
    {ok, Receipt} = payment_gateway:charge(order(1)),
    rates(1.0, 0.0),
    ?_assertEqual({ok, Receipt}, payment_gateway:charge(order(1))).

decline_is_replayed(_) ->
    rates(0.0, 1.0),
    {error, declined} = payment_gateway:charge(order(1)),
    rates(0.0, 0.0),
    ?_assertEqual({error, declined}, payment_gateway:charge(order(1))).

timeout_is_not_remembered(_) ->
    rates(1.0, 0.0),
    {error, timeout} = payment_gateway:charge(order(1)),
    rates(0.0, 0.0),
    ?_assertMatch({ok, _}, payment_gateway:charge(order(1))).

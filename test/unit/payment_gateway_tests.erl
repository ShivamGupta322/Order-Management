-module(payment_gateway_tests).
-include_lib("eunit/include/eunit.hrl").

gateway_test_() ->
    {foreach, fun() -> ok end, fun(_) -> unset() end,
     [fun no_failures_always_succeeds/1,
      fun full_timeout_rate_always_times_out/1,
      fun full_decline_rate_always_declines/1]}.

order() -> #{id => <<"o-1">>}.

charge_many() -> [payment_gateway:charge(order()) || _ <- lists:seq(1, 50)].

rates(Timeout, Decline) ->
    ok = application:set_env(order_app, gateway_timeout_rate, Timeout),
    ok = application:set_env(order_app, gateway_decline_rate, Decline).

unset() ->
    ok = application:unset_env(order_app, gateway_timeout_rate),
    ok = application:unset_env(order_app, gateway_decline_rate).

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

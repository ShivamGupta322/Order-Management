-module(order_processor_tests).
-include_lib("eunit/include/eunit.hrl").

%% payment_gateway is mocked; the store is the real Mnesia one, in RAM.

processor_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun success_saves_completed/1,
      fun timeout_is_retryable_and_not_saved/1,
      fun decline_is_fatal_and_saved_failed/1,
      fun invalid_order_is_fatal_and_not_charged/1,
      fun fail_saves_failed/1]}.

setup() ->
    ok = application:set_env(order_app, mnesia_copies, ram_copies),
    ok = order_mnesia:init(),
    ok = meck:new(payment_gateway, [no_link]).

cleanup(_) ->
    meck:unload(payment_gateway),
    stopped = mnesia:stop(),
    application:unset_env(order_app, mnesia_copies).

order() ->
    #{id => <<"o-1">>, customer_id => <<"c-42">>, amount => 1500,
      currency => <<"INR">>, items => [#{sku => <<"SKU-1">>, qty => 2}],
      attempt => 0, created_at => <<"2026-11-14T10:00:00Z">>}.

success_saves_completed(_) ->
    Receipt = #{order_id => <<"o-1">>, receipt_id => <<"r-1">>},
    ok = meck:expect(payment_gateway, charge, fun(_) -> {ok, Receipt} end),
    Result = order_processor:process(order()),
    [?_assertEqual({ok, Receipt}, Result),
     ?_assertMatch({ok, #{status := completed, amount := 1500}}, order_store:get(<<"o-1">>))].

timeout_is_retryable_and_not_saved(_) ->
    ok = meck:expect(payment_gateway, charge, fun(_) -> {error, timeout} end),
    Result = order_processor:process(order()),
    [?_assertEqual({error, retryable, timeout}, Result),
     ?_assertEqual({error, not_found}, order_store:get(<<"o-1">>))].

decline_is_fatal_and_saved_failed(_) ->
    ok = meck:expect(payment_gateway, charge, fun(_) -> {error, declined} end),
    Result = order_processor:process(order()),
    [?_assertEqual({error, fatal, declined}, Result),
     ?_assertMatch({ok, #{status := failed}}, order_store:get(<<"o-1">>))].

fail_saves_failed(_) ->
    ok = order_processor:fail((order())#{attempt => 3}),
    ?_assertMatch({ok, #{status := failed, attempt := 3}}, order_store:get(<<"o-1">>)).

invalid_order_is_fatal_and_not_charged(_) ->
    ok = meck:expect(payment_gateway, charge, fun(_) -> {ok, #{}} end),
    Result = order_processor:process(maps:remove(currency, order())),
    [?_assertEqual({error, fatal, {missing, currency}}, Result),
     ?_assertEqual(0, meck:num_calls(payment_gateway, charge, '_')),
     ?_assertEqual({error, not_found}, order_store:get(<<"o-1">>))].

-module(order_consumer_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-compile([export_all, nowarn_export_all]).

all() ->
    [completed_order_is_saved_and_published,
     declined_order_goes_to_dlq_and_is_marked_failed,
     timeout_publishes_retry_with_next_attempt,
     exhausted_retries_go_to_dlq_and_mark_failed,
     retry_is_republished_after_not_before,
     retries_commit_in_offset_order,
     poison_retry_message_goes_to_dlq_and_commits,
     failed_order_goes_to_dlq,
     invalid_order_goes_to_dlq_without_charging,
     poison_message_goes_to_dlq_and_commits,
     get_order_returns_stored_order,
     get_unknown_order_returns_404,
     worker_restarts_after_crash].

init_per_suite(Config) ->
    ok = order_test_helper:start_app(),
    Config.

end_per_suite(_Config) ->
    order_test_helper:stop_app().

init_per_testcase(_Case, Config) ->
    ok = order_test_helper:reset(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------

completed_order_is_saved_and_published(_Config) ->
    Order = order(<<"o-1">>),
    ?assertMatch({ok, commit, _}, order_test_helper:feed(Order)),
    {ok, Stored} = order_store:get(<<"o-1">>),
    ?assertEqual(Order#{status => completed},
                 maps:without([updated_at], Stored)),
    [Payload] = order_test_helper:published(<<"orders.completed">>),
    ?assertEqual({ok, Order}, order_codec:decode(Payload)),
    ?assertEqual([], order_test_helper:published(<<"orders.dlq">>)).

declined_order_goes_to_dlq_and_is_marked_failed(_Config) ->
    ok = meck:expect(payment_gateway, charge, fun(_) -> {error, declined} end),
    ?assertMatch({ok, commit, _}, order_test_helper:feed(order(<<"o-2">>))),
    ?assertMatch({ok, #{status := failed}}, order_store:get(<<"o-2">>)),
    [Payload] = order_test_helper:published(<<"orders.dlq">>),
    ?assertMatch(#{<<"id">> := <<"o-2">>, <<"reason">> := <<"declined">>,
                   <<"attempt">> := 0, <<"failed_at">> := <<_/binary>>},
                 jsx:decode(Payload, [return_maps])),
    ?assertEqual([], order_test_helper:published(<<"orders.completed">>)).

timeout_publishes_retry_with_next_attempt(_Config) ->
    ok = meck:expect(payment_gateway, charge, fun(_) -> {error, timeout} end),
    Before = order_clock:now_ms(),
    ?assertMatch({ok, commit, _}, order_test_helper:feed(order(<<"o-3">>))),
    [Payload] = order_test_helper:published(<<"orders.retry">>),
    {ok, Retried, NotBefore} = order_codec:decode_retry(Payload),
    ?assertEqual((order(<<"o-3">>))#{attempt => 1}, Retried),
    ?assert(NotBefore >= Before + 1000 andalso NotBefore =< order_clock:now_ms() + 1000),
    ?assertEqual([], order_test_helper:published(<<"orders.dlq">>)),
    ?assertEqual({error, not_found}, order_store:get(<<"o-3">>)).

exhausted_retries_go_to_dlq_and_mark_failed(_Config) ->
    ok = meck:expect(payment_gateway, charge, fun(_) -> {error, timeout} end),
    Order = (order(<<"o-7">>))#{attempt => 3},
    ?assertMatch({ok, commit, _}, order_test_helper:feed(Order)),
    ?assertEqual([], order_test_helper:published(<<"orders.retry">>)),
    [Payload] = order_test_helper:published(<<"orders.dlq">>),
    ?assertMatch(#{<<"reason">> := <<"timeout">>, <<"attempt">> := 3},
                 jsx:decode(Payload, [return_maps])),
    ?assertMatch({ok, #{status := failed, attempt := 3}}, order_store:get(<<"o-7">>)).

retry_is_republished_after_not_before(_Config) ->
    Order = (order(<<"o-8">>))#{attempt => 1},
    NotBefore = order_clock:now_ms() + 300,
    ?assertMatch({ok, _}, order_test_helper:feed_retry(11, <<"o-8">>,
                                                       order_codec:encode_retry(Order, NotBefore))),
    %% Not committed until republished.
    receive {committed, _} -> ct:fail(committed_early) after 0 -> ok end,
    ok = order_test_helper:wait_until(
           fun() -> order_test_helper:published(<<"orders">>) =/= [] end, 5000),
    ?assert(order_clock:now_ms() >= NotBefore),
    [Payload] = order_test_helper:published(<<"orders">>),
    ?assertEqual({ok, Order}, order_codec:decode(Payload)),
    ?assert(meck:called(order_producer, publish, [<<"orders">>, <<"o-8">>, '_'])),
    receive {committed, Offset} -> ?assertEqual(11, Offset) after 2000 -> ct:fail(no_commit) end.

%% A due retry behind a later one waits for it, so commits never skip ahead.
retries_commit_in_offset_order(_Config) ->
    Now = order_clock:now_ms(),
    Late = (order(<<"o-9">>))#{attempt => 1},
    Due = (order(<<"o-10">>))#{attempt => 1},
    {ok, _} = order_test_helper:feed_retry(21, <<"o-9">>, order_codec:encode_retry(Late, Now + 300)),
    {ok, _} = order_test_helper:feed_retry(22, <<"o-10">>, order_codec:encode_retry(Due, Now)),
    ok = order_test_helper:wait_until(
           fun() -> length(order_test_helper:published(<<"orders">>)) =:= 2 end, 5000),
    ?assertEqual([{ok, Late}, {ok, Due}],
                 [order_codec:decode(P) || P <- order_test_helper:published(<<"orders">>)]),
    ?assertEqual([21, 22], [receive {committed, O} -> O after 2000 -> timeout end || _ <- [1, 2]]).

poison_retry_message_goes_to_dlq_and_commits(_Config) ->
    {ok, _} = order_test_helper:feed_retry(31, <<"p-2">>, <<"{\"id\":\"p-2\"}">>),
    ok = order_test_helper:wait_until(
           fun() -> order_test_helper:published(<<"orders.dlq">>) =/= [] end, 5000),
    [Payload] = order_test_helper:published(<<"orders.dlq">>),
    ?assertMatch(#{<<"reason">> := <<"invalid_not_before">>}, jsx:decode(Payload, [return_maps])),
    ?assertEqual([], order_test_helper:published(<<"orders">>)),
    receive {committed, Offset} -> ?assertEqual(31, Offset) after 2000 -> ct:fail(no_commit) end.

%% The guide's scenario: a gateway that always times out is tried four times
%% (attempts 0-3), retried three times, then the order lands on orders.dlq.
%% Short delays keep it fast; the full loop goes through retry_manager.
failed_order_goes_to_dlq(_Config) ->
    ok = application:set_env(order_app, retry_delays_ms, [10, 20, 30]),
    ok = meck:expect(payment_gateway, charge, fun(_) -> {error, timeout} end),
    {ok, commit, _} = order_test_helper:feed(order(<<"bad-1">>)),
    ok = lists:foreach(fun retry_round/1, [1, 2, 3]),
    ?assertEqual(4, meck:num_calls(payment_gateway, charge, '_')),
    ?assertEqual(3, length(order_test_helper:published(<<"orders.retry">>))),
    [Payload] = order_test_helper:published(<<"orders.dlq">>),
    ?assertMatch(#{<<"id">> := <<"bad-1">>, <<"reason">> := <<"timeout">>, <<"attempt">> := 3},
                 jsx:decode(Payload, [return_maps])),
    ?assertMatch({ok, #{status := failed}}, order_store:get(<<"bad-1">>)).

%% Takes retry message N from orders.retry, passes it through retry_manager,
%% and feeds the republished order back to the orders consumer.
retry_round(N) ->
    RetryPayload = lists:nth(N, order_test_helper:published(<<"orders.retry">>)),
    {ok, _} = order_test_helper:feed_retry(100 + N, <<"bad-1">>, RetryPayload),
    ok = order_test_helper:wait_until(
           fun() -> length(order_test_helper:published(<<"orders">>)) =:= N end, 5000),
    {ok, Republished} = order_codec:decode(lists:nth(N, order_test_helper:published(<<"orders">>))),
    ?assertEqual(N, maps:get(attempt, Republished)),
    {ok, commit, _} = order_test_helper:feed(Republished),
    receive {committed, _} -> ok after 2000 -> ct:fail(no_commit) end.

invalid_order_goes_to_dlq_without_charging(_Config) ->
    Order = (order(<<"o-4">>))#{amount => -5},
    ?assertMatch({ok, commit, _}, order_test_helper:feed(Order)),
    [Payload] = order_test_helper:published(<<"orders.dlq">>),
    ?assertMatch(#{<<"reason">> := <<"invalid:amount">>}, jsx:decode(Payload, [return_maps])),
    ?assertEqual(0, meck:num_calls(payment_gateway, charge, '_')),
    ?assertEqual({error, not_found}, order_store:get(<<"o-4">>)).

poison_message_goes_to_dlq_and_commits(_Config) ->
    Raw = <<"not json", 255, 0>>,
    ?assertMatch({ok, commit, _}, order_test_helper:feed_raw(<<"p-1">>, Raw)),
    ?assert(meck:called(order_producer, publish, [<<"orders.dlq">>, <<"p-1">>, '_'])),
    [Payload] = order_test_helper:published(<<"orders.dlq">>),
    #{<<"raw_base64">> := Encoded} = Decoded = jsx:decode(Payload, [return_maps]),
    ?assertEqual(Raw, base64:decode(Encoded)),
    ?assertMatch(#{<<"reason">> := <<"malformed_json">>}, Decoded),
    ?assertEqual(0, meck:num_calls(payment_gateway, charge, '_')).

get_order_returns_stored_order(_Config) ->
    {ok, commit, _} = order_test_helper:feed(order(<<"o-5">>)),
    {200, Body} = http_get("/orders/o-5"),
    ?assertMatch(#{<<"id">> := <<"o-5">>, <<"status">> := <<"completed">>,
                   <<"amount">> := 1500, <<"currency">> := <<"INR">>,
                   <<"customer_id">> := <<"c-42">>,
                   <<"items">> := [#{<<"sku">> := <<"SKU-1">>, <<"qty">> := 2}]},
                 Body).

get_unknown_order_returns_404(_Config) ->
    ?assertEqual({404, #{<<"error">> => <<"not_found">>}}, http_get("/orders/nope")).

%% Supervision wiring: consumer_sup restarts the consumer and nothing else in
%% the app is touched. That no order is lost is proven against real Kafka in
%% e2e_SUITE:killed_worker_loses_no_orders.
worker_restarts_after_crash(_Config) ->
    Siblings = supervisor:which_children(order_sup) -- [consumer_child()],
    {ok, Pid} = consumer_sup:subscriber(),
    Ref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, killed} -> ok after 2000 -> ct:fail(no_down) end,
    ok = order_test_helper:wait_until(
           fun() ->
                   case consumer_sup:subscriber() of
                       {ok, New} -> New =/= Pid;
                       {error, not_running} -> false
                   end
           end, 5000),
    ?assertEqual(Siblings, supervisor:which_children(order_sup) -- [consumer_child()]),
    %% The app still serves traffic.
    {ok, commit, _} = order_test_helper:feed(order(<<"o-6">>)),
    ?assertMatch({200, #{<<"status">> := <<"completed">>}}, http_get("/orders/o-6")).

consumer_child() ->
    lists:keyfind(consumer_sup, 1, supervisor:which_children(order_sup)).

%%--------------------------------------------------------------------

order(Id) ->
    #{id => Id,
      customer_id => <<"c-42">>,
      amount => 1500,
      currency => <<"INR">>,
      items => [#{sku => <<"SKU-1">>, qty => 2}],
      attempt => 0,
      created_at => <<"2026-11-14T10:00:00Z">>}.

http_get(Path) ->
    {ok, {{_, Status, _}, _, Body}} =
        httpc:request(get, {order_test_helper:base_url() ++ Path, []}, [],
                      [{body_format, binary}]),
    {Status, jsx:decode(Body, [return_maps])}.

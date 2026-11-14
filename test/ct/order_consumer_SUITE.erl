-module(order_consumer_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-compile([export_all, nowarn_export_all]).

all() ->
    [completed_order_is_saved_and_published,
     declined_order_goes_to_dlq_and_is_marked_failed,
     gateway_timeout_goes_to_dlq,
     invalid_order_goes_to_dlq_without_charging,
     poison_message_goes_to_dlq_and_commits,
     get_order_returns_stored_order,
     get_unknown_order_returns_404].

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

%% Phase 3 behaviour: retryable failures are dead-lettered until phase 5.
gateway_timeout_goes_to_dlq(_Config) ->
    ok = meck:expect(payment_gateway, charge, fun(_) -> {error, timeout} end),
    ?assertMatch({ok, commit, _}, order_test_helper:feed(order(<<"o-3">>))),
    [Payload] = order_test_helper:published(<<"orders.dlq">>),
    ?assertMatch(#{<<"reason">> := <<"timeout">>}, jsx:decode(Payload, [return_maps])),
    ?assertEqual({error, not_found}, order_store:get(<<"o-3">>)).

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

-module(order_producer_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-compile([export_all, nowarn_export_all]).

all() ->
    [valid_order_is_published_keyed_by_id,
     published_payload_decodes_to_submitted_order,
     invalid_body_returns_400_and_publishes_nothing,
     produce_error_returns_503,
     wrong_method_returns_405,
     health_returns_200].

init_per_suite(Config) ->
    %% Port 0 lets the OS pick a free port; read it back from ranch below.
    ok = application:set_env(order_app, http_port, 0, [{persistent, true}]),
    {ok, _} = application:ensure_all_started(inets),
    ok = meck:new(order_producer, [no_link]),
    %% brod_client needs a reachable broker to start, so the producer edge is
    %% stubbed with a linked process the supervisor can own and shut down.
    ok = meck:expect(order_producer, start_link,
                     fun() -> {ok, proc_lib:spawn_link(fun stub_loop/0)} end),
    {ok, _} = application:ensure_all_started(order_app),
    [{base_url, "http://127.0.0.1:" ++ integer_to_list(ranch:get_port(order_http))}
     | Config].

end_per_suite(_Config) ->
    ok = application:stop(order_app),
    ok = application:unset_env(order_app, http_port, [{persistent, true}]),
    meck:unload().

init_per_testcase(_Case, Config) ->
    ok = meck:expect(order_producer, publish, fun(_Topic, _Key, _Value) -> ok end),
    ok = meck:reset(order_producer),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

stub_loop() ->
    receive _ -> stub_loop() end.

%%--------------------------------------------------------------------

valid_order_is_published_keyed_by_id(Config) ->
    {202, Body} = post(Config, encode(valid_order())),
    ?assertEqual(#{<<"id">> => <<"o-1001">>}, Body),
    ?assertEqual(1, meck:num_calls(order_producer, publish, '_')),
    ?assert(meck:called(order_producer, publish, [<<"orders">>, <<"o-1001">>, '_'])).

published_payload_decodes_to_submitted_order(Config) ->
    {202, _} = post(Config, encode(valid_order())),
    [Payload] = [V || {_, {order_producer, publish, [_, _, V]}, _} <- meck:history(order_producer)],
    {ok, Event} = order_codec:decode(Payload),
    ?assertEqual(valid_order(), maps:without([attempt, created_at], Event)),
    ?assertMatch(#{attempt := 0}, Event),
    #{created_at := CreatedAt} = Event,
    ?assert(is_integer(calendar:rfc3339_to_system_time(binary_to_list(CreatedAt)))).

invalid_body_returns_400_and_publishes_nothing(Config) ->
    Cases = [{<<"{not json">>, #{<<"error">> => <<"malformed_json">>}},
             {<<"[1,2]">>, #{<<"error">> => <<"not_an_object">>}},
             {encode(maps:remove(customer_id, valid_order())),
              #{<<"error">> => <<"missing">>, <<"field">> => <<"customer_id">>}},
             {encode((valid_order())#{amount => -5}),
              #{<<"error">> => <<"invalid">>, <<"field">> => <<"amount">>}},
             {encode((valid_order())#{amount => 15.5}),
              #{<<"error">> => <<"invalid">>, <<"field">> => <<"amount">>}}],
    [?assertEqual({400, Expected}, post(Config, Body)) || {Body, Expected} <- Cases],
    ?assertEqual(0, meck:num_calls(order_producer, publish, '_')).

produce_error_returns_503(Config) ->
    ok = meck:expect(order_producer, publish,
                     fun(_, _, _) -> {error, leader_not_available} end),
    Sup = whereis(order_sup),
    ?assertEqual({503, #{<<"error">> => <<"kafka_unavailable">>}},
                 post(Config, encode(valid_order()))),
    %% Not a crash: same supervisor, and the listener still serves requests.
    ?assertEqual(Sup, whereis(order_sup)),
    ?assertMatch({200, _}, request(get, Config, "/health")).

wrong_method_returns_405(Config) ->
    ?assertMatch({405, _}, request(get, Config, "/orders")),
    ?assertEqual(0, meck:num_calls(order_producer, publish, '_')).

health_returns_200(Config) ->
    ?assertEqual({200, #{<<"status">> => <<"ok">>}}, request(get, Config, "/health")).

%%--------------------------------------------------------------------

valid_order() ->
    #{id => <<"o-1001">>,
      customer_id => <<"c-42">>,
      amount => 1500,
      currency => <<"INR">>,
      items => [#{sku => <<"SKU-1">>, qty => 2}]}.

encode(Order) ->
    jsx:encode(Order).

post(Config, Body) ->
    Url = ?config(base_url, Config) ++ "/orders",
    response(httpc:request(post, {Url, [], "application/json", Body}, [],
                           [{body_format, binary}])).

request(Method, Config, Path) ->
    Url = ?config(base_url, Config) ++ Path,
    response(httpc:request(Method, {Url, []}, [], [{body_format, binary}])).

response({ok, {{_Version, Status, _Phrase}, _Headers, Body}}) ->
    {Status, jsx:decode(Body, [return_maps])}.

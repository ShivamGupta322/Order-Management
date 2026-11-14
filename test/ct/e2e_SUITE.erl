-module(e2e_SUITE).

%% End-to-end against the real Kafka from Docker Compose. Not part of the
%% default run; start it with:
%%   docker compose run --rm app rebar3 ct --dir test/ct --suite e2e_SUITE
%% Nothing is mocked. Mnesia runs in RAM, and each run uses a fresh consumer
%% group that starts at the latest offset, so old messages are not replayed.
%%
%% `latest' only applies to a partition with no committed offset. A restart
%% before a partition's first commit would skip whatever arrived meanwhile,
%% so init_per_suite sends one order to every partition and waits until the
%% group has committed on all of them.

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-compile([export_all, nowarn_export_all]).

-define(KAFKA_HOST, "kafka").
-define(KAFKA_PORT, 19092).
-define(PARTITIONS, 3).
-define(BATCH, 30).

all() ->
    [order_completes_end_to_end,
     killed_worker_loses_no_orders].

init_per_suite(Config) ->
    case gen_tcp:connect(?KAFKA_HOST, ?KAFKA_PORT, [], 2000) of
        {ok, Socket} ->
            ok = gen_tcp:close(Socket),
            start(Config);
        {error, Reason} ->
            {skip, {kafka_unreachable, Reason}}
    end.

start(Config) ->
    RunId = integer_to_binary(erlang:system_time(millisecond)),
    Env = [{http_port, 0},
           {mnesia_copies, ram_copies},
           {kafka_endpoints, [{?KAFKA_HOST, ?KAFKA_PORT}]},
           {consumer_group_id, <<"e2e-", RunId/binary>>},
           {consumer_begin_offset, latest},
           {offset_commit_interval_seconds, 1},
           {gateway_timeout_rate, 0.0},
           {gateway_decline_rate, 0.0}],
    [ok = application:set_env(order_app, K, V, [{persistent, true}]) || {K, V} <- Env],
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(order_app),
    ok = wait_for_assignment(),
    Config1 = [{run_id, RunId}, {env_keys, [K || {K, _} <- Env]} | Config],
    ok = warm_up(Config1, <<"e2e-", RunId/binary>>),
    Config1.

end_per_suite(Config) ->
    ok = application:stop(order_app),
    stopped = mnesia:stop(),
    [ok = application:unset_env(order_app, K, [{persistent, true}])
     || K <- ?config(env_keys, Config)],
    ok.

%%--------------------------------------------------------------------

order_completes_end_to_end(Config) ->
    Id = order_id(Config, <<"single">>, 1),
    ?assertEqual(202, post_order(Id)),
    ok = order_test_helper:wait_until(fun() -> completed(Id) end, 20000).

%% Posts a batch, kills one partition's worker mid-batch, and checks every
%% order still completes. The whole group subscriber restarts (brod stops it
%% when any worker dies) and resumes from committed offsets.
killed_worker_loses_no_orders(Config) ->
    Ids = [order_id(Config, <<"crash">>, N) || N <- lists:seq(1, ?BATCH)],
    {First, Rest} = lists:split(?BATCH div 3, Ids),
    [?assertEqual(202, post_order(Id)) || Id <- First],
    {ok, Subscriber} = consumer_sup:subscriber(),
    {ok, #{0 := Worker}} = consumer_worker:partition_workers(),
    exit(Worker, kill),
    [?assertEqual(202, post_order(Id)) || Id <- Rest],
    ok = order_test_helper:wait_until(
           fun() -> lists:all(fun completed/1, Ids) end, 60000),
    {ok, NewSubscriber} = consumer_sup:subscriber(),
    ?assertNotEqual(Subscriber, NewSubscriber).

%%--------------------------------------------------------------------

wait_for_assignment() ->
    order_test_helper:wait_until(
      fun() ->
              case consumer_worker:partition_workers() of
                  {ok, Workers} -> map_size(Workers) =:= ?PARTITIONS;
                  {error, _} -> false
              end
      end, 30000).

warm_up(Config, GroupId) ->
    Ids = [warm_up_id(Config, P, 1) || P <- lists:seq(0, ?PARTITIONS - 1)],
    [202 = post_order(Id) || Id <- Ids],
    ok = order_test_helper:wait_until(fun() -> lists:all(fun completed/1, Ids) end, 20000),
    order_test_helper:wait_until(fun() -> committed_partitions(GroupId) =:= ?PARTITIONS end,
                                 20000).

%% An order ID that brod's `hash' partitioner (phash2(Key) rem N) sends to P.
warm_up_id(Config, P, N) ->
    Id = order_id(Config, <<"warmup">>, N),
    case erlang:phash2(Id) rem ?PARTITIONS of
        P -> Id;
        _ -> warm_up_id(Config, P, N + 1)
    end.

%% Partitions without a commit are absent from the response.
committed_partitions(GroupId) ->
    {ok, Topics} = brod:fetch_committed_offsets(order_producer:client(), GroupId),
    length([P || #{name := <<"orders">>, partitions := Ps} <- Topics,
                 #{committed_offset := Offset} = P <- Ps, Offset >= 0]).

order_id(Config, Kind, N) ->
    RunId = ?config(run_id, Config),
    <<"e2e-", RunId/binary, "-", Kind/binary, "-", (integer_to_binary(N))/binary>>.

post_order(Id) ->
    Body = jsx:encode(#{id => Id, customer_id => <<"c-e2e">>, amount => 1000,
                        currency => <<"INR">>, items => [#{sku => <<"SKU-1">>, qty => 1}]}),
    {ok, {{_, Status, _}, _, _}} =
        httpc:request(post, {url("/orders"), [], "application/json", Body}, [], []),
    Status.

completed(Id) ->
    case httpc:request(get, {url("/orders/" ++ binary_to_list(Id)), []}, [],
                       [{body_format, binary}]) of
        {ok, {{_, 200, _}, _, Body}} ->
            maps:get(<<"status">>, jsx:decode(Body, [return_maps])) =:= <<"completed">>;
        {ok, {{_, 404, _}, _, _}} ->
            false
    end.

url(Path) ->
    order_test_helper:base_url() ++ Path.

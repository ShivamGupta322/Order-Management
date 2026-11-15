-module(order_test_helper).

%% Shared CT setup: runs order_app without a broker by stubbing the Kafka
%% edges (order_producer, consumer_worker:start_link/1 for both consumer
%% groups) and payment_gateway, with Mnesia in RAM so no files are left
%% behind. retry_manager runs for real.

-include_lib("brod/include/brod.hrl").

-export([start_app/0, stop_app/0, reset/0, base_url/0, feed/1, feed_raw/2,
         feed_retry/3, published/1, receipt/0, wait_until/2]).

-define(POLL_INTERVAL_MS, 50).

-define(MOCKS, [order_producer, consumer_worker, payment_gateway]).

start_app() ->
    ok = application:set_env(order_app, http_port, 0, [{persistent, true}]),
    ok = application:set_env(order_app, mnesia_copies, ram_copies, [{persistent, true}]),
    {ok, _} = application:ensure_all_started(inets),
    ok = meck:new(order_producer, [no_link]),
    ok = meck:expect(order_producer, start_link, fun start_stub/0),
    ok = meck:new(consumer_worker, [passthrough, no_link]),
    ok = meck:expect(consumer_worker, start_link, fun(_Role) -> start_stub() end),
    %% passthrough lets a test opt into the real gateway with meck:passthrough/1.
    ok = meck:new(payment_gateway, [passthrough, no_link]),
    ok = reset(),
    {ok, _} = application:ensure_all_started(order_app),
    ok.

stop_app() ->
    ok = application:stop(order_app),
    stopped = mnesia:stop(),
    ok = application:unset_env(order_app, http_port, [{persistent, true}]),
    ok = application:unset_env(order_app, mnesia_copies, [{persistent, true}]),
    meck:unload(?MOCKS).

%% Default edge behaviour for each test: publishing and charging succeed.
reset() ->
    ok = meck:expect(order_producer, publish, fun(_Topic, _Key, _Value) -> ok end),
    ok = meck:expect(payment_gateway, charge, fun(_Order) -> {ok, receipt()} end),
    ok = meck:reset(?MOCKS),
    ok = application:unset_env(order_app, retry_delays_ms),
    ok = flush_commits(),
    clear_tables().

flush_commits() ->
    receive {committed, _} -> flush_commits() after 0 -> ok end.

clear_tables() ->
    case mnesia:system_info(is_running) of
        yes -> [{atomic, ok} = mnesia:clear_table(T) || T <- [order, processed_order, gateway_charge]],
               ok;
        no -> ok
    end.

base_url() ->
    "http://127.0.0.1:" ++ integer_to_list(ranch:get_port(order_http)).

%% Hands an encoded order straight to the worker callback, as Kafka would.
feed(#{id := Id} = Order) ->
    feed_raw(Id, jsx:encode(Order)).

feed_raw(Key, Value) ->
    handle(orders, <<"orders">>, erlang:unique_integer([positive]), Key, Value,
           fun(_) -> ok end).

%% Hands a message to the retry-role callback. Its commit, made later by
%% retry_manager, arrives at the calling process as {committed, Offset}.
feed_retry(Offset, Key, Value) ->
    Self = self(),
    handle(retry, <<"orders.retry">>, Offset, Key, Value,
           fun(O) -> Self ! {committed, O}, ok end).

handle(Role, Topic, Offset, Key, Value, Commit) ->
    {ok, State} = consumer_worker:init(#{group_id => <<"test">>,
                                         topic => Topic,
                                         partition => 0,
                                         commit_fun => Commit,
                                         ack_fun => fun(_) -> ok end},
                                       Role),
    consumer_worker:handle_message(#kafka_message{offset = Offset, key = Key, value = Value},
                                   State).

%% Values published to Topic since the last reset, oldest first.
published(Topic) ->
    [Value || {_Pid, {order_producer, publish, [T, _Key, Value]}, _Result}
                  <- meck:history(order_producer),
              T =:= Topic].

receipt() ->
    #{order_id => <<"test">>, receipt_id => <<"r-1">>}.

%% Polls Fun until it returns true, failing the test after Timeout ms.
wait_until(Fun, Timeout) when Timeout =< 0 ->
    case Fun() of
        true -> ok;
        false -> ct:fail(wait_until_timeout)
    end;
wait_until(Fun, Timeout) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(?POLL_INTERVAL_MS),  % poll interval, not a wait for an outcome
            wait_until(Fun, Timeout - ?POLL_INTERVAL_MS)
    end.

start_stub() ->
    %% A linked process the supervisor can own and shut down.
    {ok, proc_lib:spawn_link(fun stub_loop/0)}.

stub_loop() ->
    receive _ -> stub_loop() end.

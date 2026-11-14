-module(retry_manager).
-behaviour(gen_server).

%% Holds messages read from orders.retry until their `not_before' time, then
%% publishes them (normally back to `orders') and commits their offset.
%%
%% One FIFO queue per partition, released strictly in order: an offset is
%% committed only after every earlier one, so a crash never commits past a
%% retry that was not yet republished. The cost is head-of-line blocking (a
%% 1 s retry queued behind a 30 s one waits for it), which only ever delays.
%%
%% Waiting uses erlang:send_after/3, so the loop is never blocked. Entries are
%% not persisted: they are uncommitted, so after a restart Kafka redelivers
%% them (retry_sup restarts this server and the retry group together).

-include_lib("kernel/include/logger.hrl").

-export([start_link/0, schedule/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export_type([entry/0]).

-type entry() :: #{partition := non_neg_integer(),
                   offset := integer(),
                   key := binary(),
                   topic := binary(),
                   payload := binary(),
                   not_before := integer(),
                   commit := fun((integer()) -> ok)}.
-type queues() :: #{non_neg_integer() => queue:queue(entry())}.

-define(DEFAULT_CALL_TIMEOUT_MS, 5000).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec schedule(entry()) -> ok | {error, timeout}.
schedule(Entry) ->
    Timeout = application:get_env(order_app, call_timeout_ms, ?DEFAULT_CALL_TIMEOUT_MS),
    try
        gen_server:call(?MODULE, {schedule, Entry}, Timeout)
    catch
        exit:{timeout, _} -> {error, timeout}
    end.

-spec init([]) -> {ok, queues()}.
init([]) ->
    %% Trap exits so a shutdown waits for an in-progress publish to finish.
    process_flag(trap_exit, true),
    {ok, #{}}.

-spec handle_call({schedule, entry()}, gen_server:from(), queues()) ->
          {reply, ok, queues()}.
handle_call({schedule, #{partition := Partition} = Entry}, _From, Queues) ->
    Queue = maps:get(Partition, Queues, queue:new()),
    ok = arm_if_head(queue:is_empty(Queue), Entry),
    {reply, ok, Queues#{Partition => queue:in(Entry, Queue)}}.

-spec handle_cast(term(), queues()) -> {noreply, queues()}.
handle_cast(_Msg, Queues) ->
    {noreply, Queues}.

-spec handle_info({due, non_neg_integer()} | term(), queues()) -> {noreply, queues()}.
handle_info({due, Partition}, Queues) ->
    {{value, Entry}, Rest} = queue:out(maps:get(Partition, Queues)),
    ok = release(Entry),
    ok = arm_next(queue:peek(Rest)),
    {noreply, Queues#{Partition => Rest}};
handle_info(_Info, Queues) ->
    {noreply, Queues}.

-spec terminate(term(), queues()) -> ok.
terminate(Reason, Queues) ->
    Pending = lists:sum([queue:len(Q) || Q <- maps:values(Queues)]),
    ?LOG_INFO(#{event => retry_manager_stopped, reason => Reason, pending => Pending}).

%%--------------------------------------------------------------------

arm_if_head(true, Entry) -> arm(Entry);
arm_if_head(false, _Entry) -> ok.

arm_next({value, Entry}) -> arm(Entry);
arm_next(empty) -> ok.

arm(#{partition := Partition, not_before := NotBefore}) ->
    Delay = max(0, NotBefore - order_clock:now_ms()),
    _TimerRef = erlang:send_after(Delay, self(), {due, Partition}),
    ok.

%% A failed publish crashes this server on purpose: the entry stays
%% uncommitted and is redelivered after retry_sup restarts us.
release(#{topic := Topic, key := Key, payload := Payload, offset := Offset,
          partition := Partition, commit := Commit}) ->
    ok = order_producer:publish(Topic, Key, Payload),
    ok = Commit(Offset),
    ?LOG_INFO(#{event => retry_released, order_id => Key, topic => Topic,
                partition => Partition, offset => Offset}).

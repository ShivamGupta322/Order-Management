-module(consumer_worker).
-behaviour(brod_group_subscriber_v2).

%% Thin Kafka shell, run by brod as one callback process per assigned
%% partition. Two roles, each its own consumer group:
%%
%%   orders - decode, call order_processor, publish the outcome (completed,
%%            retry, or DLQ), then commit. Supervised by consumer_sup.
%%   retry  - decode a message from orders.retry and hand it to retry_manager,
%%            which republishes it after `not_before' and commits it then.
%%            Supervised by retry_sup.
%%
%% Shutdown: the subscriber stops each worker with a message queued behind
%% the one being handled, so the in-flight order finishes, and then flushes
%% acked offsets before exiting.

-include_lib("brod/include/brod.hrl").
-include_lib("kernel/include/logger.hrl").
-include("order_topics.hrl").

-export([start_link/1, partition_workers/1]).
-export([init/2, handle_message/2, terminate/2]).
-export_type([role/0, state/0]).

-type role() :: orders | retry.
-type state() :: #{role := role(),
                   topic := binary(),
                   partition := non_neg_integer(),
                   commit := fun((integer()) -> ok)}.

-define(DEFAULT_GROUP_ID, <<"order_consumers">>).
-define(DEFAULT_RETRY_GROUP_ID, <<"order_retry">>).
-define(DEFAULT_COMMIT_INTERVAL_S, 5).
-define(DEFAULT_BEGIN_OFFSET, earliest).
-define(DEFAULT_CALL_TIMEOUT_MS, 5000).

-spec start_link(role()) -> {ok, pid()} | {error, term()}.
start_link(Role) ->
    Interval = application:get_env(order_app, offset_commit_interval_seconds,
                                   ?DEFAULT_COMMIT_INTERVAL_S),
    %% Only used when the group has no committed offset for a partition.
    BeginOffset = application:get_env(order_app, consumer_begin_offset,
                                      ?DEFAULT_BEGIN_OFFSET),
    brod_group_subscriber_v2:start_link(
      #{client => order_producer:client(),
        group_id => group_id(Role),
        topics => [topic(Role)],
        cb_module => ?MODULE,
        init_data => Role,
        message_type => message,
        consumer_config => [{begin_offset, BeginOffset}],
        group_config => [{offset_commit_policy, commit_to_kafka_v2},
                         {offset_commit_interval_seconds, Interval}]}).

group_id(orders) -> application:get_env(order_app, consumer_group_id, ?DEFAULT_GROUP_ID);
group_id(retry) -> application:get_env(order_app, retry_group_id, ?DEFAULT_RETRY_GROUP_ID).

topic(orders) -> ?TOPIC_ORDERS;
topic(retry) -> ?TOPIC_RETRY.

%% Partition number to the pid of the process consuming it. Used by tests
%% and the crash-recovery demo.
-spec partition_workers(role()) -> {ok, #{non_neg_integer() => pid()}} | {error, term()}.
partition_workers(orders) -> workers(consumer_sup:subscriber());
partition_workers(retry) -> workers(retry_sup:subscriber()).

workers({ok, Subscriber}) ->
    Timeout = application:get_env(order_app, call_timeout_ms, ?DEFAULT_CALL_TIMEOUT_MS),
    %% brod 4.7 exports only get_workers/1, which waits forever. This is the
    %% same request its unexported get_workers/2 makes, with a bounded timeout.
    try gen_server:call(Subscriber, get_workers, Timeout) of
        Workers -> {ok, maps:from_list([{P, Pid} || {{_Topic, P}, Pid} <- maps:to_list(Workers)])}
    catch
        exit:{timeout, _} -> {error, timeout}
    end;
workers({error, _} = Error) ->
    Error.

-spec init(brod_group_subscriber_v2:init_info(), role()) -> {ok, state()}.
init(#{topic := Topic, partition := Partition, commit_fun := Commit}, Role) ->
    ?LOG_INFO(#{event => consumer_worker_started, role => Role, topic => Topic,
                partition => Partition}),
    {ok, #{role => Role, topic => Topic, partition => Partition, commit => Commit}}.

-spec terminate(term(), state()) -> ok.
terminate(Reason, #{role := Role, topic := Topic, partition := Partition}) ->
    ?LOG_INFO(#{event => consumer_worker_stopped, role => Role, topic => Topic,
                partition => Partition, reason => Reason}).

%% `orders' returns commit only after the outcome is published; if anything
%% crashes first, the offset is not committed and the message is redelivered.
%% `retry' never commits here: retry_manager commits once it has republished.
-spec handle_message(brod:message(), state()) -> {ok, commit, state()} | {ok, state()}.
handle_message(#kafka_message{offset = Offset, key = Key, value = Value},
               #{role := Role, partition := Partition} = State) ->
    Meta = #{order_id => Key, partition => Partition, offset => Offset},
    handle(Role, Value, Meta, State).

handle(orders, Value, Meta, State) ->
    ok = process(order_codec:decode(Value), Value, Meta),
    {ok, commit, State};
handle(retry, Value, Meta, #{commit := Commit} = State) ->
    Entry = retry_entry(order_codec:decode_retry(Value), Value, Meta),
    %% A timeout crashes this worker on purpose, so the message is redelivered.
    ok = retry_manager:schedule(Entry#{commit => Commit}),
    {ok, State}.

%%-- orders ----------------------------------------------------------

process({ok, Order}, Value, Meta) ->
    route(order_processor:process(Order), Order, Value, Meta);
process({error, Reason}, Value, Meta) ->
    %% A poison message must not stall the partition: dead-letter and commit.
    dead_letter(raw_dead_letter(Value, Reason), Reason, Meta).

route({ok, _Receipt}, _Order, Value, #{order_id := Key} = Meta) ->
    ok = order_producer:publish(?TOPIC_COMPLETED, Key, Value),
    ?LOG_INFO(Meta#{event => order_completed});
route({error, retryable, Reason}, Order, _Value, Meta) ->
    Delays = application:get_env(order_app, retry_delays_ms, retry_policy:default_delays()),
    retry_or_give_up(retry_policy:next(attempt(Order), Delays), Order, Reason, Meta);
route({error, fatal, Reason}, Order, _Value, Meta) ->
    dead_letter(order_dead_letter(Order, Reason), Reason, Meta#{class => fatal}).

retry_or_give_up({retry, Delay}, Order, Reason, #{order_id := Key} = Meta) ->
    Next = attempt(Order) + 1,
    NotBefore = order_clock:now_ms() + Delay,
    Payload = order_codec:encode_retry(Order#{attempt => Next}, NotBefore),
    ok = order_producer:publish(?TOPIC_RETRY, Key, Payload),
    ?LOG_WARNING(Meta#{event => order_retry, attempt => Next, delay_ms => Delay,
                       reason => Reason});
retry_or_give_up(dead_letter, Order, Reason, Meta) ->
    ok = order_processor:fail(Order),
    dead_letter(order_dead_letter(Order, Reason), Reason,
                Meta#{class => retries_exhausted}).

attempt(Order) ->
    maps:get(attempt, Order, 0).

dead_letter(Payload, Reason, #{order_id := Key} = Meta) ->
    ok = order_producer:publish(?TOPIC_DLQ, Key, Payload),
    ?LOG_ERROR(Meta#{event => order_dead_lettered, reason => Reason}).

%%-- retry -----------------------------------------------------------

retry_entry({ok, Order, NotBefore}, _Value, Meta) ->
    entry(?TOPIC_ORDERS, order_codec:encode(Order), NotBefore, Meta);
retry_entry({error, Reason}, Value, Meta) ->
    %% Dead-lettered through retry_manager rather than here, so it is
    %% committed in offset order with the retries queued before it.
    ?LOG_ERROR(Meta#{event => order_dead_lettered, reason => Reason}),
    entry(?TOPIC_DLQ, raw_dead_letter(Value, Reason), 0, Meta).

entry(Topic, Payload, NotBefore, #{order_id := Key, partition := Partition, offset := Offset}) ->
    #{partition => Partition, offset => Offset, key => Key, topic => Topic,
      payload => Payload, not_before => NotBefore}.

%%--------------------------------------------------------------------

order_dead_letter(Order, Reason) ->
    order_codec:encode_dead_letter(Order, Reason, order_clock:now_rfc3339()).

raw_dead_letter(Value, Reason) ->
    order_codec:encode_dead_letter_raw(Value, Reason, order_clock:now_rfc3339()).

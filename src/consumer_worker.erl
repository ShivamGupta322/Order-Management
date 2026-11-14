-module(consumer_worker).
-behaviour(brod_group_subscriber_v2).

%% Thin Kafka shell: decode, call order_processor, publish the outcome, then
%% commit. brod runs one callback process per assigned partition.

-include_lib("brod/include/brod.hrl").
-include_lib("kernel/include/logger.hrl").
-include("order_topics.hrl").

-export([start_link/0]).
-export([init/2, handle_message/2]).
-export_type([state/0]).

-type state() :: #{topic := binary(), partition := non_neg_integer()}.

-define(DEFAULT_GROUP_ID, <<"order_consumers">>).
-define(DEFAULT_COMMIT_INTERVAL_S, 5).

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    GroupId = application:get_env(order_app, consumer_group_id, ?DEFAULT_GROUP_ID),
    Interval = application:get_env(order_app, offset_commit_interval_seconds,
                                   ?DEFAULT_COMMIT_INTERVAL_S),
    brod_group_subscriber_v2:start_link(
      #{client => order_producer:client(),
        group_id => GroupId,
        topics => [?TOPIC_ORDERS],
        cb_module => ?MODULE,
        message_type => message,
        consumer_config => [{begin_offset, earliest}],
        group_config => [{offset_commit_policy, commit_to_kafka_v2},
                         {offset_commit_interval_seconds, Interval}]}).

-spec init(brod_group_subscriber_v2:init_info(), term()) -> {ok, state()}.
init(#{topic := Topic, partition := Partition}, _CbConfig) ->
    {ok, #{topic => Topic, partition => Partition}}.

%% Returns commit only after the outcome is published. If anything crashes
%% first, the offset is not committed and the message is redelivered.
-spec handle_message(brod:message(), state()) -> {ok, commit, state()}.
handle_message(#kafka_message{offset = Offset, key = Key, value = Value},
               #{partition := Partition} = State) ->
    Meta = #{order_id => Key, partition => Partition, offset => Offset},
    ok = handle(order_codec:decode(Value), Value, Meta),
    {ok, commit, State}.

handle({ok, Order}, Value, Meta) ->
    route(order_processor:process(Order), Order, Value, Meta);
handle({error, Reason}, Value, Meta) ->
    %% A poison message must not stall the partition: dead-letter and commit.
    FailedAt = order_clock:now_rfc3339(),
    dead_letter(order_codec:encode_dead_letter_raw(Value, Reason, FailedAt), Reason, Meta).

route({ok, _Receipt}, _Order, Value, #{order_id := Key} = Meta) ->
    ok = order_producer:publish(?TOPIC_COMPLETED, Key, Value),
    ?LOG_INFO(Meta#{event => order_completed});
route({error, Class, Reason}, Order, _Value, Meta) ->
    %% Until retry_manager exists (phase 5), retryable failures are
    %% dead-lettered too instead of going to orders.retry.
    FailedAt = order_clock:now_rfc3339(),
    dead_letter(order_codec:encode_dead_letter(Order, Reason, FailedAt), Reason,
                Meta#{class => Class}).

dead_letter(Payload, Reason, #{order_id := Key} = Meta) ->
    ok = order_producer:publish(?TOPIC_DLQ, Key, Payload),
    ?LOG_ERROR(Meta#{event => order_dead_lettered, reason => Reason}).

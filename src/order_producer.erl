-module(order_producer).

%% The only module that publishes to Kafka. Tests replace it with meck.

-export([start_link/0, publish/3, client/0]).

-define(CLIENT, order_producer_client).
-define(DEFAULT_ENDPOINTS, [{"localhost", 9092}]).
-define(DEFAULT_PRODUCE_TIMEOUT_MS, 5000).

%% Starts the brod client. brod_client exits during init if no bootstrap
%% endpoint is reachable, so the app needs Kafka up at boot.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    Endpoints = application:get_env(order_app, kafka_endpoints, ?DEFAULT_ENDPOINTS),
    brod:start_link_client(Endpoints, ?CLIENT,
                           [{auto_start_producers, true},
                            {default_producer_config, []}]).

%% The brod client is shared with consumer_worker's group subscriber.
-spec client() -> atom().
client() ->
    ?CLIENT.

%% Synchronous publish, partitioned by hashing Key so one order always lands
%% on the same partition. Returns once the broker acknowledges the write.
-spec publish(Topic :: binary(), Key :: binary(), Value :: binary()) ->
          ok | {error, term()}.
publish(Topic, Key, Value) ->
    Timeout = application:get_env(order_app, produce_timeout_ms,
                                  ?DEFAULT_PRODUCE_TIMEOUT_MS),
    await_ack(brod:produce(?CLIENT, Topic, hash, Key, Value), Timeout).

await_ack({ok, CallRef}, Timeout) ->
    brod:sync_produce_request(CallRef, Timeout);
await_ack({error, _} = Error, _Timeout) ->
    Error.

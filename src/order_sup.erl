-module(order_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(HTTP_LISTENER, order_http).
-define(DEFAULT_HTTP_PORT, 8080).
-define(DEFAULT_PRODUCER_SHUTDOWN_MS, 10000).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    %% one_for_one: HTTP, producer, consumers and retry manager fail independently;
    %% 5 restarts in 10 s lets transient faults heal but escalates a crash loop.
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    %% Children stop in reverse start order, so on shutdown HTTP stops first,
    %% then consumers, then the producer both of them depend on.
    {ok, {SupFlags, [producer_spec(), consumer_spec(), http_spec()]}}.

producer_spec() ->
    #{id => producer_sup,
      start => {order_producer, start_link, []},
      restart => permanent,
      shutdown => application:get_env(order_app, producer_shutdown_ms,
                                      ?DEFAULT_PRODUCER_SHUTDOWN_MS),
      type => worker}.

consumer_spec() ->
    #{id => consumer_sup,
      start => {consumer_sup, start_link, []},
      restart => permanent,
      %% consumer_sup bounds its own child's shutdown time.
      shutdown => infinity,
      type => supervisor}.

http_spec() ->
    Port = application:get_env(order_app, http_port, ?DEFAULT_HTTP_PORT),
    Dispatch = cowboy_router:compile(
                 [{'_', [{"/orders", order_http_handler, orders},
                         {"/orders/:id", order_http_handler, order},
                         {"/health", order_http_handler, health}]}]),
    Spec = ranch:child_spec(?HTTP_LISTENER, ranch_tcp,
                            #{socket_opts => [{port, Port}],
                              connection_type => supervisor},
                            cowboy_clear,
                            #{env => #{dispatch => Dispatch},
                              connection_type => supervisor}),
    Spec#{id := http_sup}.

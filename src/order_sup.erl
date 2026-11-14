-module(order_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    %% one_for_one: HTTP, producer, consumers and retry manager fail independently;
    %% 5 restarts in 10 s lets transient faults heal but escalates a crash loop.
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 10},
    %% Children are added phase by phase (HTTP and producer in phase 2).
    {ok, {SupFlags, []}}.

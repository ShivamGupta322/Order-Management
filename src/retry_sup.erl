-module(retry_sup).
-behaviour(supervisor).

%% Supervises retry_manager and the consumer group that reads orders.retry.

-export([start_link/0, subscriber/0]).
-export([init/1]).

-define(DEFAULT_SHUTDOWN_MS, 10000).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% The running retry group subscriber, if any.
-spec subscriber() -> {ok, pid()} | {error, not_running}.
subscriber() ->
    running([Pid || {retry_group, Pid, _, _} <- supervisor:which_children(?MODULE),
                    is_pid(Pid)]).

running([Pid]) -> {ok, Pid};
running([]) -> {error, not_running}.

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    %% one_for_all: retry_manager's queues hold the retry group's uncommitted
    %% messages, so if either dies both restart and the group re-reads from its
    %% last commit. 5 restarts in 30 s, then escalate to order_sup.
    SupFlags = #{strategy => one_for_all, intensity => 5, period => 30},
    Shutdown = application:get_env(order_app, consumer_shutdown_ms, ?DEFAULT_SHUTDOWN_MS),
    %% retry_manager starts first because the group's workers call it.
    Children = [#{id => retry_manager,
                  start => {retry_manager, start_link, []},
                  restart => permanent,
                  shutdown => Shutdown,
                  type => worker},
                #{id => retry_group,
                  start => {consumer_worker, start_link, [retry]},
                  restart => permanent,
                  shutdown => Shutdown,
                  type => worker}],
    {ok, {SupFlags, Children}}.

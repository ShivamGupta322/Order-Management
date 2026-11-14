-module(consumer_sup).
-behaviour(supervisor).

%% Supervises the brod group subscriber, which runs one consumer_worker
%% callback process per assigned partition. If any partition worker crashes,
%% the subscriber flushes acked offsets and stops; this supervisor restarts it,
%% and every partition resumes from its last committed offset.

-export([start_link/0, subscriber/0]).
-export([init/1]).

-define(DEFAULT_SHUTDOWN_MS, 10000).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% The running group subscriber, if any (absent while it is restarting).
-spec subscriber() -> {ok, pid()} | {error, not_running}.
subscriber() ->
    running([Pid || {consumer_group, Pid, _, _} <- supervisor:which_children(?MODULE),
                    is_pid(Pid)]).

running([Pid]) -> {ok, Pid};
running([]) -> {error, not_running}.

-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([]) ->
    %% one_for_one over a single child. 5 restarts in 30 s rides out a crashing
    %% order or a broker blip; a tighter loop escalates to order_sup.
    SupFlags = #{strategy => one_for_one, intensity => 5, period => 30},
    %% The subscriber finishes each worker's in-flight message and flushes
    %% offset commits in terminate/2, so it needs time, never brutal_kill.
    Shutdown = application:get_env(order_app, consumer_shutdown_ms, ?DEFAULT_SHUTDOWN_MS),
    Child = #{id => consumer_group,
              start => {consumer_worker, start_link, [orders]},
              restart => permanent,
              shutdown => Shutdown,
              type => worker},
    {ok, {SupFlags, [Child]}}.

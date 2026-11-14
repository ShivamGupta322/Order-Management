-module(order_app).
-behaviour(application).

-include_lib("kernel/include/logger.hrl").

-export([start/2, stop/1]).

-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    %% Tables must be loaded before any worker can read or write them.
    ok = order_mnesia:init(),
    {ok, Pid} = order_sup:start_link(),
    ?LOG_INFO(#{event => order_app_started}),
    {ok, Pid}.

-spec stop(term()) -> ok.
stop(_State) ->
    ok.

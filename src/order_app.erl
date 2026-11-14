-module(order_app).
-behaviour(application).

-include_lib("kernel/include/logger.hrl").

-export([start/2, stop/1]).

-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    {ok, Pid} = order_sup:start_link(),
    ?LOG_INFO(#{event => order_app_started}),
    {ok, Pid}.

-spec stop(term()) -> ok.
stop(_State) ->
    ok.

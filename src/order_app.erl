-module(order_app).
-behaviour(application).

-include_lib("kernel/include/logger.hrl").

-export([start/2, stop/1]).

-spec start(application:start_type(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _StartArgs) ->
    ok = configure_logging(application:get_env(order_app, log_format, text)),
    ok = order_metrics:declare(),
    %% Tables must be loaded before any worker can read or write them.
    ok = order_mnesia:init(),
    {ok, Pid} = order_sup:start_link(),
    ?LOG_INFO(#{event => order_app_started}),
    {ok, Pid}.

-spec stop(term()) -> ok.
stop(_State) ->
    ok.

%% JSON lines for the release; the default text format keeps test logs readable.
configure_logging(json) ->
    logger:set_handler_config(default, formatter, {order_log_formatter, #{}});
configure_logging(text) ->
    ok.

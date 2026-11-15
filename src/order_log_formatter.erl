-module(order_log_formatter).

%% A logger formatter that writes one JSON object per line. Report maps (how
%% this app logs: ?LOG_INFO(#{event => ..., order_id => ...})) become
%% top-level fields, so order_id, partition and offset are searchable.
%% Uses OTP's json module; no dependency.
%%
%% Pure: takes a log event, returns iodata. Enabled at boot when the
%% `log_format' app env is `json'.

-export([format/2, check_config/1]).

-spec format(logger:log_event(), logger:formatter_config()) -> unicode:chardata().
format(#{level := Level, msg := Msg, meta := Meta}, _Config) ->
    Base = #{<<"time">> => time(Meta), <<"level">> => atom_to_binary(Level)},
    Event = maps:merge(Base, maps:merge(location(Meta), message(Msg))),
    [json:encode(Event), $\n].

-spec check_config(logger:formatter_config()) -> ok.
check_config(_Config) ->
    ok.

%%--------------------------------------------------------------------

time(#{time := Micros}) ->
    list_to_binary(calendar:system_time_to_rfc3339(Micros, [{unit, microsecond},
                                                            {offset, "Z"}]));
time(_Meta) ->
    null.

location(#{mfa := {M, F, A}}) ->
    #{<<"mfa">> => iolist_to_binary(io_lib:format("~ts:~ts/~b", [M, F, A]))};
location(_Meta) ->
    #{}.

message({report, Report}) when is_map(Report) ->
    maps:from_list([{key(K), value(V)} || {K, V} <- maps:to_list(Report)]);
message({report, Report}) when is_list(Report) ->
    message({report, maps:from_list(Report)});
message({string, String}) ->
    #{<<"msg">> => unicode:characters_to_binary(String)};
message({Format, Args}) ->
    #{<<"msg">> => unicode:characters_to_binary(io_lib:format(Format, Args))}.

key(K) when is_atom(K) -> atom_to_binary(K);
key(K) when is_binary(K) -> K;
key(K) -> printed(K).

%% Anything JSON cannot hold directly (pids, tuples, non-UTF-8 binaries, ...)
%% is printed as Erlang text rather than dropped.
value(V) when is_integer(V); is_float(V); is_boolean(V); V =:= null -> V;
value(V) when is_atom(V) -> atom_to_binary(V);
value(V) when is_binary(V) -> utf8_or_printed(unicode:characters_to_binary(V), V);
value(V) when is_map(V) -> maps:from_list([{key(K), value(X)} || {K, X} <- maps:to_list(V)]);
value(V) -> printed(V).

utf8_or_printed(Utf8, _V) when is_binary(Utf8) -> Utf8;
utf8_or_printed(_Invalid, V) -> printed(V).

printed(V) ->
    unicode:characters_to_binary(io_lib:format("~0tp", [V])).

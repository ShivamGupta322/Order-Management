-module(order_clock).

%% The one place the app reads wall-clock time, so tests can mock it.

-export([now_rfc3339/0, now_ms/0]).

-spec now_rfc3339() -> binary().
now_rfc3339() ->
    list_to_binary(calendar:system_time_to_rfc3339(erlang:system_time(second),
                                                   [{offset, "Z"}])).

-spec now_ms() -> integer().
now_ms() ->
    erlang:system_time(millisecond).

-module(retry_policy).

%% Backoff schedule and dead-letter decision. Pure: no processes, no I/O.

-export([next/1, next/2, default_delays/0]).

-define(DEFAULT_DELAYS_MS, [1000, 5000, 30000]).

%% `Attempt' is the attempt that just failed, starting at 0.
-spec next(non_neg_integer()) -> {retry, pos_integer()} | dead_letter.
next(Attempt) ->
    next(Attempt, ?DEFAULT_DELAYS_MS).

%% One delay per allowed retry; once they are used up, dead-letter.
-spec next(non_neg_integer(), [pos_integer()]) -> {retry, pos_integer()} | dead_letter.
next(Attempt, Delays) when is_integer(Attempt), Attempt >= 0, Attempt < length(Delays) ->
    {retry, lists:nth(Attempt + 1, Delays)};
next(Attempt, Delays) when is_integer(Attempt), Attempt >= 0, is_list(Delays) ->
    dead_letter.

-spec default_delays() -> [pos_integer()].
default_delays() ->
    ?DEFAULT_DELAYS_MS.

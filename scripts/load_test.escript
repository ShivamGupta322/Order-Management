#!/usr/bin/env escript
%% Load test: posts N orders from CONCURRENCY sender processes over kept-alive
%% HTTP connections, then waits until the app has processed them all, read
%% from its own orders_processed_total metric.
%%
%%   escript scripts/load_test.escript [N] [CONCURRENCY] [BASE_URL]
%%
%% Run it inside the compose network to measure the app rather than Docker
%% Desktop's port forwarding:
%%   docker compose run --rm --no-deps app escript scripts/load_test.escript 5000 50 http://orders:8080
%%
%% Gateway timeouts are retried after 1 s, 5 s and 30 s, so with the default
%% failure rates the last orders finish up to ~36 s late. For raw throughput,
%% set the rates to 0 first (see the README).

-define(TIMEOUT_MS, 300000).

main(Args) ->
    {N, Concurrency, BaseUrl} = args(Args),
    {ok, _} = application:ensure_all_started(inets),
    ok = httpc:set_options([{max_sessions, Concurrency}, {max_keep_alive_length, N},
                            {keep_alive_timeout, 60000}]),
    Run = integer_to_list(erlang:system_time(millisecond)),
    Before = processed(BaseUrl),
    Start = now_ms(),
    Codes = send_all(N, Concurrency, BaseUrl, Run),
    Posted = now_ms(),
    ok = wait_processed(BaseUrl, Before + N, Start),
    Done = now_ms(),
    report(N, Concurrency, Codes, Posted - Start, Done - Start).

args([]) -> args(["1000"]);
args([N]) -> args([N, "20"]);
args([N, C]) -> args([N, C, "http://localhost:8080"]);
args([N, C, Url]) -> {list_to_integer(N), list_to_integer(C), Url}.

%% Splits N orders across Concurrency senders; returns status code counts.
send_all(N, Concurrency, BaseUrl, Run) ->
    Self = self(),
    Senders = [spawn_monitor(fun() -> Self ! {codes, self(), send(Ids, BaseUrl, Run, #{})} end)
               || Ids <- split(lists:seq(1, N), Concurrency)],
    lists:foldl(fun({Pid, Ref}, Acc) -> merge(collect(Pid, Ref), Acc) end, #{}, Senders).

collect(Pid, Ref) ->
    receive
        {codes, Pid, Codes} -> erlang:demonitor(Ref, [flush]), Codes;
        {'DOWN', Ref, process, Pid, Reason} -> #{{crashed, Reason} => 1}
    end.

send([], _BaseUrl, _Run, Codes) ->
    Codes;
send([I | Rest], BaseUrl, Run, Codes) ->
    Body = io_lib:format("{\"id\":\"load-~s-~b\",\"customer_id\":\"c-load\",\"amount\":1000,"
                         "\"currency\":\"INR\",\"items\":[{\"sku\":\"SKU-1\",\"qty\":1}]}", [Run, I]),
    Code = status(httpc:request(post, {BaseUrl ++ "/orders", [], "application/json",
                                       lists:flatten(Body)}, [], [])),
    send(Rest, BaseUrl, Run, maps:update_with(Code, fun(X) -> X + 1 end, 1, Codes)).

status({ok, {{_, Code, _}, _, _}}) -> Code;
status({error, Reason}) -> {error, Reason}.

split(List, Parts) ->
    Size = max(1, (length(List) + Parts - 1) div Parts),
    chunks(List, Size).

chunks([], _Size) -> [];
chunks(List, Size) when length(List) =< Size -> [List];
chunks(List, Size) -> {Chunk, Rest} = lists:split(Size, List), [Chunk | chunks(Rest, Size)].

merge(A, B) -> maps:fold(fun(K, V, Acc) -> maps:update_with(K, fun(X) -> X + V end, V, Acc) end, B, A).

%% Sum of orders_processed_total over all results.
processed(BaseUrl) ->
    {ok, {{_, 200, _}, _, Body}} = httpc:request(get, {BaseUrl ++ "/metrics", []}, [], []),
    lists:sum([list_to_number(lists:last(string:lexemes(Line, " ")))
               || Line <- string:lexemes(Body, "\n"),
                  lists:prefix("orders_processed_total{", Line)]).

list_to_number(S) ->
    try list_to_integer(S) catch error:badarg -> round(list_to_float(S)) end.

wait_processed(BaseUrl, Target, Start) ->
    waiting(processed(BaseUrl) >= Target, now_ms() - Start > ?TIMEOUT_MS, BaseUrl, Target, Start).

waiting(true, _TimedOut, _BaseUrl, _Target, _Start) -> ok;
waiting(false, true, _BaseUrl, _Target, _Start) -> erlang:error(timeout);
waiting(false, false, BaseUrl, Target, Start) ->
    receive after 100 -> wait_processed(BaseUrl, Target, Start) end.

report(N, Concurrency, Codes, PostMs, TotalMs) ->
    io:format("orders:            ~b (concurrency ~b)~n", [N, Concurrency]),
    io:format("HTTP status codes: ~p~n", [Codes]),
    io:format("accepted in:       ~b ms (~b orders/s)~n", [PostMs, rate(N, PostMs)]),
    io:format("all processed in:  ~b ms (~b orders/s end to end)~n", [TotalMs, rate(N, TotalMs)]).

rate(N, Ms) -> N * 1000 div max(1, Ms).

now_ms() -> erlang:monotonic_time(millisecond).

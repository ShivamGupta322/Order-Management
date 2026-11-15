-module(idempotency_store).

%% Order IDs that have reached a final outcome, so a redelivered or re-posted
%% message for them is skipped. Looked up on every message, so it is a plain
%% module over a Mnesia table: no process to serialise reads behind.

-include("order_tables.hrl").

-export([seen/1, mark/1]).

-spec seen(binary()) -> boolean().
seen(Id) when is_binary(Id) ->
    mnesia:dirty_read(processed_order, Id) =/= [].

%% Marking twice is harmless: the second write replaces the first.
-spec mark(binary()) -> ok.
mark(Id) when is_binary(Id) ->
    Row = #processed_order{id = Id, processed_at = order_clock:now_ms()},
    {atomic, ok} = mnesia:transaction(fun() -> mnesia:write(Row) end),
    ok.

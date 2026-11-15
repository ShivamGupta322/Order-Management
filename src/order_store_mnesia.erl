-module(order_store_mnesia).
-behaviour(order_store).

%% Mnesia implementation of order_store.

-include("order_tables.hrl").

-export([save/1, get/1, update_status/2]).

%% Only validated orders are saved, so missing fields are a bug and crash here.
%% Insert-if-absent, like order_store_pg: the first final outcome wins and a
%% redelivered one is a no-op.
-spec save(map()) -> ok | {error, term()}.
save(#{id := Id, status := Status} = Order) ->
    Row = #order{id = Id,
                 status = Status,
                 customer_id = maps:get(customer_id, Order),
                 amount = maps:get(amount, Order),
                 currency = maps:get(currency, Order),
                 items = maps:get(items, Order),
                 attempt = maps:get(attempt, Order, 0),
                 created_at = maps:get(created_at, Order, undefined),
                 updated_at = order_clock:now_ms()},
    transaction(fun() -> insert_new(mnesia:read(order, Id, write), Row) end).

insert_new([], Row) -> mnesia:write(Row);
insert_new([_Existing], _Row) -> ok.

-spec get(binary()) -> {ok, map()} | {error, not_found}.
get(Id) ->
    to_result(mnesia:dirty_read(order, Id)).

-spec update_status(binary(), order_store:status()) -> ok | {error, term()}.
update_status(Id, Status) ->
    transaction(fun() -> set_status(mnesia:read(order, Id, write), Status) end).

set_status([Row], Status) ->
    mnesia:write(Row#order{status = Status, updated_at = order_clock:now_ms()});
set_status([], _Status) ->
    mnesia:abort(not_found).

transaction(Fun) ->
    transaction_result(mnesia:transaction(Fun)).

transaction_result({atomic, ok}) -> ok;
transaction_result({aborted, Reason}) -> {error, Reason}.

to_result([Row]) -> {ok, to_map(Row)};
to_result([]) -> {error, not_found}.

to_map(#order{} = Row) ->
    Order = #{id => Row#order.id,
              status => Row#order.status,
              customer_id => Row#order.customer_id,
              amount => Row#order.amount,
              currency => Row#order.currency,
              items => Row#order.items,
              attempt => Row#order.attempt,
              updated_at => Row#order.updated_at},
    with_created_at(Row#order.created_at, Order).

with_created_at(undefined, Order) -> Order;
with_created_at(CreatedAt, Order) -> Order#{created_at => CreatedAt}.

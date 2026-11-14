-module(order_validator).

%% Field and amount checks for order fields. Pure: no processes, no I/O.
%% `attempt` is optional (the server sets it), but when present it must be a
%% non-negative integer: a bad one would crash the retry decision on every
%% redelivery and stall the partition.

-export([validate/1]).
-export_type([field/0, reason/0]).

-type field() :: id | customer_id | amount | currency | items | attempt.
-type reason() :: {missing | invalid, field()}.

-define(CURRENCIES, [<<"INR">>, <<"USD">>, <<"EUR">>, <<"GBP">>]).

%% Fields are checked in this order and the first failure is returned.
-define(FIELDS, [id, amount, customer_id, currency, items]).

-spec validate(map()) -> {ok, order_codec:order()} | {error, reason()}.
validate(Order) when is_map(Order) ->
    check(Order, ?FIELDS).

check(Order, []) ->
    check_attempt(maps:find(attempt, Order), Order);
check(Order, [Field | Rest]) ->
    check_field(Field, maps:find(Field, Order), Order, Rest).

check_field(Field, error, _Order, _Rest) ->
    {error, {missing, Field}};
check_field(Field, {ok, Value}, Order, Rest) ->
    case valid(Field, Value) of
        true -> check(Order, Rest);
        false -> {error, {invalid, Field}}
    end.

check_attempt(error, Order) ->
    {ok, Order};
check_attempt({ok, Attempt}, Order) when is_integer(Attempt), Attempt >= 0 ->
    {ok, Order};
check_attempt({ok, _Invalid}, _Order) ->
    {error, {invalid, attempt}}.

valid(id, Value) -> non_empty_binary(Value);
valid(customer_id, Value) -> non_empty_binary(Value);
valid(amount, Value) -> pos_integer(Value);
valid(currency, Value) -> lists:member(Value, ?CURRENCIES);
valid(items, [_ | _] = Items) -> lists:all(fun valid_item/1, Items);
valid(items, _) -> false.

valid_item(#{sku := Sku, qty := Qty}) ->
    non_empty_binary(Sku) andalso pos_integer(Qty);
valid_item(_) ->
    false.

non_empty_binary(Value) -> is_binary(Value) andalso byte_size(Value) > 0.

pos_integer(Value) -> is_integer(Value) andalso Value > 0.

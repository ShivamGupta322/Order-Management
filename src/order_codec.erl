-module(order_codec).

%% JSON encode/decode of order events. Pure: no processes, no I/O.

-export([encode/1, decode/1, encode_retry/2, decode_retry/1,
         encode_dead_letter/3, encode_dead_letter_raw/3]).
-export_type([order/0, item/0, decode_error/0]).

-type item() :: #{sku => binary(), qty => integer()}.
-type order() :: #{id => binary(),
                   customer_id => binary(),
                   amount => integer(),
                   currency => binary(),
                   items => [item()],
                   attempt => non_neg_integer(),
                   created_at => binary()}.
-type decode_error() :: malformed_json | not_an_object | invalid_not_before.
-type reason() :: atom() | {atom(), atom()}.

%% Known JSON keys and the atoms they map to. Only these become atoms, so
%% external input can never create new atoms; anything else is dropped.
-define(ORDER_FIELDS, [{<<"id">>, id},
                       {<<"customer_id">>, customer_id},
                       {<<"amount">>, amount},
                       {<<"currency">>, currency},
                       {<<"items">>, items},
                       {<<"attempt">>, attempt},
                       {<<"created_at">>, created_at}]).
-define(ITEM_FIELDS, [{<<"sku">>, sku}, {<<"qty">>, qty}]).

-spec encode(order()) -> binary().
encode(Order) ->
    jsx:encode(Order).

%% A retry message is the order plus `not_before': the earliest time, in Unix
%% milliseconds, it may go back on `orders'.
-spec encode_retry(order(), NotBefore :: integer()) -> binary().
encode_retry(Order, NotBefore) when is_integer(NotBefore) ->
    jsx:encode(Order#{not_before => NotBefore}).

-spec decode_retry(binary()) -> {ok, order(), NotBefore :: integer()} | {error, decode_error()}.
decode_retry(Json) when is_binary(Json) ->
    retry_from_json(parse(Json)).

retry_from_json({ok, #{<<"not_before">> := NotBefore} = Map}) when is_integer(NotBefore) ->
    {ok, Order} = from_json({ok, Map}),
    {ok, Order, NotBefore};
retry_from_json({ok, Map}) when is_map(Map) ->
    {error, invalid_not_before};
retry_from_json(Other) ->
    from_json(Other).

%% A DLQ message is the original order plus why and when it failed.
-spec encode_dead_letter(map(), reason(), FailedAt :: binary()) -> binary().
encode_dead_letter(Order, Reason, FailedAt) ->
    jsx:encode(Order#{attempt => maps:get(attempt, Order, 0),
                      reason => reason_to_binary(Reason),
                      failed_at => FailedAt}).

%% An undecodable message has no order to carry, so the raw bytes travel
%% base64-encoded: they may not be valid UTF-8, which JSON strings require.
-spec encode_dead_letter_raw(binary(), reason(), FailedAt :: binary()) -> binary().
encode_dead_letter_raw(Raw, Reason, FailedAt) ->
    jsx:encode(#{raw_base64 => base64:encode(Raw),
                 reason => reason_to_binary(Reason),
                 failed_at => FailedAt}).

reason_to_binary(Reason) when is_atom(Reason) ->
    atom_to_binary(Reason);
reason_to_binary({Kind, Field}) when is_atom(Kind), is_atom(Field) ->
    <<(atom_to_binary(Kind))/binary, ":", (atom_to_binary(Field))/binary>>.

%% Decoding only maps structure; field values are checked by order_validator.
-spec decode(binary()) -> {ok, order()} | {error, decode_error()}.
decode(Json) when is_binary(Json) ->
    from_json(parse(Json)).

parse(Json) ->
    try
        {ok, jsx:decode(Json, [return_maps])}
    catch
        error:badarg -> {error, malformed_json}
    end.

from_json({ok, Map}) when is_map(Map) ->
    {ok, decode_items(pick(Map, ?ORDER_FIELDS))};
from_json({ok, _NotAnObject}) ->
    {error, not_an_object};
from_json({error, _} = Error) ->
    Error.

decode_items(#{items := Items} = Order) when is_list(Items) ->
    Order#{items := [decode_item(Item) || Item <- Items]};
decode_items(Order) ->
    Order.

decode_item(Item) when is_map(Item) ->
    pick(Item, ?ITEM_FIELDS);
decode_item(Other) ->
    Other.

pick(Json, Fields) ->
    maps:from_list([{Key, Value} || {Name, Key} <- Fields,
                                    {ok, Value} <- [maps:find(Name, Json)]]).

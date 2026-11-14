-module(order_codec).

%% JSON encode/decode of order events. Pure: no processes, no I/O.

-export([encode/1, decode/1]).
-export_type([order/0, item/0, decode_error/0]).

-type item() :: #{sku => binary(), qty => integer()}.
-type order() :: #{id => binary(),
                   customer_id => binary(),
                   amount => integer(),
                   currency => binary(),
                   items => [item()],
                   attempt => non_neg_integer(),
                   created_at => binary()}.
-type decode_error() :: malformed_json | not_an_object.

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

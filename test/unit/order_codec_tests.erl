-module(order_codec_tests).
%% proper.hrl must come first: eunit.hrl skips its own ?LET when one exists.
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

full_event() ->
    #{id => <<"o-1001">>,
      customer_id => <<"c-42">>,
      amount => 1500,
      currency => <<"INR">>,
      items => [#{sku => <<"SKU-1">>, qty => 2}],
      attempt => 0,
      created_at => <<"2026-11-14T10:00:00Z">>}.

%% From the implementation guide.
codec_roundtrip_test() ->
    Order = #{id => <<"o-1">>, amount => 1500, currency => <<"INR">>},
    ?assertEqual({ok, Order}, order_codec:decode(order_codec:encode(Order))).

full_event_roundtrip_test() ->
    ?assertEqual({ok, full_event()}, order_codec:decode(order_codec:encode(full_event()))).

amount_stays_integer_test() ->
    {ok, #{amount := Amount}} = order_codec:decode(<<"{\"amount\":1500}">>),
    ?assert(is_integer(Amount)).

malformed_json_test_() ->
    [?_assertEqual({error, malformed_json}, order_codec:decode(Json))
     || Json <- [<<>>, <<"{">>, <<"{\"id\":}">>, <<"not json">>, <<"{\"id\":\"o-1\"} trailing">>]].

not_an_object_test_() ->
    [?_assertEqual({error, not_an_object}, order_codec:decode(Json))
     || Json <- [<<"[1,2]">>, <<"42">>, <<"\"o-1\"">>, <<"null">>]].

unknown_fields_ignored_test() ->
    Json = <<"{\"id\":\"o-1\",\"total\":99,\"note\":\"x\","
             "\"items\":[{\"sku\":\"S\",\"qty\":1,\"colour\":\"red\"}]}">>,
    ?assertEqual({ok, #{id => <<"o-1">>, items => [#{sku => <<"S">>, qty => 1}]}},
                 order_codec:decode(Json)).

%% Unknown keys must never become atoms (atoms are not garbage collected).
unknown_keys_do_not_create_atoms_test() ->
    Key = <<"order_codec_tests_never_an_atom">>,
    {ok, _} = order_codec:decode(<<"{\"", Key/binary, "\":1}">>),
    ?assertError(badarg, binary_to_existing_atom(Key, utf8)).

non_map_items_kept_for_validator_test() ->
    ?assertEqual({ok, #{items => [<<"SKU-1">>, 3]}},
                 order_codec:decode(<<"{\"items\":[\"SKU-1\",3]}">>)).

roundtrip_property_test() ->
    ?assert(proper:quickcheck(prop_roundtrip(), [{numtests, 300}, {to_file, user}])).

prop_roundtrip() ->
    ?FORALL(Order, order(),
            order_codec:decode(order_codec:encode(Order)) =:= {ok, Order}).

order() ->
    ?LET({Id, CustomerId, Amount, Currency, Items, Attempt, CreatedAt},
         {utf8(), utf8(), integer(), utf8(), list(item()), non_neg_integer(), utf8()},
         #{id => Id, customer_id => CustomerId, amount => Amount, currency => Currency,
           items => Items, attempt => Attempt, created_at => CreatedAt}).

item() ->
    ?LET({Sku, Qty}, {utf8(), integer()}, #{sku => Sku, qty => Qty}).

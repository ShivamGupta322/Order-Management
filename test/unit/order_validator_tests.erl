-module(order_validator_tests).
-include_lib("eunit/include/eunit.hrl").

valid_order() ->
    #{id => <<"o-1001">>,
      customer_id => <<"c-42">>,
      amount => 1500,
      currency => <<"INR">>,
      items => [#{sku => <<"SKU-1">>, qty => 2}]}.

valid_order_test() ->
    Order = valid_order(),
    ?assertEqual({ok, Order}, order_validator:validate(Order)).

%% From the implementation guide: amount is checked before customer_id.
invalid_amount_test() ->
    ?assertMatch({error, {invalid, amount}},
                 order_validator:validate(#{id => <<"o-2">>, amount => -5})).

missing_fields_test_() ->
    [?_assertEqual({error, {missing, Field}},
                   order_validator:validate(maps:remove(Field, valid_order())))
     || Field <- [id, customer_id, amount, currency, items]].

amount_test_() ->
    Validate = fun(Amount) -> order_validator:validate((valid_order())#{amount => Amount}) end,
    [?_assertEqual({error, {invalid, amount}}, Validate(0)),
     ?_assertEqual({error, {invalid, amount}}, Validate(-1)),
     ?_assertEqual({error, {invalid, amount}}, Validate(15.0)),
     ?_assertEqual({error, {invalid, amount}}, Validate(<<"1500">>)),
     ?_assertMatch({ok, _}, Validate(1))].

currency_test_() ->
    Validate = fun(Currency) -> order_validator:validate((valid_order())#{currency => Currency}) end,
    [?_assertEqual({error, {invalid, currency}}, Validate(<<"XYZ">>)),
     ?_assertEqual({error, {invalid, currency}}, Validate(<<"inr">>)),
     ?_assertEqual({error, {invalid, currency}}, Validate(42)),
     ?_assertMatch({ok, _}, Validate(<<"USD">>))].

id_test_() ->
    Validate = fun(Key, Value) -> order_validator:validate((valid_order())#{Key => Value}) end,
    [?_assertEqual({error, {invalid, id}}, Validate(id, <<>>)),
     ?_assertEqual({error, {invalid, id}}, Validate(id, 1001)),
     ?_assertEqual({error, {invalid, customer_id}}, Validate(customer_id, <<>>)),
     ?_assertEqual({error, {invalid, customer_id}}, Validate(customer_id, null))].

items_test_() ->
    Validate = fun(Items) -> order_validator:validate((valid_order())#{items => Items}) end,
    [?_assertEqual({error, {invalid, items}}, Validate([])),
     ?_assertEqual({error, {invalid, items}}, Validate(not_a_list)),
     ?_assertEqual({error, {invalid, items}}, Validate([#{sku => <<"S">>, qty => 0}])),
     ?_assertEqual({error, {invalid, items}}, Validate([#{sku => <<"S">>, qty => 1.5}])),
     ?_assertEqual({error, {invalid, items}}, Validate([#{sku => <<>>, qty => 1}])),
     ?_assertEqual({error, {invalid, items}}, Validate([#{qty => 1}])),
     ?_assertEqual({error, {invalid, items}}, Validate([<<"SKU-1">>])),
     ?_assertEqual({error, {invalid, items}},
                   Validate([#{sku => <<"S">>, qty => 1}, #{sku => <<"T">>, qty => -1}])),
     ?_assertMatch({ok, _}, Validate([#{sku => <<"S">>, qty => 1}, #{sku => <<"T">>, qty => 3}]))].

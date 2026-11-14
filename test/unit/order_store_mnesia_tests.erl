-module(order_store_mnesia_tests).
-include_lib("eunit/include/eunit.hrl").

store_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun save_then_get_roundtrips/1,
      fun get_unknown_is_not_found/1,
      fun save_overwrites/1,
      fun update_status/1,
      fun update_status_unknown_is_not_found/1,
      fun created_at_is_optional/1]}.

setup() ->
    ok = application:set_env(order_app, mnesia_copies, ram_copies),
    ok = order_mnesia:init().

cleanup(_) ->
    stopped = mnesia:stop(),
    application:unset_env(order_app, mnesia_copies).

order() ->
    #{id => <<"o-1">>, status => completed, customer_id => <<"c-42">>,
      amount => 1500, currency => <<"INR">>,
      items => [#{sku => <<"SKU-1">>, qty => 2}],
      attempt => 1, created_at => <<"2026-11-14T10:00:00Z">>}.

save_then_get_roundtrips(_) ->
    ok = order_store_mnesia:save(order()),
    {ok, Stored} = order_store_mnesia:get(<<"o-1">>),
    [?_assertEqual(order(), maps:without([updated_at], Stored)),
     ?_assert(is_integer(maps:get(updated_at, Stored)))].

get_unknown_is_not_found(_) ->
    ?_assertEqual({error, not_found}, order_store_mnesia:get(<<"nope">>)).

save_overwrites(_) ->
    ok = order_store_mnesia:save((order())#{status => failed}),
    ok = order_store_mnesia:save(order()),
    ?_assertMatch({ok, #{status := completed}}, order_store_mnesia:get(<<"o-1">>)).

update_status(_) ->
    ok = order_store_mnesia:save(order()),
    ok = order_store_mnesia:update_status(<<"o-1">>, failed),
    ?_assertMatch({ok, #{status := failed, amount := 1500}}, order_store_mnesia:get(<<"o-1">>)).

update_status_unknown_is_not_found(_) ->
    ?_assertEqual({error, not_found}, order_store_mnesia:update_status(<<"nope">>, failed)).

created_at_is_optional(_) ->
    ok = order_store_mnesia:save(maps:remove(created_at, order())),
    {ok, Stored} = order_store_mnesia:get(<<"o-1">>),
    ?_assertNot(maps:is_key(created_at, Stored)).

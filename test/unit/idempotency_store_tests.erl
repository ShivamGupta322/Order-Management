-module(idempotency_store_tests).
-include_lib("eunit/include/eunit.hrl").

store_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun unknown_id_is_not_seen/1,
      fun marked_id_is_seen/1,
      fun marking_twice_is_harmless/1,
      fun ids_are_independent/1]}.

setup() ->
    ok = application:set_env(order_app, mnesia_copies, ram_copies),
    ok = order_mnesia:init().

cleanup(_) ->
    stopped = mnesia:stop(),
    application:unset_env(order_app, mnesia_copies).

unknown_id_is_not_seen(_) ->
    ?_assertNot(idempotency_store:seen(<<"o-1">>)).

marked_id_is_seen(_) ->
    ok = idempotency_store:mark(<<"o-1">>),
    ?_assert(idempotency_store:seen(<<"o-1">>)).

marking_twice_is_harmless(_) ->
    ok = idempotency_store:mark(<<"o-1">>),
    ok = idempotency_store:mark(<<"o-1">>),
    ?_assert(idempotency_store:seen(<<"o-1">>)).

ids_are_independent(_) ->
    ok = idempotency_store:mark(<<"o-1">>),
    ?_assertNot(idempotency_store:seen(<<"o-2">>)).

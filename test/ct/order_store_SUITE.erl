-module(order_store_SUITE).

%% The order_store contract, run against both implementations through the
%% order_store API, selected with `store_impl' exactly as the app does. The
%% PostgreSQL group uses its own `orders_test' database, so it never touches
%% the live app's data, and skips itself if PostgreSQL is unreachable:
%%   docker compose run --rm app rebar3 ct --dir test/ct --suite order_store_SUITE

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-compile([export_all, nowarn_export_all]).

-define(PG_HOST, "postgres").
-define(PG_PORT, 5432).
-define(TEST_DB, "orders_test").

all() ->
    [{group, mnesia}, {group, pg}].

groups() ->
    [{mnesia, [], contract()},
     {pg, [], contract() ++ [status_history_is_recorded]}].

contract() ->
    [save_then_get_roundtrips,
     get_unknown_is_not_found,
     save_keeps_first_outcome,
     update_status_changes_status,
     update_status_unknown_is_not_found,
     created_at_is_optional,
     concurrent_writers_all_succeed].

init_per_group(mnesia, Config) ->
    ok = application:set_env(order_app, store_impl, order_store_mnesia),
    ok = application:set_env(order_app, mnesia_copies, ram_copies),
    ok = order_mnesia:init(),
    [{store, mnesia} | Config];
init_per_group(pg, Config) ->
    case gen_tcp:connect(?PG_HOST, ?PG_PORT, [], 2000) of
        {ok, Socket} ->
            ok = gen_tcp:close(Socket),
            start_pg(Config);
        {error, Reason} ->
            {skip, {postgres_unreachable, Reason}}
    end.

start_pg(Config) ->
    ok = ensure_test_database(),
    ok = application:set_env(order_app, pg, pg_opts(?TEST_DB)),
    ok = application:set_env(order_app, store_impl, order_store_pg),
    {ok, Conn} = order_store_pg:start_link(),
    %% init_per_group's process exits when it returns; keep the connection.
    true = unlink(Conn),
    [{store, pg}, {conn, Conn} | Config].

end_per_group(mnesia, _Config) ->
    stopped = mnesia:stop(),
    ok = application:unset_env(order_app, mnesia_copies),
    application:unset_env(order_app, store_impl);
end_per_group(pg, Config) ->
    ok = epgsql:close(?config(conn, Config)),
    ok = application:unset_env(order_app, pg),
    application:unset_env(order_app, store_impl).

init_per_testcase(_Case, Config) ->
    ok = clear(?config(store, Config), Config),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

clear(mnesia, _Config) ->
    {atomic, ok} = mnesia:clear_table(order),
    ok;
clear(pg, Config) ->
    {ok, _, _} = epgsql:squery(?config(conn, Config), "TRUNCATE order_status_history, orders"),
    ok.

pg_opts(Database) ->
    #{host => ?PG_HOST, port => ?PG_PORT, username => "orders", password => "orders",
      database => Database, timeout => 5000}.

ensure_test_database() ->
    {ok, Admin} = epgsql:connect(pg_opts("orders")),
    {ok, _, Rows} = epgsql:squery(Admin, "SELECT 1 FROM pg_database WHERE datname = '" ?TEST_DB "'"),
    ok = create_database(Rows, Admin),
    epgsql:close(Admin).

create_database([], Admin) ->
    {ok, _, _} = epgsql:squery(Admin, "CREATE DATABASE " ?TEST_DB),
    ok;
create_database([_Exists], _Admin) ->
    ok.

%%-- the contract ----------------------------------------------------

save_then_get_roundtrips(_Config) ->
    ok = order_store:save(order()),
    {ok, Stored} = order_store:get(<<"o-1">>),
    ?assertEqual(order(), maps:without([updated_at], Stored)),
    ?assert(is_integer(maps:get(updated_at, Stored))).

get_unknown_is_not_found(_Config) ->
    ?assertEqual({error, not_found}, order_store:get(<<"nope">>)).

%% Insert-if-absent: a redelivered final result is a no-op.
save_keeps_first_outcome(_Config) ->
    ok = order_store:save((order())#{status => failed}),
    ok = order_store:save(order()),
    ?assertMatch({ok, #{status := failed}}, order_store:get(<<"o-1">>)).

update_status_changes_status(_Config) ->
    ok = order_store:save(order()),
    ok = order_store:update_status(<<"o-1">>, failed),
    ?assertMatch({ok, #{status := failed, amount := 1500}}, order_store:get(<<"o-1">>)).

update_status_unknown_is_not_found(_Config) ->
    ?assertEqual({error, not_found}, order_store:update_status(<<"nope">>, failed)).

created_at_is_optional(_Config) ->
    ok = order_store:save(maps:remove(created_at, order())),
    {ok, Stored} = order_store:get(<<"o-1">>),
    ?assertNot(maps:is_key(created_at, Stored)).

%% The partition workers write in parallel through the store. Regression test:
%% epgsql:equery on one shared connection interleaved its parse and execute
%% steps across processes and failed with "unnamed prepared statement does
%% not exist".
concurrent_writers_all_succeed(_Config) ->
    Ids = [<<"c-", (integer_to_binary(N))/binary>> || N <- lists:seq(1, 50)],
    Self = self(),
    Pids = [spawn_link(fun() ->
                               Result = order_store:save((order())#{id => Id}),
                               Self ! {saved, self(), Result}
                       end) || Id <- Ids],
    Results = [receive {saved, Pid, R} -> R after 10000 -> timeout end || Pid <- Pids],
    ?assertEqual(lists:duplicate(50, ok), Results),
    [?assertMatch({ok, #{id := Id}}, order_store:get(Id)) || Id <- Ids].

%%-- PostgreSQL only -------------------------------------------------

status_history_is_recorded(Config) ->
    ok = order_store:save(order()),
    ok = order_store:save(order()),                       % redelivery: no new row
    ok = order_store:update_status(<<"o-1">>, failed),
    {ok, _, Rows} = epgsql:squery(?config(conn, Config),
                                  "SELECT status, attempt FROM order_status_history "
                                  "WHERE order_id = 'o-1' ORDER BY id"),
    ?assertEqual([{<<"completed">>, <<"2">>}, {<<"failed">>, <<"2">>}], Rows).

%%--------------------------------------------------------------------

order() ->
    #{id => <<"o-1">>, status => completed, customer_id => <<"c-42">>,
      amount => 1500, currency => <<"INR">>,
      items => [#{sku => <<"SKU-1">>, qty => 2}, #{sku => <<"SKU-2">>, qty => 1}],
      attempt => 2, created_at => <<"2026-11-14T10:00:00Z">>}.

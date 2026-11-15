-module(order_store_pg).
-behaviour(order_store).

%% PostgreSQL implementation of order_store, the system of record.
%%
%% One epgsql connection, started by order_sup (only when store_impl is this
%% module) and registered as `order_pg'. epgsql queues requests from many
%% processes on one connection, so the partition workers share it. epgsql's
%% query calls have no client-side timeout, so a server-side
%% statement_timeout bounds every query instead.
%%
%% Every query is a named prepared statement, parsed once per connection.
%% equery/3 is unsafe on a shared connection: it parses an *unnamed*
%% statement and executes it in a second command, so concurrent callers
%% overwrite each other's statement ("unnamed prepared statement does not
%% exist"). Executing a parsed #statement{} is a single command.
%%
%% Each write and its status-history row happen in one SQL statement.

-export([start_link/0]).
-export([save/1, get/1, update_status/2]).

-define(CONN, order_pg).
%% Changes only when a connection (re)starts, so persistent_term suits it.
-define(STATEMENTS, {?MODULE, statements}).
-define(DEFAULT_OPTS, #{host => "localhost", port => 5432, username => "orders",
                        password => "orders", database => "orders", timeout => 5000}).
-define(DEFAULT_STATEMENT_TIMEOUT_MS, 5000).

-define(SAVE_SQL,
        "WITH inserted AS ("
        "  INSERT INTO orders (id, customer_id, amount, currency, items, status, attempt,"
        "                      created_at, updated_at)"
        "  VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8::text::timestamptz,"
        "          to_timestamp($9::bigint / 1000.0))"
        "  ON CONFLICT (id) DO NOTHING"
        "  RETURNING id, status, attempt, updated_at) "
        "INSERT INTO order_status_history (order_id, status, attempt, changed_at) "
        "SELECT id, status, attempt, updated_at FROM inserted").

-define(UPDATE_STATUS_SQL,
        "WITH updated AS ("
        "  UPDATE orders SET status = $2, updated_at = to_timestamp($3::bigint / 1000.0)"
        "  WHERE id = $1"
        "  RETURNING id, status, attempt, updated_at) "
        "INSERT INTO order_status_history (order_id, status, attempt, changed_at) "
        "SELECT id, status, attempt, updated_at FROM updated").

%% The order comes back as JSON so order_codec maps it exactly like a Kafka
%% event; json_strip_nulls drops an absent created_at.
-define(GET_SQL,
        "SELECT json_strip_nulls(json_build_object("
        "         'id', id, 'customer_id', customer_id, 'amount', amount,"
        "         'currency', currency, 'items', items, 'attempt', attempt,"
        "         'created_at', to_char(created_at AT TIME ZONE 'UTC',"
        "                               'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')))::text,"
        "       status, (extract(epoch FROM updated_at) * 1000)::bigint "
        "FROM orders WHERE id = $1").

%%-- connection ------------------------------------------------------

%% Connects, applies the schema, and registers the connection. The
%% connection process is linked to the caller (the supervisor) and is itself
%% the supervised child.
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    Opts = application:get_env(order_app, pg, ?DEFAULT_OPTS),
    connected(epgsql:connect(Opts)).

connected({ok, Conn}) ->
    prepared(prepare(Conn), Conn);
connected({error, _} = Error) ->
    Error.

prepared(ok, Conn) ->
    true = register(?CONN, Conn),
    {ok, Conn};
prepared({error, _} = Error, Conn) ->
    ok = epgsql:close(Conn),
    Error.

prepare(Conn) ->
    Timeout = application:get_env(order_app, pg_statement_timeout_ms,
                                  ?DEFAULT_STATEMENT_TIMEOUT_MS),
    SetTimeout = ["SET statement_timeout = ", integer_to_list(Timeout)],
    Setup = all_ok([epgsql:squery(Conn, SetTimeout) | as_list(epgsql:squery(Conn, schema()))]),
    then(Setup, fun() -> parse_statements(Conn) end).

then(ok, Next) -> Next();
then({error, _} = Error, _Next) -> Error.

parse_statements(Conn) ->
    Parsed = [{Key, epgsql:parse(Conn, atom_to_list(Key), Sql, [])}
              || {Key, Sql} <- [{save_order, ?SAVE_SQL},
                                {get_order, ?GET_SQL},
                                {update_order_status, ?UPDATE_STATUS_SQL}]],
    then(all_ok([Result || {_Key, Result} <- Parsed]),
         fun() ->
                 persistent_term:put(?STATEMENTS,
                                     maps:from_list([{K, S} || {K, {ok, S}} <- Parsed]))
         end).

schema() ->
    {ok, Sql} = file:read_file(filename:join([code:priv_dir(order_app), "sql", "schema.sql"])),
    Sql.

as_list(Results) when is_list(Results) -> Results;
as_list(Result) -> [Result].

all_ok(Results) ->
    case [E || {error, _} = E <- Results] of
        [] -> ok;
        [Error | _] -> Error
    end.

%%-- order_store callbacks -------------------------------------------

%% Insert-if-absent: a redelivered final result is a no-op, so the first
%% final outcome wins (matching the idempotency store).
-spec save(map()) -> ok | {error, term()}.
save(#{id := Id, status := Status} = Order) ->
    Params = [Id,
              maps:get(customer_id, Order),
              maps:get(amount, Order),
              maps:get(currency, Order),
              jsx:encode(maps:get(items, Order)),
              atom_to_binary(Status),
              maps:get(attempt, Order, 0),
              maps:get(created_at, Order, null),
              order_clock:now_ms()],
    written(query(save_order, Params)).

-spec get(binary()) -> {ok, map()} | {error, not_found}.
get(Id) ->
    {ok, _Columns, Rows} = query(get_order, [Id]),
    to_result(Rows).

-spec update_status(binary(), order_store:status()) -> ok | {error, term()}.
update_status(Id, Status) ->
    updated(query(update_order_status, [Id, atom_to_binary(Status), order_clock:now_ms()])).

%%--------------------------------------------------------------------

query(Key, Params) ->
    Statement = maps:get(Key, persistent_term:get(?STATEMENTS)),
    epgsql:prepared_query(whereis(?CONN), Statement, Params).

written({ok, _Count}) -> ok;
written({error, _} = Error) -> Error.

updated({ok, 0}) -> {error, not_found};
updated({ok, _Count}) -> ok;
updated({error, _} = Error) -> Error.

to_result([{Json, Status, UpdatedAt}]) ->
    {ok, Order} = order_codec:decode(Json),
    {ok, Order#{status => status(Status), updated_at => UpdatedAt}};
to_result([]) ->
    {error, not_found}.

%% Statuses are a fixed set, so they are mapped explicitly rather than turned
%% into atoms from database text.
status(<<"pending">>) -> pending;
status(<<"processing">>) -> processing;
status(<<"completed">>) -> completed;
status(<<"failed">>) -> failed.

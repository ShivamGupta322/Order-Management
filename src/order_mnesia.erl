-module(order_mnesia).

%% Creates the Mnesia schema and tables at boot and waits for them to load.

-include("order_tables.hrl").

-export([init/0]).

-define(DEFAULT_WAIT_MS, 5000).

-spec init() -> ok.
init() ->
    Copies = application:get_env(order_app, mnesia_copies, disc_copies),
    ok = ensure_schema(Copies, mnesia:system_info(is_running)),
    ok = mnesia:start(),
    ok = ensure_table(order, [{attributes, record_info(fields, order)},
                              {Copies, [node()]}]),
    ok = ensure_table(processed_order, [{attributes, record_info(fields, processed_order)},
                                        {Copies, [node()]}]),
    ok = ensure_table(gateway_charge, [{attributes, record_info(fields, gateway_charge)},
                                       {Copies, [node()]}]),
    Wait = application:get_env(order_app, mnesia_wait_ms, ?DEFAULT_WAIT_MS),
    ok = mnesia:wait_for_tables(?TABLES, Wait).

%% A disc schema can only be created while Mnesia is stopped. RAM-only nodes
%% (tests) need no schema on disk.
ensure_schema(disc_copies, no) ->
    schema_created(mnesia:create_schema([node()]));
ensure_schema(_Copies, _IsRunning) ->
    ok.

schema_created(ok) -> ok;
schema_created({error, {_Node, {already_exists, _}}}) -> ok.

ensure_table(Name, Opts) ->
    table_created(mnesia:create_table(Name, Opts)).

table_created({atomic, ok}) -> ok;
table_created({aborted, {already_exists, _Name}}) -> ok.

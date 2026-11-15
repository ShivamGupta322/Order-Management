-module(order_store).

%% Storage contract. The implementation is chosen with the `store_impl' app
%% env, so business code never calls Mnesia or PostgreSQL directly.
%%
%% save/1 is insert-if-absent: saving an order that already exists returns ok
%% and changes nothing, so a redelivered final result is a no-op (the guide's
%% `INSERT ... ON CONFLICT (id) DO NOTHING'). update_status/2 changes an
%% existing order and returns {error, not_found} otherwise.

-export([save/1, get/1, update_status/2]).
-export_type([status/0]).

-type status() :: pending | processing | completed | failed.

-callback save(Order :: map()) -> ok | {error, term()}.
-callback get(Id :: binary()) -> {ok, map()} | {error, not_found}.
-callback update_status(Id :: binary(), Status :: atom()) -> ok | {error, term()}.

-spec save(map()) -> ok | {error, term()}.
save(Order) -> (impl()):save(Order).

-spec get(binary()) -> {ok, map()} | {error, not_found}.
get(Id) -> (impl()):get(Id).

-spec update_status(binary(), status()) -> ok | {error, term()}.
update_status(Id, Status) -> (impl()):update_status(Id, Status).

impl() ->
    application:get_env(order_app, store_impl, order_store_mnesia).

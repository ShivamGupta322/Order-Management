-module(payment_gateway).

%% Simulated payment gateway. Outcomes are random, with failure rates taken
%% from app env so demos can force timeouts or declines.
%%
%% Like a real gateway, it treats the order ID as an idempotency key: once a
%% charge for an order has a final result (success or decline), charging the
%% same order again returns that result without charging again. This closes
%% the gap where the worker crashes after the charge but before marking the
%% order processed. A timeout never reached the gateway, so it is not
%% remembered and the next attempt really charges.
%%
%% The gateway's memory is its own Mnesia table, standing in for the storage
%% a real gateway keeps on its side.

-include("order_tables.hrl").

-export([charge/1]).
-export_type([receipt/0]).

-type receipt() :: #{order_id := binary(), receipt_id := binary()}.
-type result() :: {ok, receipt()} | {error, timeout | declined}.

-spec charge(order_codec:order()) -> result().
charge(#{id := Id}) ->
    replay_or_charge(mnesia:dirty_read(gateway_charge, Id), Id).

replay_or_charge([#gateway_charge{result = Result}], _Id) ->
    Result;
replay_or_charge([], Id) ->
    TimeoutRate = application:get_env(order_app, gateway_timeout_rate, 0.0),
    DeclineRate = application:get_env(order_app, gateway_decline_rate, 0.0),
    remember(outcome(rand:uniform(), TimeoutRate, DeclineRate, Id), Id).

outcome(Roll, TimeoutRate, _DeclineRate, _Id) when Roll =< TimeoutRate ->
    {error, timeout};
outcome(Roll, TimeoutRate, DeclineRate, _Id) when Roll =< TimeoutRate + DeclineRate ->
    {error, declined};
outcome(_Roll, _TimeoutRate, _DeclineRate, Id) ->
    {ok, #{order_id => Id,
           receipt_id => integer_to_binary(erlang:unique_integer([positive]))}}.

remember({error, timeout} = Result, _Id) ->
    Result;
remember(Result, Id) ->
    Row = #gateway_charge{order_id = Id, result = Result, charged_at = order_clock:now_ms()},
    {atomic, ok} = mnesia:transaction(fun() -> mnesia:write(Row) end),
    Result.

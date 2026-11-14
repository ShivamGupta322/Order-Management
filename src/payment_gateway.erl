-module(payment_gateway).

%% Simulated payment gateway. Outcomes are random, with failure rates taken
%% from app env so demos can force timeouts or declines.

-export([charge/1]).
-export_type([receipt/0]).

-type receipt() :: #{order_id := binary(), receipt_id := binary()}.

-spec charge(order_codec:order()) -> {ok, receipt()} | {error, timeout | declined}.
charge(#{id := Id}) ->
    TimeoutRate = application:get_env(order_app, gateway_timeout_rate, 0.0),
    DeclineRate = application:get_env(order_app, gateway_decline_rate, 0.0),
    outcome(rand:uniform(), TimeoutRate, DeclineRate, Id).

outcome(Roll, TimeoutRate, _DeclineRate, _Id) when Roll =< TimeoutRate ->
    {error, timeout};
outcome(Roll, TimeoutRate, DeclineRate, _Id) when Roll =< TimeoutRate + DeclineRate ->
    {error, declined};
outcome(_Roll, _TimeoutRate, _DeclineRate, Id) ->
    {ok, #{order_id => Id,
           receipt_id => integer_to_binary(erlang:unique_integer([positive]))}}.

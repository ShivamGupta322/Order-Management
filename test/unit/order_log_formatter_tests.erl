-module(order_log_formatter_tests).
-include_lib("eunit/include/eunit.hrl").

event(Msg) ->
    #{level => warning, msg => Msg,
      meta => #{time => 1794685666388123, mfa => {consumer_worker, route, 4}, pid => self()}}.

decode(Event) ->
    Line = iolist_to_binary(order_log_formatter:format(Event, #{})),
    ?assertEqual($\n, binary:last(Line)),
    json:decode(Line).

report_fields_become_top_level_test() ->
    Out = decode(event({report, #{event => order_retry, order_id => <<"o-1">>,
                                  partition => 2, offset => 41, attempt => 1}})),
    ?assertMatch(#{<<"event">> := <<"order_retry">>, <<"order_id">> := <<"o-1">>,
                   <<"partition">> := 2, <<"offset">> := 41, <<"attempt">> := 1,
                   <<"level">> := <<"warning">>,
                   <<"mfa">> := <<"consumer_worker:route/4">>}, Out).

time_is_rfc3339_utc_test() ->
    #{<<"time">> := Time} = decode(event({report, #{event => x}})),
    ?assertMatch(<<"2026-11-", _/binary>>, Time),
    ?assertEqual($Z, binary:last(Time)).

string_message_test() ->
    ?assertMatch(#{<<"msg">> := <<"hello">>}, decode(event({string, "hello"}))).

format_and_args_message_test() ->
    ?assertMatch(#{<<"msg">> := <<"n=3">>}, decode(event({"n=~b", [3]}))).

keyword_list_report_test() ->
    ?assertMatch(#{<<"event">> := <<"x">>}, decode(event({report, [{event, x}]}))).

%% OTP's own reports carry pids, tuples and nested terms; none may break a line.
non_json_values_are_printed_test() ->
    Out = decode(event({report, #{pid => self(), reason => {badmatch, {error, 1}},
                                  raw => <<255, 0>>, nested => #{a => [1, x]}}})),
    ?assertMatch(#{<<"reason">> := <<"{badmatch,{error,1}}">>,
                   <<"raw">> := <<"<<255,0>>">>,
                   <<"nested">> := #{<<"a">> := <<"[1,x]">>}}, Out),
    ?assert(is_binary(maps:get(<<"pid">>, Out))).

check_config_test() ->
    ?assertEqual(ok, order_log_formatter:check_config(#{})).

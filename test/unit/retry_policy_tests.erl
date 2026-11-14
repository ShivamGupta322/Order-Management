-module(retry_policy_tests).
-include_lib("eunit/include/eunit.hrl").

%% From the implementation guide.
backoff_test_() ->
    [?_assertEqual({retry, 1000},  retry_policy:next(0)),
     ?_assertEqual({retry, 5000},  retry_policy:next(1)),
     ?_assertEqual({retry, 30000}, retry_policy:next(2)),
     ?_assertEqual(dead_letter,    retry_policy:next(3))].

past_the_limit_test_() ->
    [?_assertEqual(dead_letter, retry_policy:next(N)) || N <- [4, 10, 1000]].

custom_delays_test_() ->
    Delays = [10, 20],
    [?_assertEqual({retry, 10}, retry_policy:next(0, Delays)),
     ?_assertEqual({retry, 20}, retry_policy:next(1, Delays)),
     ?_assertEqual(dead_letter, retry_policy:next(2, Delays))].

no_retries_test() ->
    ?assertEqual(dead_letter, retry_policy:next(0, [])).

default_delays_test() ->
    ?assertEqual([1000, 5000, 30000], retry_policy:default_delays()).

invalid_attempt_test_() ->
    [?_assertError(function_clause, retry_policy:next(-1)),
     ?_assertError(function_clause, retry_policy:next(<<"1">>))].

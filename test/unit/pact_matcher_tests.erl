-module(pact_matcher_tests).
-include_lib("eunit/include/eunit.hrl").

type() -> #{<<"matchers">> => [#{<<"match">> => <<"type">>}]}.
integer() -> #{<<"matchers">> => [#{<<"match">> => <<"integer">>}]}.

example() ->
    #{<<"id">> => <<"o-1">>, <<"amount">> => 1500,
      <<"items">> => [#{<<"sku">> => <<"S">>, <<"qty">> => 2}]}.

rules() ->
    #{<<"$.id">> => type(),
      <<"$.amount">> => integer(),
      <<"$.items">> => #{<<"matchers">> => [#{<<"match">> => <<"type">>, <<"min">> => 1}]},
      <<"$.items[*].sku">> => type(),
      <<"$.items[*].qty">> => integer()}.

verify(Actual) -> pact_matcher:verify(example(), Actual, rules()).

identical_value_passes_test() ->
    ?assertEqual([], verify(example())).

different_values_pass_when_rules_allow_test() ->
    ?assertEqual([], verify(#{<<"id">> => <<"x-99">>, <<"amount">> => 7,
                              <<"items">> => [#{<<"sku">> => <<"A">>, <<"qty">> => 1},
                                              #{<<"sku">> => <<"B">>, <<"qty">> => 5}]})).

extra_fields_are_allowed_test() ->
    ?assertEqual([], verify((example())#{<<"note">> => <<"extra">>})).

missing_field_test() ->
    ?assertEqual([{<<"$.amount">>, missing}], verify(maps:remove(<<"amount">>, example()))).

type_mismatch_test() ->
    ?assertMatch([{<<"$.id">>, {expected_type, string, got, 42}}],
                 verify((example())#{<<"id">> => 42})).

integer_matcher_rejects_float_and_string_test_() ->
    [?_assertMatch([{<<"$.amount">>, {expected_integer, _}}], verify((example())#{<<"amount">> => A}))
     || A <- [15.5, <<"1500">>]].

array_min_test() ->
    ?assertMatch([{<<"$.items">>, {expected_at_least, 1, got, 0}}],
                 verify((example())#{<<"items">> => []})).

array_elements_checked_against_template_test() ->
    Items = [#{<<"sku">> => <<"A">>, <<"qty">> => 1}, #{<<"qty">> => 2}],
    ?assertEqual([{<<"$.items[*].sku">>, missing}],
                 verify((example())#{<<"items">> => Items})).

nested_rule_applies_to_every_element_test() ->
    Items = [#{<<"sku">> => <<"A">>, <<"qty">> => 1}, #{<<"sku">> => <<"B">>, <<"qty">> => 1.5}],
    ?assertMatch([{<<"$.items[*].qty">>, {expected_integer, 1.5}}],
                 verify((example())#{<<"items">> => Items})).

no_matcher_means_equality_test_() ->
    [?_assertEqual([], pact_matcher:verify(#{<<"v">> => 1}, #{<<"v">> => 1}, #{})),
     ?_assertMatch([{<<"$.v">>, {expected, 1, got, 2}}],
                   pact_matcher:verify(#{<<"v">> => 1}, #{<<"v">> => 2}, #{}))].

regex_matcher_test_() ->
    Rules = #{<<"$.c">> => #{<<"matchers">> => [#{<<"match">> => <<"regex">>,
                                                   <<"regex">> => <<"^[A-Z]{3}$">>}]}},
    Verify = fun(V) -> pact_matcher:verify(#{<<"c">> => <<"INR">>}, #{<<"c">> => V}, Rules) end,
    [?_assertEqual([], Verify(<<"USD">>)),
     ?_assertMatch([{<<"$.c">>, {expected_to_match, _, got, <<"usd">>}}], Verify(<<"usd">>)),
     ?_assertMatch([{<<"$.c">>, {expected_to_match, _, got, 1}}], Verify(1))].

not_an_object_test() ->
    ?assertMatch([{<<"$">>, {expected_object, _}}], verify([1, 2])).

unsupported_matcher_is_an_error_test() ->
    Rules = #{<<"$.v">> => #{<<"matchers">> => [#{<<"match">> => <<"decimal">>}]}},
    ?assertMatch([{<<"$.v">>, {unsupported_matcher, _}}],
                 pact_matcher:verify(#{<<"v">> => 1.5}, #{<<"v">> => 1.5}, Rules)).

message_errors_reads_matching_rules_test() ->
    Message = #{<<"contents">> => example(), <<"matchingRules">> => #{<<"body">> => rules()}},
    ?assertEqual([], pact_matcher:message_errors(Message, (example())#{<<"id">> => <<"z">>})).

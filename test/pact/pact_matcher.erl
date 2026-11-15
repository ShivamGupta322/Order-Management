-module(pact_matcher).

%% A small verifier for Pact v3 message contracts (Option B in the guide: no
%% Pact library). Checks an actual JSON value against a message's example
%% `contents' and its `matchingRules.body'.
%%
%% Supported matchers: `type' (with optional `min' for arrays), `integer',
%% and `regex'. Any other matcher is reported as an error, so a contract can
%% never pass on a rule this module silently ignores.
%%
%% Semantics follow Pact: every field in the example must be present; extra
%% fields in the actual value are allowed; a value with no matcher must equal
%% the example; an array with a `type' matcher has each element checked
%% against the example's first element, at path `<array>[*]'.

-export([message_errors/2, verify/3, pact_file/0]).

-type json() :: null | boolean() | number() | binary() | [json()] | #{binary() => json()}.
-type error() :: {Path :: binary(), Reason :: term()}.

-define(PACT_FILE, "order_consumer-order_api_producer.json").

%% The one place the pact file is named. Resolved from this module's source
%% location, because CT runs with its log directory as the working directory.
-spec pact_file() -> file:filename().
pact_file() ->
    Root = filename:dirname(filename:dirname(filename:dirname(?FILE))),
    filename:join([Root, "pacts", ?PACT_FILE]).

-spec message_errors(#{binary() => json()}, json()) -> [error()].
message_errors(#{<<"contents">> := Expected} = Message, Actual) ->
    Rules = maps:get(<<"body">>, maps:get(<<"matchingRules">>, Message, #{}), #{}),
    verify(Expected, Actual, Rules).

-spec verify(json(), json(), #{binary() => json()}) -> [error()].
verify(Expected, Actual, Rules) ->
    check(<<"$">>, Expected, Actual, Rules).

check(Path, Expected, Actual, Rules) ->
    Matchers = matchers(Path, Rules),
    case node_errors(Path, Matchers, Expected, Actual) of
        [] -> children(Path, Matchers, Expected, Actual, Rules);
        Errors -> Errors
    end.

matchers(Path, Rules) ->
    case maps:find(Path, Rules) of
        {ok, #{<<"matchers">> := Matchers}} -> Matchers;
        error -> []
    end.

%%-- this node -------------------------------------------------------

node_errors(Path, [], Expected, Actual) when is_map(Expected) ->
    expect(is_map(Actual), Path, {expected_object, Actual});
node_errors(Path, [], Expected, Actual) ->
    expect(Expected =:= Actual, Path, {expected, Expected, got, Actual});
node_errors(Path, Matchers, Expected, Actual) ->
    lists:flatmap(fun(M) -> match(Path, M, Expected, Actual) end, Matchers).

match(Path, #{<<"match">> := <<"type">>} = M, Expected, Actual) ->
    expect(json_type(Expected) =:= json_type(Actual), Path,
           {expected_type, json_type(Expected), got, Actual})
        ++ min_length(Path, maps:get(<<"min">>, M, 0), Actual);
match(Path, #{<<"match">> := <<"integer">>}, _Expected, Actual) ->
    expect(is_integer(Actual), Path, {expected_integer, Actual});
match(Path, #{<<"match">> := <<"regex">>, <<"regex">> := Regex}, _Expected, Actual) ->
    expect(is_binary(Actual) andalso re:run(Actual, Regex, [{capture, none}]) =:= match,
           Path, {expected_to_match, Regex, got, Actual});
match(Path, Matcher, _Expected, _Actual) ->
    [{Path, {unsupported_matcher, Matcher}}].

min_length(Path, Min, Actual) when is_list(Actual), length(Actual) < Min ->
    [{Path, {expected_at_least, Min, got, length(Actual)}}];
min_length(_Path, _Min, _Actual) ->
    [].

%%-- children --------------------------------------------------------

children(Path, _Matchers, Expected, Actual, Rules) when is_map(Expected) ->
    lists:flatmap(fun(Key) -> field(<<Path/binary, ".", Key/binary>>, Key, Expected, Actual, Rules) end,
                  lists:sort(maps:keys(Expected)));
children(Path, [_ | _], [Template | _], Actual, Rules) when is_list(Actual) ->
    lists:flatmap(fun(Element) -> check(<<Path/binary, "[*]">>, Template, Element, Rules) end,
                  Actual);
children(_Path, _Matchers, _Expected, _Actual, _Rules) ->
    [].

field(Path, Key, Expected, Actual, Rules) ->
    case maps:find(Key, Actual) of
        {ok, Value} -> check(Path, maps:get(Key, Expected), Value, Rules);
        error -> [{Path, missing}]
    end.

%%--------------------------------------------------------------------

expect(true, _Path, _Reason) -> [];
expect(false, Path, Reason) -> [{Path, Reason}].

json_type(V) when is_binary(V) -> string;
json_type(V) when is_number(V) -> number;
json_type(V) when is_boolean(V) -> boolean;
json_type(null) -> null;
json_type(V) when is_list(V) -> array;
json_type(V) when is_map(V) -> object.

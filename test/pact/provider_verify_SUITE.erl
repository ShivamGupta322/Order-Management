-module(provider_verify_SUITE).

%% Provider side of the message contract. Posts a real order through the HTTP
%% API, captures the payload the producer actually publishes to `orders', and
%% verifies it against every field and matching rule in the pact file
%% written by consumer_pact_SUITE.

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-compile([export_all, nowarn_export_all]).

all() ->
    [published_event_satisfies_contract,
     renamed_field_breaks_contract,
     float_amount_breaks_contract].

init_per_suite(Config) ->
    {ok, Json} = file:read_file(pact_matcher:pact_file()),
    #{<<"messages">> := Messages} = jsx:decode(Json, [return_maps]),
    ok = order_test_helper:start_app(),
    [{messages, Messages} | Config].

end_per_suite(_Config) ->
    order_test_helper:stop_app().

init_per_testcase(_Case, Config) ->
    ok = order_test_helper:reset(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------

%% Values deliberately differ from the contract's examples: the matching
%% rules, not the example values, decide what the consumer accepts.
published_event_satisfies_contract(Config) ->
    Event = published_event(),
    [?assertEqual([], pact_matcher:message_errors(M, Event)) || M <- ?config(messages, Config)].

%% The guide's promise: renaming `amount' to `total' fails the build.
renamed_field_breaks_contract(Config) ->
    #{<<"amount">> := Amount} = Event = published_event(),
    Renamed = (maps:remove(<<"amount">>, Event))#{<<"total">> => Amount},
    [?assertEqual([{<<"$.amount">>, missing}], pact_matcher:message_errors(M, Renamed))
     || M <- ?config(messages, Config)].

float_amount_breaks_contract(Config) ->
    Event = (published_event())#{<<"amount">> => 15.5},
    [?assertMatch([{<<"$.amount">>, {expected_integer, 15.5}}],
                  pact_matcher:message_errors(M, Event))
     || M <- ?config(messages, Config)].

%%--------------------------------------------------------------------

%% Provider state "an order is posted to the API".
published_event() ->
    Body = jsx:encode(#{id => <<"o-pact-7">>, customer_id => <<"c-9">>, amount => 4999,
                        currency => <<"USD">>,
                        items => [#{sku => <<"SKU-A">>, qty => 1}, #{sku => <<"SKU-B">>, qty => 3}]}),
    Url = order_test_helper:base_url() ++ "/orders",
    {ok, {{_, 202, _}, _, _}} = httpc:request(post, {Url, [], "application/json", Body}, [], []),
    [Payload] = order_test_helper:published(<<"orders">>),
    jsx:decode(Payload, [return_maps]).

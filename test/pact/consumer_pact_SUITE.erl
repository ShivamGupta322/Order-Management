-module(consumer_pact_SUITE).

%% Consumer side of the message contract. Defines the `order.created' event
%% the consumer needs, proves each message decodes and is processed by
%% order_processor without error, and writes the pact file that
%% provider_verify_SUITE checks the producer against.
%%
%% Changing the event shape means changing message() here, the generated
%% file in pacts/, and (if needed) provider_verify_SUITE, in one change.

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-compile([export_all, nowarn_export_all]).

all() ->
    [examples_satisfy_their_own_rules,
     contract_messages_are_processed,
     pact_file_is_written].

init_per_suite(Config) ->
    ok = application:set_env(order_app, mnesia_copies, ram_copies),
    ok = order_mnesia:init(),
    ok = meck:new(payment_gateway, [no_link]),
    ok = meck:expect(payment_gateway, charge,
                     fun(#{id := Id}) -> {ok, #{order_id => Id, receipt_id => <<"r-1">>}} end),
    Config.

end_per_suite(_Config) ->
    meck:unload(payment_gateway),
    stopped = mnesia:stop(),
    application:unset_env(order_app, mnesia_copies).

%%--------------------------------------------------------------------

examples_satisfy_their_own_rules(_Config) ->
    [?assertEqual([], pact_matcher:message_errors(M, maps:get(<<"contents">>, M)))
     || M <- messages()].

contract_messages_are_processed(_Config) ->
    [begin
         {ok, Order} = order_codec:decode(jsx:encode(maps:get(<<"contents">>, M))),
         ?assertMatch({ok, #{receipt_id := _}}, order_processor:process(Order))
     end || M <- messages()].

pact_file_is_written(_Config) ->
    File = pact_matcher:pact_file(),
    ok = filelib:ensure_dir(File),
    ok = file:write_file(File, [jsx:encode(pact(), [{space, 1}, {indent, 2}]), $\n]),
    {ok, Written} = file:read_file(File),
    ?assertEqual(pact(), jsx:decode(Written, [return_maps])).

%%--------------------------------------------------------------------

pact() ->
    #{<<"consumer">> => #{<<"name">> => <<"order_consumer">>},
      <<"provider">> => #{<<"name">> => <<"order_api_producer">>},
      <<"messages">> => messages(),
      <<"metadata">> => #{<<"pactSpecification">> => #{<<"version">> => <<"3.0.0">>}}}.

messages() ->
    [#{<<"description">> => <<"an order created event">>,
       <<"contents">> =>
           #{<<"id">> => <<"o-1">>,
             <<"customer_id">> => <<"c-42">>,
             <<"amount">> => 1500,
             <<"currency">> => <<"INR">>,
             <<"items">> => [#{<<"sku">> => <<"SKU-1">>, <<"qty">> => 2}],
             <<"attempt">> => 0,
             <<"created_at">> => <<"2026-11-14T10:00:00Z">>},
       <<"matchingRules">> =>
           #{<<"body">> =>
                 #{<<"$.id">> => type(),
                   <<"$.customer_id">> => type(),
                   <<"$.amount">> => integer(),
                   <<"$.currency">> => regex(<<"^[A-Z]{3}$">>),
                   <<"$.items">> => #{<<"matchers">> => [#{<<"match">> => <<"type">>,
                                                           <<"min">> => 1}]},
                   <<"$.items[*].sku">> => type(),
                   <<"$.items[*].qty">> => integer(),
                   <<"$.attempt">> => integer(),
                   <<"$.created_at">> =>
                       regex(<<"^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z$">>)}},
       <<"metadata">> => #{<<"contentType">> => <<"application/json">>}}].

type() -> #{<<"matchers">> => [#{<<"match">> => <<"type">>}]}.
integer() -> #{<<"matchers">> => [#{<<"match">> => <<"integer">>}]}.
regex(Regex) -> #{<<"matchers">> => [#{<<"match">> => <<"regex">>, <<"regex">> => Regex}]}.

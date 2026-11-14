-module(order_http_handler).
-behaviour(cowboy_handler).

%% Thin HTTP shell: decode, validate, publish, translate the result.

-include_lib("kernel/include/logger.hrl").
-include("order_topics.hrl").

-export([init/2]).
-export_type([route/0]).

-type route() :: orders | order | health.

-define(DEFAULT_MAX_BODY_BYTES, 65536).

-spec init(cowboy_req:req(), route()) -> {ok, cowboy_req:req(), route()}.
init(Req0, Route) ->
    Req = handle(Route, cowboy_req:method(Req0), Req0),
    {ok, Req, Route}.

handle(health, <<"GET">>, Req) ->
    reply(200, #{status => ok}, Req);
handle(orders, <<"POST">>, Req) ->
    create_order(Req);
handle(order, <<"GET">>, Req) ->
    show_order(order_store:get(cowboy_req:binding(id, Req)), Req);
handle(_Route, _Method, Req) ->
    reply(405, #{error => method_not_allowed}, Req).

create_order(Req0) ->
    MaxBytes = application:get_env(order_app, max_body_bytes, ?DEFAULT_MAX_BODY_BYTES),
    case cowboy_req:read_body(Req0, #{length => MaxBytes}) of
        {ok, Body, Req} -> validate(order_codec:decode(Body), Req);
        {more, _Partial, Req} -> reply(413, #{error => body_too_large}, Req)
    end.

show_order({ok, Order}, Req) ->
    reply(200, Order, Req);
show_order({error, not_found}, Req) ->
    reply(404, #{error => not_found}, Req).

validate({ok, Order}, Req) ->
    publish(order_validator:validate(Order), Req);
validate({error, Reason}, Req) ->
    reply(400, #{error => Reason}, Req).

publish({ok, #{id := Id} = Order}, Req) ->
    Event = Order#{attempt => 0, created_at => order_clock:now_rfc3339()},
    Result = order_producer:publish(?TOPIC_ORDERS, Id, order_codec:encode(Event)),
    respond(Result, Id, Req);
publish({error, {Kind, Field}}, Req) ->
    reply(400, #{error => Kind, field => Field}, Req).

respond(ok, Id, Req) ->
    ?LOG_INFO(#{event => order_accepted, order_id => Id}),
    reply(202, #{id => Id}, Req);
respond({error, Reason}, Id, Req) ->
    ?LOG_WARNING(#{event => order_publish_failed, order_id => Id, reason => Reason}),
    reply(503, #{error => kafka_unavailable}, Req).

reply(Status, Body, Req) ->
    cowboy_req:reply(Status, #{<<"content-type">> => <<"application/json">>},
                     jsx:encode(Body), Req).

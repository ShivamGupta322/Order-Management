-module(order_metrics).

%% Declares the Prometheus metrics, with the guide's names. Each metric is
%% incremented where its event happens (order_http_handler, consumer_worker),
%% not through a wrapper here. prometheus.erl keeps them in its own registry,
%% so no process of ours is needed.

-export([declare/0, render/0]).

-define(DURATION_BUCKETS, [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5]).

%% Safe to call more than once (an app restart in tests): declare/1 returns
%% false for a metric that already exists.
-spec declare() -> ok.
declare() ->
    _ = prometheus_counter:declare([{name, orders_received_total},
                                    {help, "Orders accepted by POST /orders and published."}]),
    _ = prometheus_counter:declare([{name, orders_processed_total},
                                    {labels, [result]},
                                    {help, "Orders reaching an outcome: completed, failed, duplicate."}]),
    _ = prometheus_counter:declare([{name, orders_retried_total},
                                    {labels, [attempt]},
                                    {help, "Orders sent to orders.retry, by the attempt they will make."}]),
    _ = prometheus_counter:declare([{name, orders_dlq_total},
                                    {labels, [reason]},
                                    {help, "Messages sent to orders.dlq, by reason."}]),
    _ = prometheus_histogram:declare([{name, order_processing_duration_seconds},
                                      {buckets, ?DURATION_BUCKETS},
                                      {help, "Time to handle one message from the orders topic."}]),
    ok.

%% The Prometheus text exposition of every registered metric.
-spec render() -> binary().
render() ->
    prometheus_text_format:format().

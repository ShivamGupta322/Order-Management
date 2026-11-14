%% Kafka topic names. These are part of the external contract; do not rename.
-define(TOPIC_ORDERS, <<"orders">>).
-define(TOPIC_RETRY, <<"orders.retry">>).
-define(TOPIC_COMPLETED, <<"orders.completed">>).
-define(TOPIC_DLQ, <<"orders.dlq">>).

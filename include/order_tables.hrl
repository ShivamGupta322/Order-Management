%% Mnesia tables. Every table is named here and nowhere else.

-record(order, {id :: binary(),
                status :: pending | processing | completed | failed,
                customer_id :: binary(),
                amount :: non_neg_integer(),
                currency :: binary(),
                items :: [map()],
                attempt = 0 :: non_neg_integer(),
                created_at :: binary() | undefined,
                updated_at :: integer()}).

%% Order IDs that reached a final outcome (completed or failed).
-record(processed_order, {id :: binary(), processed_at :: integer()}).

%% The simulated payment gateway's own memory of final charge results, keyed
%% by order ID (its idempotency key). Stands in for a real gateway's storage.
-record(gateway_charge, {order_id :: binary(),
                         result :: {ok, map()} | {error, declined},
                         charged_at :: integer()}).

-define(TABLES, [order, processed_order, gateway_charge]).

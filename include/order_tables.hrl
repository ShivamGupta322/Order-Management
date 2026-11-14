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

-define(TABLES, [order]).

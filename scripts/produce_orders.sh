#!/usr/bin/env sh
# Posts N orders to the API, CONCURRENCY at a time.
#   ./scripts/produce_orders.sh [N] [CONCURRENCY] [BASE_URL]
# Order IDs are unique per run (prefix with the current time), so runs never
# collide with earlier orders as duplicates. Prints one HTTP status per order.
set -eu

N="${1:-100}"
CONCURRENCY="${2:-10}"
BASE_URL="${3:-http://localhost:8080}"
RUN="$(date +%s)"

seq 1 "$N" | xargs -P "$CONCURRENCY" -I{} \
  curl -s -o /dev/null -w "%{http_code}\n" -X POST "$BASE_URL/orders" \
    -H 'content-type: application/json' \
    -d "{\"id\":\"load-$RUN-{}\",\"customer_id\":\"c-load\",\"amount\":1000,\"currency\":\"INR\",\"items\":[{\"sku\":\"SKU-1\",\"qty\":1}]}"

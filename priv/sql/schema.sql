-- PostgreSQL schema for order_store_pg. Applied by the app at boot; every
-- statement is idempotent, so it is safe to run on every start.

CREATE TABLE IF NOT EXISTS orders (
  id          TEXT        PRIMARY KEY,
  customer_id TEXT        NOT NULL,
  amount      BIGINT      NOT NULL CHECK (amount > 0),
  currency    TEXT        NOT NULL,
  items       JSONB       NOT NULL,
  status      TEXT        NOT NULL,
  attempt     INT         NOT NULL DEFAULT 0,
  created_at  TIMESTAMPTZ,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per status an order has had, written in the same statement as the
-- change itself.
CREATE TABLE IF NOT EXISTS order_status_history (
  id         BIGSERIAL   PRIMARY KEY,
  order_id   TEXT        NOT NULL REFERENCES orders (id),
  status     TEXT        NOT NULL,
  attempt    INT         NOT NULL,
  changed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS order_status_history_order_id
  ON order_status_history (order_id, changed_at);

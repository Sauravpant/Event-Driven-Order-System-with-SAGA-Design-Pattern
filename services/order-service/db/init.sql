CREATE TABLE orders (
  id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  product_id      TEXT NOT NULL,
  quantity        INTEGER NOT NULL,
  amount_cents    INTEGER NOT NULL,
  saga_state      TEXT NOT NULL DEFAULT 'STARTED',
  -- STARTED -> INVENTORY_RESERVED -> PAYMENT_PROCESSED -> CONFIRMED
  --         -> INVENTORY_FAILED   -> FAILED
  --                                -> COMPENSATING -> FAILED
  failure_reason  TEXT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE saga_timeline (
  id          BIGSERIAL PRIMARY KEY,
  order_id    UUID NOT NULL REFERENCES orders(id),
  step        TEXT NOT NULL,
  detail      TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: written in the SAME transaction as the business
-- state change above. A relay process polls published = false, publishes
-- to Kafka keyed by order id (guarantees per-order ordering within a
-- partition), then marks published = true.
CREATE TABLE outbox_events (
  id            BIGSERIAL PRIMARY KEY,
  aggregate_id  UUID NOT NULL,
  topic         TEXT NOT NULL,
  payload       JSONB NOT NULL,
  published     BOOLEAN NOT NULL DEFAULT false,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_unpublished ON outbox_events (published) WHERE published = false;

-- Idempotency: record every inbound event id we've already acted on so a
-- redelivered/reprocessed Kafka message (e.g. after a consumer restart
-- resumes from an uncommitted offset) can never be double-processed.
CREATE TABLE processed_events (
  event_id    TEXT PRIMARY KEY,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

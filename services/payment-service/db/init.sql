CREATE TABLE payments (
  order_id      UUID PRIMARY KEY,
  amount_cents  INTEGER NOT NULL,
  status        TEXT NOT NULL, -- SUCCEEDED | FAILED
  failure_reason TEXT,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE outbox_events (
  id            BIGSERIAL PRIMARY KEY,
  aggregate_id  UUID NOT NULL,
  topic         TEXT NOT NULL,
  payload       JSONB NOT NULL,
  published     BOOLEAN NOT NULL DEFAULT false,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_outbox_unpublished ON outbox_events (published) WHERE published = false;

CREATE TABLE processed_events (
  event_id    TEXT PRIMARY KEY,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

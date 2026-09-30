CREATE TABLE inventory (
  product_id      TEXT PRIMARY KEY,
  name            TEXT NOT NULL,
  price_cents     INTEGER NOT NULL,
  available_qty   INTEGER NOT NULL,
  reserved_qty    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE reservations (
  order_id    UUID PRIMARY KEY,
  product_id  TEXT NOT NULL REFERENCES inventory(product_id),
  quantity    INTEGER NOT NULL,
  status      TEXT NOT NULL DEFAULT 'RESERVED', -- RESERVED | RELEASED
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
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

-- Seed catalog. sku-out-of-stock has 0 units on purpose so you can demo
-- the inventory-failure branch of the saga on demand.
INSERT INTO inventory (product_id, name, price_cents, available_qty) VALUES
  ('sku-widget',        'Chrome Widget',      1999, 50),
  ('sku-gadget',        'Steel Gadget',       4999, 30),
  ('sku-gizmo',         'Titanium Gizmo',     9999, 12),
  ('sku-out-of-stock',  'Sold Out Item',      2999, 0);

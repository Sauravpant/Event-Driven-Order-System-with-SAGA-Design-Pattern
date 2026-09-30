import type { PoolClient } from "pg";
import { pool } from "../db.js";
import type { SagaEventPayload, InventoryItem } from "../types.js";

async function alreadyProcessed(
  client: PoolClient,
  dedupeKey: string,
): Promise<boolean> {
  const { rows } = await client.query(
    `SELECT 1 FROM processed_events WHERE event_id = $1`,
    [dedupeKey],
  );
  return rows.length > 0;
}
async function markProcessed(
  client: PoolClient,
  dedupeKey: string,
): Promise<void> {
  await client.query(
    `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
    [dedupeKey],
  );
}
async function emit(
  client: PoolClient,
  orderId: string,
  topic: string,
  payload: Record<string, unknown>,
): Promise<void> {
  await client.query(
    `INSERT INTO outbox_events (aggregate_id, topic, payload) VALUES ($1, $2, $3)`,
    [orderId, topic, payload],
  );
}

async function handleReserve(
  client: PoolClient,
  dedupeKey: string,
  payload: SagaEventPayload,
): Promise<void> {
  const { orderId, productId, quantity } = payload as {
    orderId: string;
    productId: string;
    quantity: number;
  };
  const { rows } = await client.query<InventoryItem>(
    `SELECT * FROM inventory WHERE product_id = $1 FOR UPDATE`,
    [productId],
  );
  const item = rows[0];

  if (!item || item.available_qty < quantity) {
    await emit(client, orderId, "inventory.failed", {
      reason:
        !item ? "unknown product" : (
          `only ${item.available_qty} unit(s) available`
        ),
    });
    await markProcessed(client, dedupeKey);
    return;
  }

  await client.query(
    `UPDATE inventory SET available_qty = available_qty - $2, reserved_qty = reserved_qty + $2 WHERE product_id = $1`,
    [productId, quantity],
  );
  await client.query(
    `INSERT INTO reservations (order_id, product_id, quantity, status) VALUES ($1, $2, $3, 'RESERVED')`,
    [orderId, productId, quantity],
  );
  await emit(client, orderId, "inventory.reserved", { productId, quantity });
  await markProcessed(client, dedupeKey);
}

async function handleRelease(
  client: PoolClient,
  dedupeKey: string,
  payload: SagaEventPayload,
): Promise<void> {
  const { orderId, productId, quantity } = payload as {
    orderId: string;
    productId: string;
    quantity: number;
  };
  const { rows } = await client.query(
    `SELECT * FROM reservations WHERE order_id = $1 AND status = 'RESERVED' FOR UPDATE`,
    [orderId],
  );
  if (rows.length === 0) {
    await emit(client, orderId, "inventory.released", {});
    await markProcessed(client, dedupeKey);
    return;
  }

  await client.query(
    `UPDATE inventory SET available_qty = available_qty + $2, reserved_qty = reserved_qty - $2 WHERE product_id = $1`,
    [productId, quantity],
  );
  await client.query(
    `UPDATE reservations SET status = 'RELEASED' WHERE order_id = $1`,
    [orderId],
  );
  await emit(client, orderId, "inventory.released", {});
  await markProcessed(client, dedupeKey);
}

export async function handleInventoryCommand(
  topic: string,
  payload: SagaEventPayload,
): Promise<void> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const dedupeKey = `${topic}:${payload.eventId}`;
    if (await alreadyProcessed(client, dedupeKey)) {
      await client.query("COMMIT");
      return;
    }

    if (topic === "order.created") {
      await handleReserve(client, dedupeKey, payload);
    } else if (topic === "inventory.release") {
      await handleRelease(client, dedupeKey, payload);
    } else {
      console.warn(`[inventory] unhandled topic: ${topic}`);
    }

    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[inventory] failed to handle command:", err);
    throw err;
  } finally {
    client.release();
  }
}

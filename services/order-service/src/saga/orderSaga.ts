import type { PoolClient } from "../../node_modules/@types/pg/index.js/../node_modules/@types/pg/index.js";
import { pool } from "../db.js";
import type { SagaEventPayload, Order } from "../types.js";

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

async function logStep(
  client: PoolClient,
  orderId: string,
  step: string,
  detail?: string,
): Promise<void> {
  await client.query(
    `INSERT INTO saga_timeline (order_id, step, detail) VALUES ($1, $2, $3)`,
    [orderId, step, detail || null],
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

// dedupeKey uses the sender's own outbox-row-derived eventId, so a message
// redelivered by a consumer-group rebalance or an offset replay can never
// be double-applied to the saga state machine.
export async function handleSagaEvent(
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

    const { orderId } = payload;
    const { rows } = await client.query<Order>(
      `SELECT * FROM orders WHERE id = $1 FOR UPDATE`,
      [orderId],
    );
    const order = rows[0];
    if (!order) {
      console.warn(`[saga] event for unknown order ${orderId}, ignoring`);
      await client.query("COMMIT");
      return;
    }

    switch (topic) {
      case "inventory.reserved": {
        await client.query(
          `UPDATE orders SET saga_state = 'INVENTORY_RESERVED', updated_at = now() WHERE id = $1`,
          [orderId],
        );
        await logStep(
          client,
          orderId,
          "INVENTORY_RESERVED",
          "Stock reserved, requesting payment",
        );
        await emit(client, orderId, "payment.process", {
          amountCents: order.amount_cents,
        });
        break;
      }

      case "inventory.failed": {
        await client.query(
          `UPDATE orders SET saga_state = 'FAILED', failure_reason = $2, updated_at = now() WHERE id = $1`,
          [orderId, payload.reason || "inventory unavailable"],
        );
        await logStep(
          client,
          orderId,
          "FAILED",
          `Inventory reservation failed: ${payload.reason}`,
        );
        break;
      }

      case "payment.processed": {
        await client.query(
          `UPDATE orders SET saga_state = 'CONFIRMED', updated_at = now() WHERE id = $1`,
          [orderId],
        );
        await logStep(
          client,
          orderId,
          "CONFIRMED",
          "Payment succeeded, order confirmed",
        );
        break;
      }

      case "payment.failed": {
        // Compensation: payment failed after inventory was already
        // reserved. Tell inventory-service to release the stock it holds.
        await client.query(
          `UPDATE orders SET saga_state = 'COMPENSATING', failure_reason = $2, updated_at = now() WHERE id = $1`,
          [orderId, payload.reason || "payment declined"],
        );
        await logStep(
          client,
          orderId,
          "COMPENSATING",
          `Payment failed (${payload.reason}), releasing inventory`,
        );
        await emit(client, orderId, "inventory.release", {
          productId: order.product_id,
          quantity: order.quantity,
        });
        break;
      }

      case "inventory.released": {
        await client.query(
          `UPDATE orders SET saga_state = 'FAILED', updated_at = now() WHERE id = $1`,
          [orderId],
        );
        await logStep(
          client,
          orderId,
          "FAILED",
          "Compensation complete: inventory released, order failed",
        );
        break;
      }

      default:
        console.warn(`[saga] unhandled topic: ${topic}`);
    }

    await markProcessed(client, dedupeKey);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[saga] failed to handle event:", err);
    throw err;
  } finally {
    client.release();
  }
}

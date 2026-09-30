import type { PoolClient } from "pg";
import { pool } from "../db.js";
import type { SagaEventPayload } from "../types.js";

const FAILURE_RATE = Number(process.env.PAYMENT_FAILURE_RATE ?? 0.25);

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

export async function handlePaymentCommand(
  topic: string,
  payload: SagaEventPayload,
): Promise<void> {
  if (topic !== "payment.process") {
    console.warn(`[payment] unhandled topic: ${topic}`);
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const dedupeKey = `${topic}:${payload.eventId}`;
    if (await alreadyProcessed(client, dedupeKey)) {
      await client.query("COMMIT");
      return;
    }

    const { orderId, amountCents } = payload;
    const succeeds = Math.random() >= FAILURE_RATE;

    await new Promise((r) => setTimeout(r, 400 + Math.random() * 600));

    if (succeeds) {
      await client.query(
        `INSERT INTO payments (order_id, amount_cents, status) VALUES ($1, $2, 'SUCCEEDED')
         ON CONFLICT (order_id) DO NOTHING`,
        [orderId, amountCents],
      );
      await emit(client, orderId, "payment.processed", {});
    } else {
      const reason = "card declined by issuing bank";
      await client.query(
        `INSERT INTO payments (order_id, amount_cents, status, failure_reason) VALUES ($1, $2, 'FAILED', $3)
         ON CONFLICT (order_id) DO NOTHING`,
        [orderId, amountCents, reason],
      );
      await emit(client, orderId, "payment.failed", { reason });
    }

    await markProcessed(client, dedupeKey);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[payment] failed to handle command:", err);
    throw err;
  } finally {
    client.release();
  }
}

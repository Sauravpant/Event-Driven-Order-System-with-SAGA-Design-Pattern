import { pool } from "./db.js";
import { publishRaw } from "./kafka.js";

const POLL_MS = Number(process.env.OUTBOX_POLL_INTERVAL_MS || 750);

interface OutboxRow {
  id: number;
  aggregate_id: string;
  topic: string;
  payload: Record<string, unknown>;
}
const tick = async (): Promise<void> => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const { rows } = await client.query<OutboxRow>(
      `SELECT * FROM outbox_events WHERE published = false ORDER BY id ASC LIMIT 20 FOR UPDATE SKIP LOCKED`,
    );
    for (const row of rows) {
      await publishRaw(row.topic, {
        eventId: `inventory-service-${row.id}`,
        orderId: row.aggregate_id,
        ...row.payload,
      });
      await client.query(
        `UPDATE outbox_events SET published = true WHERE id = $1`,
        [row.id],
      );
    }
    await client.query("COMMIT");
    if (rows.length) console.log(`[outbox] relayed ${rows.length} event(s)`);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("[outbox] relay tick failed:", (err as Error).message);
  } finally {
    client.release();
  }
};

export function startOutboxRelay(): void {
  setInterval(() => {
    tick().catch((err) => console.error("[outbox] unexpected error:", err));
  }, POLL_MS);
  console.log(`[outbox] relay started, polling every ${POLL_MS}ms`);
}

import "dotenv/config";
import express from "express";
import { pool } from "./db.js";
import { startOutboxRelay } from "./outbox.js";
import { startConsumer } from "./kafka.js";
import { handlePaymentCommand } from "./consumers/paymentConsumer.js";
import type { Payment } from "./types.js";

const app = express();
app.get("/health", (_req, res) => res.json({ status: "ok", service: "payment-service" }));
app.get("/internal/payments/:orderId", async (req, res) => {
  const { rows } = await pool.query<Payment>(`SELECT * FROM payments WHERE order_id = $1`, [
    req.params.orderId,
  ]);
  if (!rows[0]) return res.status(404).json({ error: "not found" });
  res.json(rows[0]);
});

const PORT = Number(process.env.PORT) || 4003;

async function main(): Promise<void> {
  await startConsumer(handlePaymentCommand);
  startOutboxRelay();
  app.listen(PORT, () => console.log(`[payment-service] listening on ${PORT}`));
}

main().catch((err) => {
  console.error("[payment-service] fatal startup error:", err);
  process.exit(1);
});

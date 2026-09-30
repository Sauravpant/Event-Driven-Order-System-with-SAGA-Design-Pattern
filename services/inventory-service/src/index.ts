import "dotenv/config";
import express from "express";
import cors from "cors";
import { pool } from "./db.js";
import { startOutboxRelay } from "./outbox.js";
import { startConsumer } from "./kafka.js";
import { handleInventoryCommand } from "./consumers/inventoryConsumer.js";
import type { InventoryItem } from "./types.js";

const app = express();
app.use(cors());
app.use(express.json());

app.get("/health", (_req, res) =>
  res.json({ status: "ok", service: "inventory-service" }),
);

app.get("/internal/products", async (_req, res) => {
  const { rows } = await pool.query<InventoryItem>(
    `SELECT product_id, name, price_cents, available_qty FROM inventory ORDER BY product_id`,
  );
  res.json(rows);
});

app.get("/internal/products/:id", async (req, res) => {
  const { rows } = await pool.query<InventoryItem>(
    `SELECT * FROM inventory WHERE product_id = $1`,
    [req.params.id],
  );
  if (!rows[0]) return res.status(404).json({ error: "not found" });
  res.json(rows[0]);
});

const PORT = Number(process.env.PORT) || 4002;

async function main(): Promise<void> {
  await startConsumer(handleInventoryCommand);
  startOutboxRelay();
  app.listen(PORT, () =>
    console.log(`[inventory-service] listening on ${PORT}`),
  );
}

main().catch((err) => {
  console.error("[inventory-service] fatal startup error:", err);
  process.exit(1);
});

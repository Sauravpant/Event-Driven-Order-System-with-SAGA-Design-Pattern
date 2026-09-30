import { Router, type Request, type Response } from "express";
import { pool } from "../db.js";
import type { Order, TimelineEntry } from "../types.js";

const router = Router();
const INVENTORY_URL =
  process.env.INVENTORY_SERVICE_URL || "http://inventory-service:4002";

interface Product {
  product_id: string;
  price_cents: number;
}

router.post("/", async (req: Request, res: Response) => {
  const { productId, quantity } = req.body as {
    productId?: string;
    quantity?: number;
  };
  if (!productId || !quantity || quantity < 1) {
    return res
      .status(400)
      .json({ error: "productId and quantity (>=1) are required" });
  }

  let product: Product;
  try {
    const r = await fetch(`${INVENTORY_URL}/internal/products/${productId}`);
    if (!r.ok) return res.status(404).json({ error: "unknown product" });
    product = (await r.json()) as Product;
  } catch {
    return res.status(503).json({ error: "inventory-service unavailable" });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const amountCents = product.price_cents * quantity;
    const { rows } = await client.query<Order>(
      `INSERT INTO orders (product_id, quantity, amount_cents, saga_state)
       VALUES ($1, $2, $3, 'STARTED') RETURNING *`,
      [productId, quantity, amountCents],
    );
    const order = rows[0];

    await client.query(
      `INSERT INTO saga_timeline (order_id, step, detail) VALUES ($1, 'STARTED', 'Order created, requesting inventory reservation')`,
      [order.id],
    );

    await client.query(
      `INSERT INTO outbox_events (aggregate_id, topic, payload) VALUES ($1, 'order.created', $2)`,
      [order.id, JSON.stringify({ productId, quantity })],
    );

    await client.query("COMMIT");
    res.status(201).json(order);
  } catch (err) {
    await client.query("ROLLBACK");
    console.error(err);
    res.status(500).json({ error: "failed to create order" });
  } finally {
    client.release();
  }
});

router.get("/", async (_req: Request, res: Response) => {
  const { rows } = await pool.query<Order>(
    `SELECT * FROM orders ORDER BY created_at DESC LIMIT 50`,
  );
  res.json(rows);
});

router.get("/:id", async (req: Request, res: Response) => {
  const orderRes = await pool.query<Order>(
    `SELECT * FROM orders WHERE id = $1`,
    [req.params.id],
  );
  if (!orderRes.rows[0]) return res.status(404).json({ error: "not found" });
  const timelineRes = await pool.query<TimelineEntry>(
    `SELECT step, detail, created_at FROM saga_timeline WHERE order_id = $1 ORDER BY id ASC`,
    [req.params.id],
  );
  res.json({ ...orderRes.rows[0], timeline: timelineRes.rows });
});

export default router;

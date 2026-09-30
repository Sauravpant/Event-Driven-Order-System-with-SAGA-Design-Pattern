import type { Order, OrderWithTimeline, Product } from "./types";

const BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:4000";

export async function fetchProducts(): Promise<Product[]> {
  const r = await fetch(`${BASE_URL}/api/products`);
  if (!r.ok) throw new Error("failed to load products");
  return r.json() as Promise<Product[]>;
}

export async function createOrder(productId: string, quantity: number): Promise<Order> {
  const r = await fetch(`${BASE_URL}/api/orders`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ productId, quantity }),
  });
  if (!r.ok) {
    const err = (await r.json().catch(() => ({}))) as { error?: string };
    throw new Error(err.error || "failed to create order");
  }
  return r.json() as Promise<Order>;
}

export async function fetchOrder(orderId: string): Promise<OrderWithTimeline> {
  const r = await fetch(`${BASE_URL}/api/orders/${orderId}`);
  if (!r.ok) throw new Error("failed to load order");
  return r.json() as Promise<OrderWithTimeline>;
}

export async function fetchOrders(): Promise<Order[]> {
  const r = await fetch(`${BASE_URL}/api/orders`);
  if (!r.ok) throw new Error("failed to load orders");
  return r.json() as Promise<Order[]>;
}

export const INBOUND_TOPICS = ["order.created", "inventory.release"] as const;
export type InboundTopic = (typeof INBOUND_TOPICS)[number];

export interface SagaEventPayload {
  eventId: string;
  orderId: string;
  productId?: string;
  quantity?: number;
  [key: string]: unknown;
}

export interface InventoryItem {
  product_id: string;
  name: string;
  price_cents: number;
  available_qty: number;
  reserved_qty: number;
}

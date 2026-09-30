export type SagaState =
  | "STARTED"
  | "INVENTORY_RESERVED"
  | "COMPENSATING"
  | "CONFIRMED"
  | "FAILED";

export interface Order {
  id: string;
  product_id: string;
  quantity: number;
  amount_cents: number;
  saga_state: SagaState;
  failure_reason: string | null;
  created_at: string;
  updated_at: string;
}

export interface TimelineEntry {
  step: string;
  detail: string | null;
  created_at: string;
}

export const INBOUND_TOPICS = [
  "inventory.reserved",
  "inventory.failed",
  "inventory.released",
  "payment.processed",
  "payment.failed",
] as const;

export type InboundTopic = (typeof INBOUND_TOPICS)[number];

export interface SagaEventPayload {
  eventId: string;
  orderId: string;
  reason?: string;
  [key: string]: unknown;
}

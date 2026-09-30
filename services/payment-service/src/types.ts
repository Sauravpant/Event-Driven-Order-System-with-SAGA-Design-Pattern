export const INBOUND_TOPICS = ["payment.process"] as const;
export type InboundTopic = (typeof INBOUND_TOPICS)[number];

export interface SagaEventPayload {
  eventId: string;
  orderId: string;
  amountCents?: number;
  [key: string]: unknown;
}

export interface Payment {
  order_id: string;
  amount_cents: number;
  status: "SUCCEEDED" | "FAILED";
  failure_reason: string | null;
  created_at: string;
}

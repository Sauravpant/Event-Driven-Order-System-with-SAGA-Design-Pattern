export type SagaState = "STARTED" | "INVENTORY_RESERVED" | "COMPENSATING" | "CONFIRMED" | "FAILED";

export interface Product {
  product_id: string;
  name: string;
  price_cents: number;
  available_qty: number;
}

export interface TimelineStep {
  step: string;
  detail: string | null;
  created_at: string;
}

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

export interface OrderWithTimeline extends Order {
  timeline: TimelineStep[];
}

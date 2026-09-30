import { useEffect, useRef, useState } from "react";
import { fetchProducts, createOrder, fetchOrder, fetchOrders } from "./api";
import type { Order, OrderWithTimeline, Product, SagaState, TimelineStep } from "./types";

const TERMINAL_STATES = new Set<SagaState>(["CONFIRMED", "FAILED"]);

const STATE_META: Record<SagaState, { label: string; color: string }> = {
  STARTED: { label: "Order Started", color: "#94a3b8" },
  INVENTORY_RESERVED: { label: "Inventory Reserved", color: "#3b82f6" },
  COMPENSATING: { label: "Compensating", color: "#f59e0b" },
  CONFIRMED: { label: "Confirmed", color: "#22c55e" },
  FAILED: { label: "Failed", color: "#ef4444" },
};

function money(cents: number): string {
  return `$${(cents / 100).toFixed(2)}`;
}

function StatusBadge({ state }: { state: SagaState }) {
  const meta = STATE_META[state] ?? { label: state, color: "#94a3b8" };
  return (
    <span className="badge" style={{ background: meta.color }}>
      {meta.label}
    </span>
  );
}

function Timeline({ steps }: { steps: TimelineStep[] }) {
  return (
    <ol className="timeline">
      {steps.map((s, i) => (
        <li key={i}>
          <div
            className="dot"
            style={{ background: STATE_META[s.step as SagaState]?.color ?? "#94a3b8" }}
          />
          <div>
            <div className="timeline-step">{STATE_META[s.step as SagaState]?.label ?? s.step}</div>
            {s.detail && <div className="timeline-detail">{s.detail}</div>}
          </div>
        </li>
      ))}
    </ol>
  );
}

export default function App() {
  const [products, setProducts] = useState<Product[]>([]);
  const [productId, setProductId] = useState("");
  const [quantity, setQuantity] = useState(1);
  const [activeOrder, setActiveOrder] = useState<OrderWithTimeline | null>(null);
  const [orders, setOrders] = useState<Order[]>([]);
  const [error, setError] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const pollRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    fetchProducts()
      .then((p) => {
        setProducts(p);
        if (p.length) setProductId(p[0].product_id);
      })
      .catch((e: Error) => setError(e.message));
    refreshOrders();
    return () => stopPolling();
  }, []);

  function refreshOrders(): void {
    fetchOrders().then(setOrders).catch(() => {});
  }

  function stopPolling(): void {
    if (pollRef.current) clearInterval(pollRef.current);
    pollRef.current = null;
  }

  function pollOrder(orderId: string): void {
    stopPolling();
    pollRef.current = setInterval(() => {
      fetchOrder(orderId)
        .then((order) => {
          setActiveOrder(order);
          if (TERMINAL_STATES.has(order.saga_state)) {
            stopPolling();
            refreshOrders();
          }
        })
        .catch(() => stopPolling());
    }, 800);
  }

  async function handleSubmit(e: React.FormEvent): Promise<void> {
    e.preventDefault();
    setError("");
    setSubmitting(true);
    try {
      const order = await createOrder(productId, Number(quantity));
      const full = await fetchOrder(order.id);
      setActiveOrder(full);
      pollOrder(order.id);
      refreshOrders();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSubmitting(false);
    }
  }

  const selectedProduct = products.find((p) => p.product_id === productId);

  return (
    <div className="page">
      <header>
        <h1>Saga Order System</h1>
        <p className="subtitle">
          Orchestration-based saga · transactional outbox · Kafka · idempotent consumers · 3 independent
          services
        </p>
      </header>

      <div className="layout">
        <section className="card">
          <h2>Place an order</h2>
          <form onSubmit={handleSubmit}>
            <label>
              Product
              <select value={productId} onChange={(e) => setProductId(e.target.value)}>
                {products.map((p) => (
                  <option key={p.product_id} value={p.product_id}>
                    {p.name} — {money(p.price_cents)} ({p.available_qty} in stock)
                  </option>
                ))}
              </select>
            </label>
            <label>
              Quantity
              <input
                type="number"
                min={1}
                value={quantity}
                onChange={(e) => setQuantity(Number(e.target.value))}
              />
            </label>
            {selectedProduct && (
              <p className="hint">Total: {money(selectedProduct.price_cents * (quantity || 0))}</p>
            )}
            <button type="submit" disabled={submitting || !productId}>
              {submitting ? "Placing order..." : "Place Order"}
            </button>
          </form>
          {error && <p className="error">{error}</p>}

          <p className="hint">
            Tip: order "Sold Out Item" to see the inventory-failure branch, or place a normal order a
            few times to see payment fail (~25% of orders) and trigger compensation.
          </p>
        </section>

        <section className="card">
          <h2>
            Saga timeline
            {activeOrder && <StatusBadge state={activeOrder.saga_state} />}
          </h2>
          {!activeOrder && <p className="hint">Place an order to watch its saga run live.</p>}
          {activeOrder && (
            <>
              <p className="order-meta">
                Order <code>{activeOrder.id.slice(0, 8)}</code> · {activeOrder.quantity}×{" "}
                {activeOrder.product_id} · {money(activeOrder.amount_cents)}
              </p>
              {activeOrder.failure_reason && <p className="error">Reason: {activeOrder.failure_reason}</p>}
              <Timeline steps={activeOrder.timeline} />
            </>
          )}
        </section>
      </div>

      <section className="card">
        <h2>Recent orders</h2>
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Product</th>
              <th>Qty</th>
              <th>Amount</th>
              <th>State</th>
            </tr>
          </thead>
          <tbody>
            {orders.map((o) => (
              <tr
                key={o.id}
                className="clickable"
                onClick={() => {
                  fetchOrder(o.id).then(setActiveOrder);
                  if (!TERMINAL_STATES.has(o.saga_state)) pollOrder(o.id);
                }}
              >
                <td>
                  <code>{o.id.slice(0, 8)}</code>
                </td>
                <td>{o.product_id}</td>
                <td>{o.quantity}</td>
                <td>{money(o.amount_cents)}</td>
                <td>
                  <StatusBadge state={o.saga_state} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}

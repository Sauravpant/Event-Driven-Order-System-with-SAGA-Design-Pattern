import "dotenv/config";
import express from "express";
import cors from "cors";
import ordersRouter from "./routes/orders.js";
import { startOutboxRelay } from "./outbox.js";
import { startConsumer } from "./kafka.js";
import { handleSagaEvent } from "./saga/orderSaga.js";

const app = express();
app.use(cors());
app.use(express.json());

app.get("/health", (_req, res) => res.json({ status: "ok", service: "order-service" }));
app.use("/orders", ordersRouter);

const PORT = Number(process.env.PORT) || 4001;

async function main(): Promise<void> {
  await startConsumer(handleSagaEvent);
  startOutboxRelay();
  app.listen(PORT, () => console.log(`[order-service] listening on ${PORT}`));
}

main().catch((err) => {
  console.error("[order-service] fatal startup error:", err);
  process.exit(1);
});

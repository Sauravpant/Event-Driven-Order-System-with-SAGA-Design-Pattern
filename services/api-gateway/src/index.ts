import "dotenv/config";
import express from "express";
import cors from "cors";
import { createProxyMiddleware } from "http-proxy-middleware";

const ORDER_SERVICE_URL = process.env.ORDER_SERVICE_URL || "http://order-service:4001";
const INVENTORY_SERVICE_URL = process.env.INVENTORY_SERVICE_URL || "http://inventory-service:4002";

const app = express();
app.use(cors());

app.get("/health", (_req, res) => res.json({ status: "ok", service: "api-gateway" }));

app.use(
  createProxyMiddleware({
    target: ORDER_SERVICE_URL,
    changeOrigin: true,
    pathFilter: "/api/orders",
    pathRewrite: { "^/api/orders": "/orders" },
  })
);

app.use(
  createProxyMiddleware({
    target: INVENTORY_SERVICE_URL,
    changeOrigin: true,
    pathFilter: "/api/products",
    pathRewrite: { "^/api/products": "/internal/products" },
  })
);

const PORT = Number(process.env.PORT) || 4000;
app.listen(PORT, () => console.log(`[api-gateway] listening on ${PORT}`));

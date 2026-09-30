import pg from "../node_modules/@types/pg/index.js/node_modules/@types/pg/index.js";

export const pool = new pg.Pool({
  host: process.env.PGHOST || "order-db",
  port: 5432,
  user: process.env.POSTGRES_USER || "saga",
  password: process.env.POSTGRES_PASSWORD || "saga_pw",
  database: process.env.ORDER_DB_NAME || "order_db",
});

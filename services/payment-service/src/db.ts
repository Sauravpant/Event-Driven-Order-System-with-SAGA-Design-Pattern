import pg from "pg";

export const pool = new pg.Pool({
  host: process.env.PGHOST || "payment-db",
  port: 5432,
  user: process.env.POSTGRES_USER || "saga",
  password: process.env.POSTGRES_PASSWORD || "saga_pw",
  database: process.env.PAYMENT_DB_NAME || "payment_db",
});

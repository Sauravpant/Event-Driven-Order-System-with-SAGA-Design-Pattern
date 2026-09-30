# Saga Order System

A small order-processing system split into three microservices: order, inventory, and payment. Each
service has its own database and they only talk to each other over Kafka. I built this to
understand distributed transactions in practice instead of only reading about the saga pattern.
There is a longer write-up about the pattern in [`saga.md`](./saga.md).

There is no two-phase commit and no shared database. The main pieces are an orchestrator, a
message log, and a rollback plan for when something downstream fails.

## The problem

Placing an order touches three services:

1. **order-service** creates the order
2. **inventory-service** has to reserve stock for it
3. **payment-service** has to charge the customer

These are three separate databases, so they cannot all be wrapped in one ACID transaction without
using something like a distributed transaction coordinator. That is not something I wanted to run
here. Instead, `order-service` acts as the orchestrator: it sends a command, waits for the result,
and decides what happens next.

```
order-service ──"reserve stock"──▶ inventory-service ──"charge card"──▶ payment-service
      ▲                                    │                                  │
      └──────── events come back this way ─┴──────────────────────────────────┘
```

If inventory cannot be reserved, the flow stops and payment is never called. If inventory reserves
successfully but the card is declined, the reservation has to be undone. That compensation step
is the part most tutorials skip, and it is the part that makes this project interesting.

## How it's wired together

```
Frontend (5173)
     │
API Gateway (4000)  ← the only thing exposed outside the docker network
     │            │
order-service   inventory-service
  (4001)            (4002)
     │                │
 order-db        inventory-db
     │                │
     └──────┐   ┌─────┘
          Kafka (single broker, KRaft mode)
              │
        payment-service (4003)
              │
         payment-db
```

Every service has its own outbox table and a poller that reads it and publishes to Kafka. That is
not shown in the diagram, but it is probably the most important part of the whole setup. More on
that below.

`kafka-ui` runs on `:8080` and makes it possible to watch messages move between topics while
testing. It is useful for demos because seeing the messages move is more convincing than only
describing the flow.

## The two patterns doing the actual work

**Transactional outbox.** Every write that also needs to emit an event does both in one DB
transaction: the business row and a row in `outbox_events` are committed together. A background
loop reads unpublished outbox rows and pushes them to Kafka. Writing to the DB and then calling
`kafka.publish()` separately leaves a window where one succeeds and the other fails. The outbox
closes that window by keeping them in the same database transaction.

**Saga (orchestrated, not choreographed).** `order-service` holds the state machine and tells the
other two services what to do. It is not a group of services reacting to events with no central
owner. Choreography works fine for two services, but with three or more services it becomes harder
to trace what happened to one order without searching logs everywhere. Keeping the flow in one
orchestrator made this easier to build and explain.

There are also a couple of things needed to make those two patterns work safely:

- **Idempotent consumers.** Kafka provides at-least-once delivery, not exactly-once. A rebalance
  or a restart can redeliver a message. Every consumer checks a `processed_events` table keyed by
  the producer's outbox row id before doing anything, so a redelivered "charge the card" command
  cannot charge it twice.
- **Keying by orderId.** Every message is published with `key: orderId`, so all events for one
  order land on the same Kafka partition and arrive in order. Without this, `payment.processed`
  could theoretically arrive before `inventory.reserved` for the same order and leave the state
  machine in an impossible state.

## Stack

TypeScript everywhere (`strict: true`), Express, raw SQL through `pg`, Kafka via `kafkajs` in
single-broker KRaft mode, React + Vite for the frontend, and Docker Compose for the whole setup. I
used raw SQL because I did not want an ORM hiding the transaction boundaries. Being explicit about
where transactions start and end is the point of this project.

## Project layout

```
saga-order-system/
├── docker-compose.yml
├── .env.example
├── README.md
├── saga.md                    ← saga pattern write-up
├── services/
│   ├── order-service/         (the orchestrator)
│   │   ├── db/init.sql
│   │   └── src/
│   │       ├── index.ts
│   │       ├── db.ts
│   │       ├── kafka.ts
│   │       ├── outbox.ts
│   │       ├── types.ts
│   │       ├── routes/orders.ts
│   │       └── saga/orderSaga.ts   ← the actual state machine
│   ├── inventory-service/
│   │   ├── db/init.sql
│   │   └── src/consumers/inventoryConsumer.ts
│   ├── payment-service/
│   │   ├── db/init.sql
│   │   └── src/consumers/paymentConsumer.ts
│   └── api-gateway/
│       └── src/index.ts
└── frontend/
    └── src/
        ├── App.tsx
        ├── api.ts
        └── types.ts
```

Every backend service follows roughly the same shape: `db.ts`, `kafka.ts`, `outbox.ts`, a
consumer/handler file, and `index.ts` to wire everything up. After reading one service, the other
two are not too different.

## Running it

```bash
cd saga-order-system
cp .env.example .env
docker compose up --build
```

The first run takes a while because there are three separate `npm install` and `tsc` builds, plus
Kafka has to format its storage the first time it starts. After that, `docker compose up --build`
is much quicker.

| what                       | where                 |
| -------------------------- | --------------------- |
| frontend                   | http://localhost:5173 |
| api gateway                | http://localhost:4000 |
| kafka ui                   | http://localhost:8080 |
| order-service directly     | http://localhost:4001 |
| inventory-service directly | http://localhost:4002 |
| payment-service directly   | http://localhost:4003 |

When an order is placed from the frontend, it polls every 800ms so the saga state can be watched
live: `STARTED` → `INVENTORY_RESERVED` → `CONFIRMED`, or `COMPENSATING` → `FAILED` if the card is
declined.

`docker compose down -v` wipes everything, including the Postgres volumes and Kafka log, so it is
useful for starting clean.

## API

All API requests go through the gateway at `:4000`.

**`POST /api/orders`**

```bash
curl -X POST http://localhost:4000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"productId": "sku-widget", "quantity": 2}'
```

`productId` must exist in the catalog and `quantity` must be at least 1. The endpoint returns the
created order with `saga_state: "STARTED"`, 404 if the product does not exist, and 503 if
inventory-service is down. Order creation looks up the price synchronously. That is only a read,
not part of the saga, so calling it directly instead of going through Kafka is fine.

**`GET /api/orders`** — last 50 orders.

**`GET /api/orders/:id`** — one order plus its full timeline (every step the saga went through,
in order, with the little detail string logged at each step).

**`GET /api/products`** — the catalog.

## What flows through Kafka

Every message is JSON, keyed by `orderId`.

| topic                | who sends it      | who reads it      | what it means                      |
| -------------------- | ----------------- | ----------------- | ---------------------------------- |
| `order.created`      | order-service     | inventory-service | reserve stock                      |
| `inventory.reserved` | inventory-service | order-service     | stock reserved, go ahead           |
| `inventory.failed`   | inventory-service | order-service     | not enough stock / bad product id  |
| `payment.process`    | order-service     | payment-service   | charge the card                    |
| `payment.processed`  | payment-service   | order-service     | charge succeeded                   |
| `payment.failed`     | payment-service   | order-service     | charge declined                    |
| `inventory.release`  | order-service     | inventory-service | compensating: undo the reservation |
| `inventory.released` | inventory-service | order-service     | compensation done                  |

Every payload carries `eventId`, which is the sender's outbox row id and the deduplication key, as
well as `orderId`. Watching the topics in kafka-ui while placing an order is a good way to see the
saga happening instead of only trusting the code.

## Databases

Three separate Postgres instances, no cross-service foreign keys, ever.

- **order-db** — `orders`, `saga_timeline`, plus `outbox_events` / `processed_events`
- **inventory-db** — `inventory` (seeded with 4 SKUs, one deliberately at 0 stock), `reservations`,
  plus the outbox/idempotency tables
- **payment-db** — `payments`, plus the outbox/idempotency tables

## Env vars

Copy `.env.example` to `.env` before running. The main settings are:

- `PAYMENT_FAILURE_RATE` (default `0.25`) — payment-service randomly declines this fraction of
  charges on purpose, so the compensation path fires without needing to force it
- `OUTBOX_POLL_INTERVAL_MS` (default `750`) — how often each service checks its outbox for
  unpublished rows

## Forcing the failure paths

Inventory failure (order never even reaches payment):

```bash
curl -X POST http://localhost:4000/api/orders \
  -H "Content-Type: application/json" \
  -d '{"productId": "sku-out-of-stock", "quantity": 1}'
```

Payment failure / compensation: place a handful of normal orders from the UI. At a 25% failure
rate, one should reach `COMPENSATING` within a few tries. The `inventory.release` topic in kafka-ui
shows the compensation when it happens.

## Troubleshooting notes to self

- If services keep bouncing on startup, Kafka is probably still formatting its KRaft storage. Give
  it 10-15 seconds; the consumers retry on their own.
- Compose does not rebuild an image just because a file changed. Use `--build` after a code change.
- The frontend calls `localhost:4000` from the browser, not from inside a container. Changing it to
  the Docker service name breaks the browser setup.

## Kafka vs RabbitMQ, briefly

I chose Kafka mainly because messages stay on disk instead of disappearing as soon as they are
acked. That means a new consumer could be added later for analytics, an audit log, or something
similar, and it could replay the full order history. RabbitMQ would have been simpler for a
project this size, and its per-message routing and dead-lettering are nicer for smaller pipelines.
Kafka is more common at real scale, though, so it felt more useful to build with here.

## Things I know are missing

This is not production-ready. Before sending it anywhere near real traffic, I would add:

- no dead-letter queue — a message that throws just gets logged and dropped, not parked anywhere
  for retry
- no timeout on the saga itself — if a service never responds, the order just sits in
  `INVENTORY_RESERVED` forever
- single Kafka broker, replication factor 1 — fine for a demo, not fine for anything real
- zero auth on the gateway
- topics auto-create on first publish instead of being provisioned up front with real partition
  counts

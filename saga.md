## The problem SAGA solves

In a monolith, "create an order, reserve stock, charge a card" can be one database transaction. If
one step fails, the transaction rolls back and it is as if nothing happened.

Splitting those three things into three services with three databases removes that convenience.
There is no single transaction to roll back. By the time payment fails, the inventory reservation
has already committed in a different database. It is possible to wrap Postgres A, Postgres B, and
Postgres C in one ACID transaction with a distributed transaction coordinator (two-phase commit),
but 2PC has a bad reputation for good reasons. Every participant needs to be available and
responsive, locks are held while waiting for the slowest participant, and if the coordinator dies
mid-commit, participants can get stuck not knowing whether to commit or abort.

The saga pattern is the alternative. It does not pretend the whole process is one transaction. It
breaks the process into local transactions, each of which commits on its own, and each step has a
defined meaning of "undo." If step 3 fails, the whole world is not rolled back. Instead,
compensating actions run for steps 1 and 2 that already succeeded.

So instead of atomicity, the result is **eventual consistency with a defined recovery path**. An
order exists briefly in an "in progress" state that is visible to the rest of the system. If
something downstream fails, the system explicitly walks it back instead of pretending it never
happened.

## Orchestration vs. choreography

There are two main ways to implement the sequencing.

**Choreography** — no central coordinator. Each service listens for events from the others and
reacts. Order-service publishes `OrderCreated`. Inventory-service is just subscribed to that event
type and reacts to it on its own, publishing `InventoryReserved` or `InventoryFailed` when it's
done. Payment-service is subscribed to `InventoryReserved` and reacts to that. Nobody's "in
charge" — the flow emerges from everyone reacting to everyone else.

This is nice for two services, but it gets harder to manage past that because:

- there's no single place to look to answer "what's the current state of order X's saga" — the
  state has to be reconstructed from events scattered across every service's logs
- adding a new step means touching the service that comes right before it in the chain, which
  quietly couples services that are supposed to be independent
- the failure/compensation logic ends up smeared across every service instead of living in one
  place

**Orchestration** — one service (or a dedicated orchestrator) owns the state machine and tells the
other services what to do, waiting for a response before deciding the next step. That is what this
repo does: `order-service` sends "reserve inventory," waits, and then either sends "charge payment"
or records failure. The orchestrator knows the whole flow.

The downside is obvious: the orchestrator needs to know about every other service in the flow and
does more work than the others. Still, for anything more than a couple of steps, having one file
that shows the entire order lifecycle is worth that coupling. That is why I used orchestration
here.

There is no universally correct answer. Plenty of real systems use choreography for simpler two- or
three-hop flows and move to orchestration once the flow has enough steps and failure branches that
the whole thing is difficult to understand from scattered event handlers.

## Compensating transactions

The part that is easy to skip when learning this is that a compensating transaction is not the
same as a rollback. A database rollback undoes a transaction that has not committed yet. A
compensating transaction is a brand new, separately committed action that semantically undoes the
effect of a transaction that already committed successfully.

In this repo, if inventory gets reserved and then payment fails, the reservation cannot be rolled
back. It is already committed, possibly minutes ago, in a completely different database. A new
command, `inventory.release`, is issued and inventory-service processes it as its own transaction:
increase `available_qty` and change the reservation row to `RELEASED`. From the outside, the net
effect looks like the reservation never happened. It is not a rollback, though. It is forward
progress that happens to cancel something out.

This matters because compensations have to be designed for each step up front. They do not come
for free like database rollback. "What does undoing this step actually mean?" is a real design
question. For a stock reservation it is easy: put the stock back. For something like "sent a
confirmation email," there is no real undo. The best option might be a follow-up saying "ignore
that last email." Not every step in a saga is cleanly compensable, so this needs to be considered
before modeling something as a saga.

## Why idempotency isn't optional

Sagas are built on asynchronous messaging, and basically every real message system, including
Kafka, gives at-least-once delivery rather than exactly-once delivery. A consumer can crash after
processing a message but before committing its offset, then receive the same message again after a
restart. A consumer-group rebalance can cause something similar.

If "process this message" is not safe to run twice, at-least-once delivery will eventually cause a
problem. A card could be charged twice or stock could be reserved twice, probably rarely enough
that it does not show up until real damage has already happened. The fix is making every step
idempotent: before acting on a message, check whether this exact message has already been handled
using a unique id from the payload, and skip it if so. In this repo, that is the
`processed_events` table in every service. It is cheap and boring, but it is what makes "at least
once" behave like "effectively once."

## Ordering matters more than it seems like it should

The saga state machine only makes sense if events arrive in the order they were produced. If
`payment.processed` somehow arrived before `inventory.reserved` for the same order, the
orchestrator would be asked to act on a state it doesn't understand yet.

Kafka only guarantees ordering _within a partition_, not across an entire topic. Every message
here is published with `key: orderId`. Kafka guarantees that messages with the same key land on the
same partition, and a partition is strictly ordered. That one line (`key: order.id`) does a lot of
quiet work. Without it, events would need to be reordered per order on the consuming side, which
would be a much worse problem.

## When _not_ to reach for this

I wrote this project mainly to learn the pattern, but it is worth being honest that a saga is
genuinely more machinery than most systems need:

- if all the data legitimately fits in one database, a real transaction is enough — no need to
  microservice something into needing a saga
- if a step genuinely cannot fail in a way that needs compensation, a saga is not needed for it
- if the multi-step operation can run synchronously and return an error on failure, with no partial
  state visible, a saga is not needed there either

I would reach for it when there are real service boundaries, usually real database boundaries, and
often separate teams owning separate pieces. It makes sense when multiple steps need to survive
independently and failures partway through need a real, defined recovery path instead of just
"throw an error and hope."

## Further reading

- Chris Richardson's writeup at microservices.io is the closest thing to a canonical reference. It
  covers both orchestration and choreography with clearer diagrams than the ones I drew here.
- The original saga paper (Garcia-Molina & Salem, 1987) predates microservices entirely. It was
  about long-lived database transactions, and it is interesting how directly it maps onto a
  problem that did not really exist yet when it was written.

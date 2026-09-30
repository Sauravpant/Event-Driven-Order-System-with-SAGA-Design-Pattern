import { Kafka, logLevel, type Producer, type Consumer } from "kafkajs";
import { INBOUND_TOPICS, type SagaEventPayload } from "./types.js";

const CONSUMER_GROUP = "order-service-group";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: (process.env.KAFKA_BROKERS || "kafka:9092").split(","),
  logLevel: logLevel.NOTHING,
  retry: { retries: 20, initialRetryTime: 1000 },
});

let producer: Producer | null = null;
let consumer: Consumer | null = null;

// When KAFKA_AUTO_CREATE_TOPICS_ENABLE is on, a topic doesn't exist until
// the first request for it triggers creation — so the very next metadata
// fetch (which subscribe() issues) can briefly come back
// UNKNOWN_TOPIC_OR_PARTITION while the broker is still electing a leader
// for the new partition. That's a normal, transient race on cold start,
// not a real failure, so we retry a few times with backoff instead of
// letting it bubble up and kill the process.
async function subscribeWithRetry(
  target: Consumer,
  topics: string[],
  attempts = 15,
  delayMs = 1500
): Promise<void> {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await target.subscribe({ topics, fromBeginning: false });
      return;
    } catch (err) {
      if (attempt === attempts) throw err;
      console.warn(
        `[kafka] subscribe attempt ${attempt}/${attempts} failed (likely topic still being created), retrying in ${delayMs}ms:`,
        (err as Error).message
      );
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
}

export async function connectProducer(): Promise<Producer> {
  if (producer) return producer;
  producer = kafka.producer();
  await producer.connect();
  console.log("[kafka] producer connected");
  return producer;
}

// Topic name doubles as the message key's semantic namespace; we key every
// message by orderId so all events for one order land on the same
// partition and are delivered to a given consumer in the order they were
// produced — that ordering guarantee is what makes the saga state machine
// safe to reason about.
export async function publishRaw(
  topic: string,
  payload: SagaEventPayload
): Promise<void> {
  const p = await connectProducer();
  await p.send({
    topic,
    messages: [{ key: payload.orderId, value: JSON.stringify(payload) }],
  });
}

export async function startConsumer(
  onMessage: (topic: string, payload: SagaEventPayload) => Promise<void>
): Promise<void> {
  consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();
  await subscribeWithRetry(consumer, [...INBOUND_TOPICS]);

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      try {
        const payload = JSON.parse(message.value.toString()) as SagaEventPayload;
        await onMessage(topic, payload);
      } catch (err) {
        // A poison message would otherwise wedge this partition forever
        // (Kafka has no per-message nack/dead-letter built in like AMQP —
        // committing the offset is what "acks" it). We log loudly and move
        // on rather than block the whole saga pipeline.
        console.error(`[kafka] failed to process message on ${topic}, skipping:`, err);
      }
    },
  });

  console.log(`[kafka] order-service consuming group=${CONSUMER_GROUP} topics=[${INBOUND_TOPICS.join(", ")}]`);
}

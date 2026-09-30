import { Kafka, logLevel, type Producer, type Consumer } from "kafkajs";
import { INBOUND_TOPICS, type SagaEventPayload } from "./types.js";

const CONSUMER_GROUP = "inventory-service-group";

const kafka = new Kafka({
  clientId: "inventory-service",
  brokers: (process.env.KAFKA_BROKERS || "kafka:9092").split(","),
  logLevel: logLevel.NOTHING,
  retry: { retries: 20, initialRetryTime: 1000 },
});

let producer: Producer | null = null;
let consumer: Consumer | null = null;

async function subscribeWithRetry(
  target: Consumer,
  topics: string[],
  attempts = 15,
  delayMs = 1500,
): Promise<void> {
  for (let attempt = 1; attempt <= attempts; attempt++) {
    try {
      await target.subscribe({ topics, fromBeginning: false });
      return;
    } catch (err) {
      if (attempt === attempts) throw err;
      console.warn(
        `[kafka] subscribe attempt ${attempt}/${attempts} failed (likely topic still being created), retrying in ${delayMs}ms:`,
        (err as Error).message,
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

export async function publishRaw(
  topic: string,
  payload: SagaEventPayload,
): Promise<void> {
  const p = await connectProducer();
  await p.send({
    topic,
    messages: [{ key: payload.orderId, value: JSON.stringify(payload) }],
  });
}

export async function startConsumer(
  onMessage: (topic: string, payload: SagaEventPayload) => Promise<void>,
): Promise<void> {
  consumer = kafka.consumer({ groupId: CONSUMER_GROUP });
  await consumer.connect();
  await subscribeWithRetry(consumer, [...INBOUND_TOPICS]);

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      try {
        const payload = JSON.parse(
          message.value.toString(),
        ) as SagaEventPayload;
        await onMessage(topic, payload);
      } catch (err) {
        console.error(
          `[kafka] failed to process message on ${topic}, skipping:`,
          err,
        );
      }
    },
  });

  console.log(
    `[kafka] inventory-service consuming group=${CONSUMER_GROUP} topics=[${INBOUND_TOPICS.join(", ")}]`,
  );
}

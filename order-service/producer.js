import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "order",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();
await producer.connect();

const runProducer = async () => {
  await producer.connect();
  try {
    await producer.send({
      topic: "orders",
      messages: [
        {
          key: "order1",
          value: JSON.stringify({
            orderId: "order1",
            userId: "user42",
            items: [
              { productId: "p1", quantity: 2 },
              { productId: "p2", quantity: 1 },
            ],
            total: 499,
            status: "created",
            createdAt: "2025-07-30T18:00:00Z",
          }),
        },
      ],
    });
    console.log("✅ Messages sent successfully");
  } catch (error) {
    console.error("Producer failed to send message:", error);
  }

  await producer.disconnect();
};

runProducer().catch((e) => console.error(`❌ Producer error: ${e.message}`, e));

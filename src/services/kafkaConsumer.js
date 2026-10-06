const { kafka } = require("../config/kafka");

const consumer = kafka.consumer({
  groupId: "payment-wallet-consumer",
});

async function startKafkaConsumer() {
  await consumer.connect();

  console.log("Kafka consumer connected");

  await consumer.subscribe({
    topic: "payment-events",
    fromBeginning: true,
  });

  console.log("Kafka consumer subscribed to payment-events");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());

      console.log("\n===== PAYMENT EVENT RECEIVED =====");

      console.log("Topic:", topic);
      console.log("Partition:", partition);
      console.log("Event:", event);

      console.log("==================================\n");
    },
  });
}

module.exports = {
  startKafkaConsumer,
};
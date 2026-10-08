const { kafka } = require("../config/kafka");
const pool = require("../config/db");

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

      const eventId = event.transactionId;
      const eventType = event.event || "UNKNOWN";

      console.log("\n===== PAYMENT EVENT RECEIVED =====");

      console.log("Topic:", topic);
      console.log("Partition:", partition);
      console.log("Event:", event);

      try {
        const result = await pool.query(
          `INSERT INTO processed_events
           (event_id, event_type)
           VALUES ($1, $2)
           ON CONFLICT (event_id) DO NOTHING
           RETURNING *`,
          [eventId, eventType]
        );

        if (result.rows.length === 0) {
          console.log(
            `Duplicate event ignored: ${eventId}`
          );

          console.log("==================================\n");

          return;
        }

        console.log(
          `Event processed successfully: ${eventId}`
        );

        console.log("==================================\n");
      } catch (error) {
        console.error(
          "Failed to process Kafka event:",
          error
        );

        throw error;
      }
    },
  });
}

module.exports = {
  startKafkaConsumer,
};
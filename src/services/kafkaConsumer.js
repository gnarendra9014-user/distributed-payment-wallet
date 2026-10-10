const { kafka } = require("../config/kafka");
const pool = require("../config/db");

const consumer = kafka.consumer({
  groupId: "payment-wallet-consumer",
});

async function handlePaymentMessage({ topic, partition, message }) {
  let event;
  try {
    event = JSON.parse(message.value.toString());
  } catch (parseError) {
    console.error("Failed to parse Kafka message JSON:", parseError.message);
    return;
  }

  // Use unique outboxEventId when available to allow distinct events for same transaction (e.g. transfer vs refund)
  const eventId = event.outboxEventId || event.eventId || event.transactionId;
  const eventType = event.event || event.eventType || "UNKNOWN";

  console.log("\n===== PAYMENT EVENT RECEIVED =====");
  console.log("Topic:", topic);
  console.log("Partition:", partition);
  console.log("Event ID:", eventId);
  console.log("Event Type:", eventType);
  console.log("Event Data:", event);

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
      console.log(`Duplicate event ignored: ${eventId}`);
      console.log("==================================\n");
      return;
    }

    console.log(`Event processed successfully: ${eventId}`);
    console.log("==================================\n");
  } catch (error) {
    console.error("Failed to record processed Kafka event:", error.message);
    throw error;
  }
}

async function startKafkaConsumer() {
  await consumer.connect();
  console.log("Kafka consumer connected");

  await consumer.subscribe({
    topic: "payment-events",
    fromBeginning: true,
  });

  console.log("Kafka consumer subscribed to payment-events");

  await consumer.run({
    eachMessage: handlePaymentMessage,
  });
}

module.exports = {
  startKafkaConsumer,
  handlePaymentMessage,
};
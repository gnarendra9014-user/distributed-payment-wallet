const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "payment-wallet-api",
  brokers: [process.env.KAFKA_BROKER || "localhost:9092"],
});

const producer = kafka.producer();

module.exports = {
  kafka,
  producer,
};
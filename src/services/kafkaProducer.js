const { producer } = require("../config/kafka");

let isConnected = false;

producer.on(producer.events.CONNECT, () => {
  isConnected = true;
});

producer.on(producer.events.DISCONNECT, () => {
  isConnected = false;
});

async function ensureProducerConnected() {
  if (!isConnected) {
    await producer.connect();
    isConnected = true;
  }
}

async function publishPaymentEvent(event) {
  await ensureProducerConnected();

  await producer.send({
    topic: "payment-events",
    messages: [
      {
        key: String(event.transactionId),
        value: JSON.stringify(event),
      },
    ],
  });
}

module.exports = {
  publishPaymentEvent,
  ensureProducerConnected,
};
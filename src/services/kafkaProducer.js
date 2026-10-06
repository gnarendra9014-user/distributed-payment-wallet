const { producer } = require("../config/kafka");

async function publishPaymentEvent(event) {
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
};
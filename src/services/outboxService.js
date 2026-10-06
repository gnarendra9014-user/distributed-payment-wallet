async function createOutboxEvent(client, event) {
  await client.query(
    `INSERT INTO outbox_events
     (
       event_type,
       aggregate_id,
       payload
     )
     VALUES ($1, $2, $3)`,
    [
      event.eventType,
      event.aggregateId,
      JSON.stringify(event.payload),
    ]
  );
}

module.exports = {
  createOutboxEvent,
};
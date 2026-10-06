const pool = require("../config/db");
const { publishPaymentEvent } = require("./kafkaProducer");

async function processOutboxEvents() {
  const client = await pool.connect();

try {
    await pool.query(
      `UPDATE outbox_events
       SET status = 'PENDING',
           processing_at = NULL
       WHERE status = 'PROCESSING'
       AND processing_at < CURRENT_TIMESTAMP - INTERVAL '30 seconds'`
    );
  } catch (error) {
    console.error(
      "Failed to recover stale outbox events:",
      error
    );
  }

  let events = [];

  try {
    await client.query("BEGIN");

    const result = await client.query(
      `SELECT *
       FROM outbox_events
       WHERE status = 'PENDING'
       ORDER BY id
       LIMIT 10
       FOR UPDATE SKIP LOCKED`
    );

    events = result.rows;

    if (events.length === 0) {
      await client.query("COMMIT");
      return;
    }

    const eventIds = events.map((event) => event.id);

    await client.query(
      `UPDATE outbox_events
       SET status = 'PROCESSING,
            proccessing_at=CURRENT_TIMESTAMP'
       WHERE id = ANY($1::bigint[])`,
      [eventIds]
    );

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");

    console.error("Failed to claim outbox events:", error);

    return;
  } finally {
    client.release();
  }

  for (const event of events) {
    try {
      await publishPaymentEvent({
        transactionId: event.aggregate_id,
        ...event.payload,
      });

      const updateClient = await pool.connect();

      try {
        await updateClient.query(
          `UPDATE outbox_events
           SET status = 'PROCESSED',
                proccessing_at=NULL,
               processed_at = CURRENT_TIMESTAMP
           WHERE id = $1`,
          [event.id]
        );
      } finally {
        updateClient.release();
      }

      console.log(
        `Outbox event ${event.id} published successfully`
      );
    } catch (publishError) {
      console.error(
        `Failed to publish outbox event ${event.id}:`,
        publishError
      );

      const retryClient = await pool.connect();

      try {
        await retryClient.query(
          `UPDATE outbox_events
           SET status = 'PENDING',
                proccessing_At=NULL
           WHERE id = $1`,
          [event.id]
        );
      } finally {
        retryClient.release();
      }
    }
  }
}
module.exports = {
  processOutboxEvents,
};
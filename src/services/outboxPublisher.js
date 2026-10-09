
const pool = require("../config/db");
const { publishPaymentEvent } = require("./kafkaProducer");

async function processOutboxEvents() {
  // Recover events stuck in PROCESSING for more than 30 seconds.
  console.log("Outbox publisher is running...");
  try {
    await pool.query(
      `UPDATE outbox_events
       SET status = 'PENDING',
           processing_at = NULL
       WHERE status = 'PROCESSING'
         AND processing_at < CURRENT_TIMESTAMP - INTERVAL '30 seconds'`
    );
  } catch (error) {
    console.error("Failed to recover stale outbox events:", error);
  }

  const client = await pool.connect();
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
    console.log("Pending outbox events found:", result.rows.length);

    events = result.rows;

    if (events.length === 0) {
      await client.query("COMMIT");
      return;
    }

    const eventIds = events.map((event) => event.id);

    await client.query(
      `UPDATE outbox_events
       SET status = 'PROCESSING',
           processing_at = CURRENT_TIMESTAMP
       WHERE id = ANY($1::bigint[])`,
      [eventIds]
    );

    await client.query("COMMIT");
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackError) {
      console.error("Rollback failed:", rollbackError);
    }

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

      await pool.query(
        `UPDATE outbox_events
         SET status = 'PROCESSED',
             processing_at = NULL,
             processed_at = CURRENT_TIMESTAMP
         WHERE id = $1`,
        [event.id]
      );

      console.log(
        `Outbox event ${event.id} published successfully`
      );
    } catch (publishError) {
      console.error(
        `Failed to publish outbox event ${event.id}:`,
        publishError
      );

      try {
        await pool.query(
          `UPDATE outbox_events
           SET status = 'PENDING',
               processing_at = NULL
           WHERE id = $1`,
          [event.id]
        );
      } catch (retryError) {
        console.error(
          `Failed to reset outbox event ${event.id}:`,
          retryError
        );
      }
    }
  }
}

module.exports = {
  processOutboxEvents,
};

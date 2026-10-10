const pool = require("../config/db");
const { publishPaymentEvent } = require("./kafkaProducer");

const MAX_RETRIES = 5;
let isProcessing = false;

async function processOutboxEvents() {
  if (isProcessing) {
    console.log("Outbox publisher already running, skipping tick...");
    return;
  }

  isProcessing = true;

  try {
    // 1. Mark stale PROCESSING events that exceeded max retries as FAILED
    await pool.query(
      `UPDATE outbox_events
       SET status = 'FAILED',
           processing_at = NULL,
           error_message = 'Max retries exceeded during processing recovery'
       WHERE status = 'PROCESSING'
         AND processing_at < CURRENT_TIMESTAMP - INTERVAL '30 seconds'
         AND retry_count >= $1`,
      [MAX_RETRIES]
    );

    // 2. Recover stale PROCESSING events that can still be retried
    await pool.query(
      `UPDATE outbox_events
       SET status = 'PENDING',
           processing_at = NULL
       WHERE status = 'PROCESSING'
         AND processing_at < CURRENT_TIMESTAMP - INTERVAL '30 seconds'
         AND retry_count < $1`,
      [MAX_RETRIES]
    );

    // 3. Atomically claim a batch of PENDING events using SKIP LOCKED
    const client = await pool.connect();
    let events = [];

    try {
      await client.query("BEGIN");

      const result = await client.query(
        `SELECT *
         FROM outbox_events
         WHERE status = 'PENDING'
           AND retry_count < $1
         ORDER BY id
         LIMIT 10
         FOR UPDATE SKIP LOCKED`,
        [MAX_RETRIES]
      );

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
    } catch (claimError) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error("Rollback failed:", rollbackError.message);
      }
      console.error("Failed to claim outbox events:", claimError.message);
      return;
    } finally {
      client.release();
    }

    // 4. Publish each claimed event to Kafka
    for (const event of events) {
      try {
        const payloadData = typeof event.payload === "string" 
          ? JSON.parse(event.payload) 
          : event.payload;

        await publishPaymentEvent({
          outboxEventId: event.id,
          transactionId: event.aggregate_id,
          ...payloadData,
        });

        await pool.query(
          `UPDATE outbox_events
           SET status = 'PROCESSED',
               processing_at = NULL,
               processed_at = CURRENT_TIMESTAMP
           WHERE id = $1`,
          [event.id]
        );

        console.log(`Outbox event ${event.id} published successfully`);
      } catch (publishError) {
        console.error(`Failed to publish outbox event ${event.id}:`, publishError.message);

        const newRetryCount = (event.retry_count || 0) + 1;
        const newStatus = newRetryCount >= MAX_RETRIES ? "FAILED" : "PENDING";
        const errorMessage = (publishError && publishError.message) || "Unknown publish error";

        try {
          await pool.query(
            `UPDATE outbox_events
             SET status = $1,
                 retry_count = $2,
                 processing_at = NULL,
                 error_message = $3
             WHERE id = $4`,
            [newStatus, newRetryCount, errorMessage, event.id]
          );
        } catch (updateError) {
          console.error(`Failed to update retry state for event ${event.id}:`, updateError.message);
        }
      }
    }
  } catch (error) {
    console.error("Unexpected error in outbox publisher:", error.message);
  } finally {
    isProcessing = false;
  }
}

module.exports = {
  processOutboxEvents,
  MAX_RETRIES,
};

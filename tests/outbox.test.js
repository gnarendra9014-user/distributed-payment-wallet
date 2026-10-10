const { describe, it, before, after } = require("node:test");
const assert = require("node:assert/strict");

require("dotenv").config();
const pool = require("../src/config/db");
const { processOutboxEvents, MAX_RETRIES } = require("../src/services/outboxPublisher");

describe("Transactional Outbox Publisher & Dead-Letter Queue Tests", () => {
  let createdEventIds = [];

  after(async () => {
    if (createdEventIds.length > 0) {
      await pool.query(
        `DELETE FROM outbox_events WHERE id = ANY($1::bigint[])`,
        [createdEventIds]
      ).catch(() => {});
    }
    await pool.end().catch(() => {});
  });

  it("marks stale PROCESSING events that exceed MAX_RETRIES as FAILED", async () => {
    // Insert a simulated poison event that failed 5 times and timed out
    const insertRes = await pool.query(
      `INSERT INTO outbox_events (event_type, aggregate_id, payload, status, retry_count, processing_at)
       VALUES ($1, $2, $3, 'PROCESSING', $4, CURRENT_TIMESTAMP - INTERVAL '60 seconds')
       RETURNING id`,
      ["TEST_POISON_EVENT", 99999, JSON.stringify({ test: true }), MAX_RETRIES]
    );

    const poisonId = insertRes.rows[0].id;
    createdEventIds.push(poisonId);

    // Run outbox publisher recovery cycle
    await processOutboxEvents();

    const check = await pool.query(
      `SELECT status, retry_count, error_message FROM outbox_events WHERE id = $1`,
      [poisonId]
    );

    assert.equal(check.rows.length, 1);
    assert.equal(check.rows[0].status, "FAILED", "Event exceeding max retries must be marked FAILED");
    assert.ok(check.rows[0].error_message.includes("Max retries exceeded"));
  });

  it("recovers stale PROCESSING events with retry_count < MAX_RETRIES back to PENDING", async () => {
    // Insert a transiently stalled event
    const insertRes = await pool.query(
      `INSERT INTO outbox_events (event_type, aggregate_id, payload, status, retry_count, processing_at)
       VALUES ($1, $2, $3, 'PROCESSING', 1, CURRENT_TIMESTAMP - INTERVAL '60 seconds')
       RETURNING id`,
      ["TEST_RECOVERABLE_EVENT", 99998, JSON.stringify({ test: true })]
    );

    const recoverableId = insertRes.rows[0].id;
    createdEventIds.push(recoverableId);

    // Run recovery
    await processOutboxEvents();

    const check = await pool.query(
      `SELECT status FROM outbox_events WHERE id = $1`,
      [recoverableId]
    );

    assert.equal(check.rows.length, 1);
    // It should have either been recovered to PENDING or published to PROCESSED
    assert.ok(
      check.rows[0].status === "PENDING" || check.rows[0].status === "PROCESSED",
      `Expected status PENDING or PROCESSED, got ${check.rows[0].status}`
    );
  });
});

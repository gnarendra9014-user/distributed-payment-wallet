const { describe, it, before, after } = require("node:test");
const assert = require("node:assert/strict");
const http = require("http");

require("dotenv").config();
const { app } = require("../src/server");
const pool = require("../src/config/db");
const redisClient = require("../src/config/redis");
const { handlePaymentMessage } = require("../src/services/kafkaConsumer");
const { processOutboxEvents, MAX_RETRIES } = require("../src/services/outboxPublisher");

describe("Payment and Wallet System Integration Tests", () => {
  let server;
  let baseUrl;
  let userA, tokenA;
  let userB, tokenB;

  before(async () => {
    // Ensure redis connection for tests
    if (!redisClient.isOpen) {
      await redisClient.connect().catch((err) => {
        console.warn("Redis already connected or connecting:", err.message);
      });
    }

    // Start server on an ephemeral port
    await new Promise((resolve) => {
      server = app.listen(0, () => {
        const port = server.address().port;
        baseUrl = `http://127.0.0.1:${port}`;
        resolve();
      });
    });
  });

  after(async () => {
    if (server) {
      await new Promise((resolve) => server.close(resolve));
    }
    // Clean up connections
    await pool.end().catch(() => {});
    if (redisClient.isOpen) {
      await redisClient.quit().catch(() => {});
    }
  });

  // ----------------------------------------------------
  // 1. Authentication & User Creation Tests
  // ----------------------------------------------------
  describe("Authentication & User Registration", () => {
    const timestamp = Date.now();
    const emailA = `alice_${timestamp}@test.com`;
    const emailB = `bob_${timestamp}@test.com`;

    it("registers user A and automatically initializes a wallet", async () => {
      const res = await fetch(`${baseUrl}/users`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "Alice",
          email: emailA,
          password: "password123",
        }),
      });

      assert.equal(res.status, 201);
      const data = await res.json();
      assert.equal(data.email, emailA);
      assert.ok(data.id);
      assert.ok(data.wallet);
      assert.equal(Number(data.wallet.balance), 0);
      userA = data;
    });

    it("registers user B and automatically initializes a wallet", async () => {
      const res = await fetch(`${baseUrl}/users`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "Bob",
          email: emailB,
          password: "password456",
        }),
      });

      assert.equal(res.status, 201);
      const data = await res.json();
      assert.equal(data.email, emailB);
      assert.ok(data.id);
      assert.ok(data.wallet);
      userB = data;
    });

    it("prevents duplicate registration with HTTP 409", async () => {
      const res = await fetch(`${baseUrl}/users`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "Alice Duplicate",
          email: emailA,
          password: "password123",
        }),
      });

      assert.equal(res.status, 409);
      const data = await res.json();
      assert.equal(data.error, "Email already registered");
    });

    it("rejects invalid input (short password, bad email)", async () => {
      const shortPassRes = await fetch(`${baseUrl}/users`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "Charlie",
          email: "charlie@test.com",
          password: "123",
        }),
      });
      assert.equal(shortPassRes.status, 400);

      const badEmailRes = await fetch(`${baseUrl}/users`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: "Charlie",
          email: "invalid-email-address",
          password: "password123",
        }),
      });
      assert.equal(badEmailRes.status, 400);
    });

    it("authenticates users and issues JWT token", async () => {
      const resA = await fetch(`${baseUrl}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: emailA, password: "password123" }),
      });
      assert.equal(resA.status, 200);
      const dataA = await resA.json();
      assert.ok(dataA.token);
      tokenA = dataA.token;

      const resB = await fetch(`${baseUrl}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: emailB, password: "password456" }),
      });
      assert.equal(resB.status, 200);
      const dataB = await resB.json();
      assert.ok(dataB.token);
      tokenB = dataB.token;
    });

    it("rejects login with wrong password", async () => {
      const res = await fetch(`${baseUrl}/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: emailA, password: "wrong-password" }),
      });
      assert.equal(res.status, 401);
    });
  });

  // ----------------------------------------------------
  // 2. Authorization Security Tests
  // ----------------------------------------------------
  describe("Authorization & Route Protection", () => {
    it("rejects unauthenticated requests to wallet endpoints with 401", async () => {
      const walletRes = await fetch(`${baseUrl}/users/${userA.id}/wallet`);
      assert.equal(walletRes.status, 401);

      const topupRes = await fetch(`${baseUrl}/wallets/${userA.id}/topup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ amount: 100 }),
      });
      assert.equal(topupRes.status, 401);

      const transferRes = await fetch(`${baseUrl}/wallets/transfer`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Idempotency-Key": "unauth-key",
        },
        body: JSON.stringify({ receiver_user_id: userB.id, amount: 50 }),
      });
      assert.equal(transferRes.status, 401);
    });

    it("forbids user A from accessing or mutating user B's wallet (403)", async () => {
      // User A trying to view User B's wallet
      const viewRes = await fetch(`${baseUrl}/users/${userB.id}/wallet`, {
        headers: { Authorization: `Bearer ${tokenA}` },
      });
      assert.equal(viewRes.status, 403);

      // User A trying to topup User B's wallet
      const topupRes = await fetch(`${baseUrl}/wallets/${userB.id}/topup`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
        },
        body: JSON.stringify({ amount: 100 }),
      });
      assert.equal(topupRes.status, 403);

      // User A trying to withdraw from User B's wallet
      const withdrawRes = await fetch(`${baseUrl}/wallets/${userB.id}/withdraw`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
        },
        body: JSON.stringify({ amount: 50 }),
      });
      assert.equal(withdrawRes.status, 403);
    });
  });

  // ----------------------------------------------------
  // 3. Wallet Balance Consistency & Transaction Rollback
  // ----------------------------------------------------
  describe("Wallet Topup, Withdrawal & Balance Consistency", () => {
    it("rejects non-positive amounts without leaking uncommitted transactions", async () => {
      const negativeRes = await fetch(`${baseUrl}/wallets/${userA.id}/topup`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
        },
        body: JSON.stringify({ amount: -50 }),
      });
      assert.equal(negativeRes.status, 400);

      const zeroRes = await fetch(`${baseUrl}/wallets/${userA.id}/topup`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
        },
        body: JSON.stringify({ amount: "invalid-number" }),
      });
      assert.equal(zeroRes.status, 400);
    });

    it("successfully tops up wallet and updates balance", async () => {
      const res = await fetch(`${baseUrl}/wallets/${userA.id}/topup`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
        },
        body: JSON.stringify({ amount: 100 }),
      });

      assert.equal(res.status, 201);
      const data = await res.json();
      assert.equal(Number(data.wallet.balance), 100);
    });

    it("withdraws funds with row-level balance verification", async () => {
      const res = await fetch(`${baseUrl}/wallets/${userA.id}/withdraw`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
        },
        body: JSON.stringify({ amount: 30 }),
      });

      assert.equal(res.status, 201);
      const data = await res.json();
      assert.equal(Number(data.wallet.balance), 70);
    });

    it("rejects withdrawal if balance is insufficient (prevents overdraft)", async () => {
      const res = await fetch(`${baseUrl}/wallets/${userA.id}/withdraw`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
        },
        body: JSON.stringify({ amount: 500 }),
      });

      assert.equal(res.status, 400);
      const data = await res.json();
      assert.equal(data.error, "Insufficient wallet balance");

      // Verify balance was unchanged
      const walletRes = await fetch(`${baseUrl}/users/${userA.id}/wallet`, {
        headers: { Authorization: `Bearer ${tokenA}` },
      });
      const wallet = await walletRes.json();
      assert.equal(Number(wallet.balance), 70);
    });
  });

  // ----------------------------------------------------
  // 4. Idempotency Key Handling & Reuse Protection
  // ----------------------------------------------------
  describe("Idempotency Key Reuse & Concurrent Requests", () => {
    const idempotencyKey = `idem-${Date.now()}-${Math.random()}`;
    let transferTxId;

    it("successfully executes payment transfer on first request", async () => {
      const res = await fetch(`${baseUrl}/wallets/transfer`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
          "Idempotency-Key": idempotencyKey,
        },
        body: JSON.stringify({
          receiver_user_id: userB.id,
          amount: 40,
        }),
      });

      assert.equal(res.status, 201);
      const data = await res.json();
      assert.equal(data.message, "Payment transferred successfully");
      assert.equal(Number(data.sender_wallet.balance), 30); // 70 - 40 = 30
      assert.equal(Number(data.receiver_wallet.balance), 40); // 0 + 40 = 40
      assert.ok(data.transaction.id);
      transferTxId = data.transaction.id;
    });

    it("returns cached result (200 OK) when reusing idempotency key with IDENTICAL details", async () => {
      const res = await fetch(`${baseUrl}/wallets/transfer`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
          "Idempotency-Key": idempotencyKey,
        },
        body: JSON.stringify({
          receiver_user_id: userB.id,
          amount: 40,
        }),
      });

      assert.equal(res.status, 200);
      const data = await res.json();
      assert.equal(data.message, "Payment already processed");
      assert.equal(data.transaction.id, transferTxId);

      // Verify balances were NOT debited again
      const walletResA = await fetch(`${baseUrl}/users/${userA.id}/wallet`, {
        headers: { Authorization: `Bearer ${tokenA}` },
      });
      const walletA = await walletResA.json();
      assert.equal(Number(walletA.balance), 30);
    });

    it("REJECTS reusing idempotency key with DIFFERENT amount (HTTP 422)", async () => {
      const res = await fetch(`${baseUrl}/wallets/transfer`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
          "Idempotency-Key": idempotencyKey,
        },
        body: JSON.stringify({
          receiver_user_id: userB.id,
          amount: 999, // DIFFERENT AMOUNT
        }),
      });

      assert.equal(res.status, 422);
      const data = await res.json();
      assert.equal(
        data.error,
        "Idempotency key has already been used with different payment details"
      );

      // Verify balance remains unchanged
      const walletResA = await fetch(`${baseUrl}/users/${userA.id}/wallet`, {
        headers: { Authorization: `Bearer ${tokenA}` },
      });
      const walletA = await walletResA.json();
      assert.equal(Number(walletA.balance), 30);
    });

    it("REJECTS reusing idempotency key with DIFFERENT receiver (HTTP 422)", async () => {
      const res = await fetch(`${baseUrl}/wallets/transfer`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
          "Idempotency-Key": idempotencyKey,
        },
        body: JSON.stringify({
          receiver_user_id: 999999, // DIFFERENT RECEIVER
          amount: 40,
        }),
      });

      assert.equal(res.status, 422);
      const data = await res.json();
      assert.equal(
        data.error,
        "Idempotency key has already been used with different payment details"
      );
    });

    it("prevents self-transfer (sender === receiver) with 400", async () => {
      const res = await fetch(`${baseUrl}/wallets/transfer`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
          "Idempotency-Key": `self-${Date.now()}`,
        },
        body: JSON.stringify({
          receiver_user_id: userA.id,
          amount: 10,
        }),
      });

      assert.equal(res.status, 400);
      const data = await res.json();
      assert.equal(data.error, "Sender and receiver cannot be the same");
    });
  });

  // ----------------------------------------------------
  // 5. Refunds & Outbox Event Emission
  // ----------------------------------------------------
  describe("Refunds & Transaction Outbox", () => {
    let transferTx;

    before(async () => {
      // Execute a fresh transfer of $10 to refund
      const idemKey = `refund-target-${Date.now()}`;
      const res = await fetch(`${baseUrl}/wallets/transfer`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${tokenA}`,
          "Idempotency-Key": idemKey,
        },
        body: JSON.stringify({
          receiver_user_id: userB.id,
          amount: 10,
        }),
      });
      assert.equal(res.status, 201);
      const data = await res.json();
      transferTx = data.transaction;
    });

    it("processes refund successfully and emits outbox event", async () => {
      const res = await fetch(`${baseUrl}/transactions/${transferTx.id}/refund`, {
        method: "POST",
        headers: { Authorization: `Bearer ${tokenA}` },
      });

      assert.equal(res.status, 201);
      const data = await res.json();
      assert.equal(data.message, "Refund processed successfully");
      assert.equal(data.refund_transaction.type, "REFUND");
      assert.equal(data.refund_transaction.status, "SUCCESS");

      // Verify outbox table recorded the REFUND_COMPLETED event
      const outboxCheck = await pool.query(
        `SELECT * FROM outbox_events WHERE aggregate_id = $1 AND event_type = 'REFUND_COMPLETED'`,
        [data.refund_transaction.id]
      );
      assert.ok(outboxCheck.rows.length >= 1, "Must create an outbox event for refund");
    });

    it("prevents double-refund on same transaction with 400", async () => {
      const res = await fetch(`${baseUrl}/transactions/${transferTx.id}/refund`, {
        method: "POST",
        headers: { Authorization: `Bearer ${tokenA}` },
      });

      assert.equal(res.status, 400);
      const data = await res.json();
      assert.equal(data.error, "Transaction has already been refunded");
    });

    it("prevents non-sender from requesting refund with 403", async () => {
      const res = await fetch(`${baseUrl}/transactions/${transferTx.id}/refund`, {
        method: "POST",
        headers: { Authorization: `Bearer ${tokenB}` },
      });

      assert.equal(res.status, 403);
      const data = await res.json();
      assert.equal(data.error, "Only the sender can request a refund");
    });
  });

  // ----------------------------------------------------
  // 6. Kafka Consumer Deduplication Test
  // ----------------------------------------------------
  describe("Kafka Consumer Deduplication", () => {
    it("deduplicates identical Kafka payment events", async () => {
      const testEventId = 999000000 + Math.floor(Math.random() * 100000);
      const kafkaMessage = {
        topic: "payment-events",
        partition: 0,
        message: {
          value: Buffer.from(
            JSON.stringify({
              outboxEventId: testEventId,
              transactionId: 1234,
              event: "PAYMENT_COMPLETED",
            })
          ),
        },
      };

      // First delivery: should insert
      await handlePaymentMessage(kafkaMessage);

      const check1 = await pool.query(
        `SELECT * FROM processed_events WHERE event_id = $1`,
        [testEventId]
      );
      assert.equal(check1.rows.length, 1);

      // Second duplicate delivery: should be safely ignored
      await handlePaymentMessage(kafkaMessage);

      const check2 = await pool.query(
        `SELECT * FROM processed_events WHERE event_id = $1`,
        [testEventId]
      );
      assert.equal(check2.rows.length, 1, "Duplicate delivery must not insert duplicate row");
    });
  });
});

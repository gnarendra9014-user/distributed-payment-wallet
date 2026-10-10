require("dotenv").config();
const express = require("express");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");

const pool = require("./config/db");
const redisClient = require("./config/redis");
const { producer } = require("./config/kafka");
const authenticateToken = require("./middleware/auth");

const { publishPaymentEvent } = require("./services/kafkaProducer");
const { startKafkaConsumer } = require("./services/kafkaConsumer");
const { createOutboxEvent } = require("./services/outboxService");
const { processOutboxEvents } = require("./services/outboxPublisher");
const {
  getIdempotencyResult,
  saveIdempotencyResult,
  acquireIdempotencyLock,
  releaseIdempotencyLock,
  createPayloadFingerprint,
  verifyPayloadFingerprint,
} = require("./services/idempotencyService");

const PORT = process.env.PORT || 8000;
const app = express();

app.use(express.json());

// ----------------------------------------------------
// System & Health Endpoints
// ----------------------------------------------------

app.get("/", (req, res) => {
  res.send("Payment & Wallet System is running!");
});

app.get("/health", async (req, res) => {
  try {
    const result = await pool.query("SELECT NOW()");
    res.json({
      status: "OK",
      database: "Connected",
      time: result.rows[0].now,
    });
  } catch (error) {
    console.error("Health check database failure:", error.message);
    res.status(500).json({
      status: "ERROR",
      database: "Disconnected",
    });
  }
});

// ----------------------------------------------------
// Authentication & User Management
// ----------------------------------------------------

app.post("/users", async (req, res) => {
  const client = await pool.connect();
  try {
    const { name, email, password } = req.body;

    if (!name || !email || !password) {
      return res.status(400).json({
        error: "Name, email and password are required",
      });
    }

    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({
        error: "Invalid email format",
      });
    }

    if (typeof password !== "string" || password.length < 6) {
      return res.status(400).json({
        error: "Password must be at least 6 characters",
      });
    }

    const passwordHash = await bcrypt.hash(password, 10);

    await client.query("BEGIN");

    const userResult = await client.query(
      `INSERT INTO users (name, email, password_hash)
       VALUES ($1, $2, $3)
       RETURNING id, name, email, created_at`,
      [name.trim(), email.toLowerCase().trim(), passwordHash]
    );

    const newUser = userResult.rows[0];

    // Auto-create initial wallet with 0.00 balance in the same transaction
    const walletResult = await client.query(
      `INSERT INTO wallets (user_id, balance)
       VALUES ($1, 0.00)
       RETURNING *`,
      [newUser.id]
    );

    await client.query("COMMIT");

    res.status(201).json({
      ...newUser,
      wallet: walletResult.rows[0],
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});

    if (error.code === "23505") {
      return res.status(409).json({
        error: "Email already registered",
      });
    }

    console.error("Failed to create user:", error.message);
    res.status(500).json({
      error: "Failed to create user",
    });
  } finally {
    client.release();
  }
});

app.post("/login", async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({
        error: "Email and password are required",
      });
    }

    const result = await pool.query(
      `SELECT * FROM users WHERE email = $1`,
      [email.toLowerCase().trim()]
    );

    if (result.rows.length === 0) {
      return res.status(401).json({
        error: "Invalid email or password",
      });
    }

    const user = result.rows[0];
    const passwordMatch = await bcrypt.compare(password, user.password_hash);

    if (!passwordMatch) {
      return res.status(401).json({
        error: "Invalid email or password",
      });
    }

    const token = jwt.sign(
      {
        userId: user.id,
        email: user.email,
      },
      process.env.JWT_SECRET,
      {
        expiresIn: "1h",
      }
    );

    res.json({
      message: "Login successful",
      token,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
      },
    });
  } catch (error) {
    console.error("Login failed:", error.message);
    res.status(500).json({
      error: "Login failed",
    });
  }
});

// ----------------------------------------------------
// Wallet Management
// ----------------------------------------------------

app.get("/users/:userId/wallet", authenticateToken, async (req, res) => {
  try {
    const { userId } = req.params;

    if (Number(userId) !== Number(req.user.userId)) {
      return res.status(403).json({
        error: "You can only view your own wallet",
      });
    }

    const result = await pool.query(
      `SELECT * FROM wallets WHERE user_id = $1`,
      [userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        error: "Wallet not found",
      });
    }

    res.json(result.rows[0]);
  } catch (error) {
    console.error("Failed to get wallet:", error.message);
    res.status(500).json({
      error: "Failed to get wallet",
    });
  }
});

app.post("/users/:userId/wallet", authenticateToken, async (req, res) => {
  try {
    const { userId } = req.params;

    if (Number(userId) !== Number(req.user.userId)) {
      return res.status(403).json({
        error: "You can only create a wallet for your own account",
      });
    }

    const result = await pool.query(
      `INSERT INTO wallets (user_id, balance)
       VALUES ($1, 0.00)
       ON CONFLICT (user_id) DO NOTHING
       RETURNING *`,
      [userId]
    );

    if (result.rows.length === 0) {
      const existing = await pool.query(
        `SELECT * FROM wallets WHERE user_id = $1`,
        [userId]
      );
      return res.status(200).json({
        message: "Wallet already exists",
        wallet: existing.rows[0],
      });
    }

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Failed to create wallet:", error.message);
    res.status(500).json({
      error: "Failed to create wallet",
    });
  }
});

app.post("/wallets/:userId/topup", authenticateToken, async (req, res) => {
  const { userId } = req.params;
  const { amount } = req.body;

  if (Number(userId) !== Number(req.user.userId)) {
    return res.status(403).json({
      error: "You can only top up your own wallet",
    });
  }

  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
    return res.status(400).json({
      error: "Amount must be a valid number greater than zero",
    });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const walletResult = await client.query(
      `UPDATE wallets
       SET balance = balance + $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, userId]
    );

    if (walletResult.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        error: "Wallet not found",
      });
    }

    const transactionResult = await client.query(
      `INSERT INTO transactions (user_id, sender_user_id, receiver_user_id, type, amount, status)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [userId, userId, userId, "TOPUP", numericAmount, "SUCCESS"]
    );

    await createOutboxEvent(client, {
      eventType: "WALLET_TOPUP",
      aggregateId: transactionResult.rows[0].id,
      payload: {
        event: "WALLET_TOPUP",
        transactionId: transactionResult.rows[0].id,
        userId: Number(userId),
        amount: numericAmount,
      },
    });

    await client.query("COMMIT");

    res.status(201).json({
      message: "Wallet topped up successfully",
      wallet: walletResult.rows[0],
      transaction: transactionResult.rows[0],
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    console.error("Failed to top up wallet:", error.message);
    res.status(500).json({
      error: "Failed to top up wallet",
    });
  } finally {
    client.release();
  }
});

app.post("/wallets/:userId/withdraw", authenticateToken, async (req, res) => {
  const { userId } = req.params;
  const { amount } = req.body;

  if (Number(userId) !== Number(req.user.userId)) {
    return res.status(403).json({
      error: "You can only withdraw from your own wallet",
    });
  }

  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
    return res.status(400).json({
      error: "Amount must be a valid number greater than zero",
    });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const walletResult = await client.query(
      `SELECT * FROM wallets WHERE user_id = $1 FOR UPDATE`,
      [userId]
    );

    if (walletResult.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        error: "Wallet not found",
      });
    }

    const wallet = walletResult.rows[0];
    if (Number(wallet.balance) < numericAmount) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "Insufficient wallet balance",
      });
    }

    const walletUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance - $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, userId]
    );

    const transactionResult = await client.query(
      `INSERT INTO transactions (user_id, sender_user_id, receiver_user_id, type, amount, status)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [userId, userId, userId, "WITHDRAWAL", numericAmount, "SUCCESS"]
    );

    await createOutboxEvent(client, {
      eventType: "WALLET_WITHDRAWAL",
      aggregateId: transactionResult.rows[0].id,
      payload: {
        event: "WALLET_WITHDRAWAL",
        transactionId: transactionResult.rows[0].id,
        userId: Number(userId),
        amount: numericAmount,
      },
    });

    await client.query("COMMIT");

    res.status(201).json({
      message: "Withdrawal successful",
      wallet: walletUpdate.rows[0],
      transaction: transactionResult.rows[0],
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    console.error("Failed to withdraw money:", error.message);
    res.status(500).json({
      error: "Failed to withdraw money",
    });
  } finally {
    client.release();
  }
});

// ----------------------------------------------------
// Wallet Transfers (Idempotent, Distributed, Outbox-backed)
// ----------------------------------------------------

app.post("/wallets/transfer", authenticateToken, async (req, res) => {
  const idempotencyKey = req.headers["idempotency-key"];
  if (!idempotencyKey || typeof idempotencyKey !== "string" || !idempotencyKey.trim()) {
    return res.status(400).json({
      error: "Idempotency-Key header is required",
    });
  }

  const { receiver_user_id, amount } = req.body;
  const sender_user_id = req.user.userId;

  if (!receiver_user_id || amount === undefined) {
    return res.status(400).json({
      error: "receiver_user_id and amount are required",
    });
  }

  if (Number(sender_user_id) === Number(receiver_user_id)) {
    return res.status(400).json({
      error: "Sender and receiver cannot be the same",
    });
  }

  const numericAmount = Number(amount);
  if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
    return res.status(400).json({
      error: "Amount must be a valid number greater than zero",
    });
  }

  // Create deterministic fingerprint of payment details
  const payloadFingerprint = createPayloadFingerprint({
    sender_user_id: Number(sender_user_id),
    receiver_user_id: Number(receiver_user_id),
    amount: numericAmount,
  });

  // 1. Check Redis for completed payment with this idempotency key
  const cachedResult = await getIdempotencyResult(idempotencyKey);
  if (cachedResult) {
    // Detect idempotency key reuse with different payment details
    const fingerprintMatches = verifyPayloadFingerprint(cachedResult, payloadFingerprint);
    const detailsMatch = cachedResult.transaction &&
      Number(cachedResult.transaction.sender_user_id) === Number(sender_user_id) &&
      Number(cachedResult.transaction.receiver_user_id) === Number(receiver_user_id) &&
      Number(cachedResult.transaction.amount) === numericAmount;

    if (!fingerprintMatches || (cachedResult.transaction && !detailsMatch)) {
      return res.status(422).json({
        error: "Idempotency key has already been used with different payment details",
      });
    }

    return res.status(200).json({
      message: "Payment already processed",
      transaction: cachedResult.transaction,
    });
  }

  // 2. Acquire Redis distributed lock
  let lockAcquired = false;
  try {
    lockAcquired = await acquireIdempotencyLock(idempotencyKey);
    if (!lockAcquired) {
      return res.status(409).json({
        error: "Payment with this idempotency key is already being processed",
      });
    }
  } catch (lockError) {
    console.error("Lock acquisition error:", lockError.message);
  }

  // 3. Check PostgreSQL for existing transaction
  try {
    const existingTransaction = await pool.query(
      `SELECT * FROM transactions WHERE idempotency_key = $1`,
      [idempotencyKey]
    );

    if (existingTransaction.rows.length > 0) {
      const existing = existingTransaction.rows[0];

      // Verify payment parameters match original transaction
      if (
        Number(existing.sender_user_id) !== Number(sender_user_id) ||
        Number(existing.receiver_user_id) !== Number(receiver_user_id) ||
        Number(existing.amount) !== numericAmount
      ) {
        return res.status(422).json({
          error: "Idempotency key has already been used with different payment details",
        });
      }

      const existingResult = {
        message: "Payment already processed",
        transaction: existing,
      };

      await saveIdempotencyResult(idempotencyKey, existingResult, payloadFingerprint);
      return res.status(200).json(existingResult);
    }
  } catch (pgCheckError) {
    console.error("Postgres idempotency check error:", pgCheckError.message);
  }

  // 4. Execute atomic transfer transaction in PostgreSQL
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Lock both wallets in deterministic order to prevent deadlocks
    const walletResult = await client.query(
      `SELECT *
       FROM wallets
       WHERE user_id IN ($1, $2)
       ORDER BY user_id
       FOR UPDATE`,
      [sender_user_id, receiver_user_id]
    );

    if (walletResult.rows.length !== 2) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        error: "Sender or receiver wallet not found",
      });
    }

    const senderWallet = walletResult.rows.find(
      (wallet) => Number(wallet.user_id) === Number(sender_user_id)
    );
    const receiverWallet = walletResult.rows.find(
      (wallet) => Number(wallet.user_id) === Number(receiver_user_id)
    );

    // Verify sender balance
    if (Number(senderWallet.balance) < numericAmount) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "Insufficient wallet balance",
      });
    }

    // Deduct from sender
    const senderUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance - $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, sender_user_id]
    );

    // Add to receiver
    const receiverUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance + $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, receiver_user_id]
    );

    // Record transfer transaction
    const transactionResult = await client.query(
      `INSERT INTO transactions
       (
         user_id,
         sender_user_id,
         receiver_user_id,
         type,
         amount,
         status,
         idempotency_key
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7)
       RETURNING *`,
      [
        sender_user_id,
        sender_user_id,
        receiver_user_id,
        "TRANSFER",
        numericAmount,
        "SUCCESS",
        idempotencyKey,
      ]
    );

    // Persist event in transactional outbox table
    await createOutboxEvent(client, {
      eventType: "PAYMENT_COMPLETED",
      aggregateId: transactionResult.rows[0].id,
      payload: {
        event: "PAYMENT_COMPLETED",
        transactionId: transactionResult.rows[0].id,
        senderUserId: Number(sender_user_id),
        receiverUserId: Number(receiver_user_id),
        amount: numericAmount,
      },
    });

    await client.query("COMMIT");

    const responseData = {
      message: "Payment transferred successfully",
      sender_wallet: senderUpdate.rows[0],
      receiver_wallet: receiverUpdate.rows[0],
      transaction: transactionResult.rows[0],
    };

    // Cache successful result with fingerprint in Redis
    await saveIdempotencyResult(idempotencyKey, responseData, payloadFingerprint);

    return res.status(201).json(responseData);
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});

    // Handle concurrent duplicate idempotency key race condition
    if (error.code === "23505") {
      try {
        const existingTx = await pool.query(
          `SELECT * FROM transactions WHERE idempotency_key = $1`,
          [idempotencyKey]
        );

        if (existingTx.rows.length > 0) {
          const existing = existingTx.rows[0];

          if (
            Number(existing.sender_user_id) !== Number(sender_user_id) ||
            Number(existing.receiver_user_id) !== Number(receiver_user_id) ||
            Number(existing.amount) !== numericAmount
          ) {
            return res.status(422).json({
              error: "Idempotency key has already been used with different payment details",
            });
          }

          const existingResult = {
            message: "Payment already processed",
            transaction: existing,
          };

          await saveIdempotencyResult(idempotencyKey, existingResult, payloadFingerprint);
          return res.status(200).json(existingResult);
        }
      } catch (dupError) {
        console.error("Duplicate transaction lookup error:", dupError.message);
      }

      return res.status(409).json({
        error: "Payment with this idempotency key is already being processed",
      });
    }

    console.error("Failed to transfer payment:", error.message);
    return res.status(500).json({
      error: "Failed to transfer payment",
    });
  } finally {
    if (lockAcquired) {
      await releaseIdempotencyLock(idempotencyKey);
    }
    client.release();
  }
});

// ----------------------------------------------------
// Transaction History & Refunds
// ----------------------------------------------------

app.get("/users/:userId/transactions", authenticateToken, async (req, res) => {
  try {
    const { userId } = req.params;

    if (Number(userId) !== Number(req.user.userId)) {
      return res.status(403).json({
        error: "You can only view your own transactions",
      });
    }

    const result = await pool.query(
      `SELECT *
       FROM transactions
       WHERE user_id = $1
          OR sender_user_id = $1
          OR receiver_user_id = $1
       ORDER BY created_at DESC`,
      [userId]
    );

    res.json({
      transactions: result.rows,
    });
  } catch (error) {
    console.error("Failed to fetch transactions:", error.message);
    res.status(500).json({
      error: "Failed to fetch transactions",
    });
  }
});

app.post("/transactions/:transactionId/refund", authenticateToken, async (req, res) => {
  const { transactionId } = req.params;
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Lock original transaction
    const transactionResult = await client.query(
      `SELECT * FROM transactions WHERE id = $1 FOR UPDATE`,
      [transactionId]
    );

    if (transactionResult.rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        error: "Transaction not found",
      });
    }

    const originalTransaction = transactionResult.rows[0];

    // Only successful transfers can be refunded
    if (
      originalTransaction.type !== "TRANSFER" ||
      originalTransaction.status !== "SUCCESS"
    ) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "Only successful transfers can be refunded",
      });
    }

    // Only original sender can request refund
    if (Number(originalTransaction.sender_user_id) !== Number(req.user.userId)) {
      await client.query("ROLLBACK");
      return res.status(403).json({
        error: "Only the sender can request a refund",
      });
    }

    // Prevent double refunds
    if (originalTransaction.refund_transaction_id) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "Transaction has already been refunded",
      });
    }

    const senderUserId = originalTransaction.sender_user_id;
    const receiverUserId = originalTransaction.receiver_user_id;
    const amount = Number(originalTransaction.amount);

    // Lock both wallets in deterministic order
    const walletResult = await client.query(
      `SELECT *
       FROM wallets
       WHERE user_id IN ($1, $2)
       ORDER BY user_id
       FOR UPDATE`,
      [senderUserId, receiverUserId]
    );

    if (walletResult.rows.length !== 2) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        error: "Sender or receiver wallet not found",
      });
    }

    const receiverWallet = walletResult.rows.find(
      (wallet) => Number(wallet.user_id) === Number(receiverUserId)
    );
    const senderWallet = walletResult.rows.find(
      (wallet) => Number(wallet.user_id) === Number(senderUserId)
    );

    // Receiver must have sufficient balance to return
    if (Number(receiverWallet.balance) < amount) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        error: "Receiver has insufficient balance for refund",
      });
    }

    // Take money back from receiver
    const receiverUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance - $1
       WHERE user_id = $2
       RETURNING *`,
      [amount, receiverUserId]
    );

    // Return money to sender
    const senderUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance + $1
       WHERE user_id = $2
       RETURNING *`,
      [amount, senderUserId]
    );

    // Record refund transaction
    const refundResult = await client.query(
      `INSERT INTO transactions
       (
         user_id,
         sender_user_id,
         receiver_user_id,
         type,
         amount,
         status
       )
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING *`,
      [
        receiverUserId,
        receiverUserId,
        senderUserId,
        "REFUND",
        amount,
        "SUCCESS",
      ]
    );

    // Link refund to original transaction
    await client.query(
      `UPDATE transactions
       SET refund_transaction_id = $1
       WHERE id = $2`,
      [refundResult.rows[0].id, transactionId]
    );

    // Emit outbox event for the refund
    await createOutboxEvent(client, {
      eventType: "REFUND_COMPLETED",
      aggregateId: refundResult.rows[0].id,
      payload: {
        event: "REFUND_COMPLETED",
        transactionId: refundResult.rows[0].id,
        originalTransactionId: originalTransaction.id,
        senderUserId: Number(senderUserId),
        receiverUserId: Number(receiverUserId),
        amount: amount,
      },
    });

    await client.query("COMMIT");

    res.status(201).json({
      message: "Refund processed successfully",
      original_transaction: originalTransaction,
      refund_transaction: refundResult.rows[0],
      sender_wallet: senderUpdate.rows[0],
      receiver_wallet: receiverUpdate.rows[0],
    });
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    console.error("Failed to process refund:", error.message);
    res.status(500).json({
      error: "Failed to process refund",
    });
  } finally {
    client.release();
  }
});

// Protected raw transaction endpoint for system/admin operations
app.post("/transactions", authenticateToken, async (req, res) => {
  try {
    const { user_id, type, amount } = req.body;

    if (!user_id || !type || amount === undefined) {
      return res.status(400).json({
        error: "user_id, type, and amount are required",
      });
    }

    const numericAmount = Number(amount);
    if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        error: "Amount must be a valid number greater than zero",
      });
    }

    const result = await pool.query(
      `INSERT INTO transactions (user_id, type, amount, status)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [user_id, type, numericAmount, "PENDING"]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error("Failed to create transaction:", error.message);
    res.status(500).json({
      error: "Failed to create transaction",
    });
  }
});

// ----------------------------------------------------
// Server Lifecycle
// ----------------------------------------------------

let outboxInterval = null;

async function startServer() {
  try {
    await redisClient.connect();
    console.log("Redis connected");

    await producer.connect();
    console.log("Kafka producer connected");

    await startKafkaConsumer();

    const server = app.listen(PORT, () => {
      console.log(`The server is running on PORT ${PORT}`);

      outboxInterval = setInterval(() => {
        processOutboxEvents();
      }, 5000);
    });

    return server;
  } catch (error) {
    console.error("Server startup failed:", error.message);
  }
}

if (require.main === module) {
  startServer();
}

module.exports = {
  app,
  startServer,
};

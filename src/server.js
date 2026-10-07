const express =require("express");
const redisClient = require("./config/redis");
const bcrypt = require("bcrypt");
const jwt=require("jsonwebtoken");
const authenticateToken = require("./middleware/auth");
require("dotenv").config();
const pool =require('./config/db');
console.log("Database:", process.env.DB_NAME);
const { producer } = require("./config/kafka");
const { publishPaymentEvent } = require("./services/kafkaProducer");
const { startKafkaConsumer } = require("./services/kafkaConsumer");
const { createOutboxEvent } = require("./services/outboxService");
const { processOutboxEvents } = require("./services/outboxPublisher");
const {
  getIdempotencyResult,
  saveIdempotencyResult,
  acquireIdempotencyLock,
  releaseIdempotencyLock,
} = require("./services/idempotencyService");


const app=express();

app.use(express.json());

app.get('/',(req,res) =>{
    res.send("Payment & Wallet System is running!");
})

app.get('/health', async (req,res) => {
    try{
        const result=await pool.query("SELECT NOW()");

        res.json({
            status :"OK",
            database : "Connected",
            time:result.rows[0].now,
        });
    }catch(error){
        console.error(error);

        res.status(500).json({
            status:"ERROR",
            database :"Disconnected",
        });
    }
})

app.get("/users/:userId/wallet", async (req, res) => {
  try {
    const { userId } = req.params;
    

    const result = await pool.query(
      "SELECT * FROM wallets WHERE user_id = $1",
      [userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        error: "Wallet not found",
      });
    }

    res.json(result.rows[0]);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to get wallet",
    });
  }
});

app.post("/transactions", async (req, res) => {
  try {
    console.log(req.body)
    const { user_id, type, amount } = req.body;

    const result = await pool.query(
      `INSERT INTO transactions (user_id, type, amount)
       VALUES ($1, $2, $3)
       RETURNING *`,
      [user_id, type, amount]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to create transaction",
    });
  }
});

app.post("/wallets/:userId/topup", async (req, res) => {
  const client = await pool.connect();

  try {
    const { userId } = req.params;
    const { amount } = req.body;

    await client.query("BEGIN");

    const numericAmount = Number(amount);

    if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        error: "Amount must be a valid number greater than zero",
      });
    }

    const walletResult = await client.query(
      `UPDATE wallets
       SET balance = balance + $1
       WHERE user_id = $2
       RETURNING *`,
      [amount, userId]
    );

    if (walletResult.rows.length === 0) {
      await client.query("ROLLBACK");

      return res.status(404).json({
        error: "Wallet not found",
      });
    }

    const transactionResult = await client.query(
      `INSERT INTO transactions
       (user_id, type, amount, status)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [userId, "TOPUP", amount, "SUCCESS"]
    );

    await client.query("COMMIT");

 

    res.status(201).json({
      message: "Wallet topped up successfully",
      wallet: walletResult.rows[0],
      transaction: transactionResult.rows[0],
    });

  } catch (error) {
    await client.query("ROLLBACK");

    console.error(error);

    res.status(500).json({
      error: "Failed to top up wallet",
    });
  } finally {
    client.release();
  }
});
app.post("/wallets/:userId/withdraw", authenticateToken, async (req, res) => {
  const client = await pool.connect();

  try {
    const { userId } = req.params;
    const { amount } = req.body;

    // User can only withdraw from their own wallet
    if (Number(userId) !== Number(req.user.userId)) {
      return res.status(403).json({
        error: "You can only withdraw from your own wallet",
      });
    }

    // Validate amount
    const numericAmount = Number(amount);

    if (!Number.isFinite(numericAmount) || numericAmount <= 0) {
      return res.status(400).json({
        error: "Amount must be a valid number greater than zero",
      });
    }

    await client.query("BEGIN");

    // Lock the wallet row while we perform the withdrawal
    const walletResult = await client.query(
      `SELECT *
       FROM wallets
       WHERE user_id = $1
       FOR UPDATE`,
      [userId]
    );

    if (walletResult.rows.length === 0) {
      await client.query("ROLLBACK");

      return res.status(404).json({
        error: "Wallet not found",
      });
    }

    const wallet = walletResult.rows[0];

    // Check balance
    if (Number(wallet.balance) < numericAmount) {
      await client.query("ROLLBACK");

      return res.status(400).json({
        error: "Insufficient wallet balance",
      });
    }

    // Deduct money
    const walletUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance - $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, userId]
    );

    // Record withdrawal
    const transactionResult = await client.query(
      `INSERT INTO transactions
       (user_id, type, amount, status)
       VALUES ($1, $2, $3, $4)
       RETURNING *`,
      [userId, "WITHDRAWAL", numericAmount, "SUCCESS"]
    );

    await client.query("COMMIT");

    res.status(201).json({
      message: "Withdrawal successful",
      wallet: walletUpdate.rows[0],
      transaction: transactionResult.rows[0],
    });

  } catch (error) {
    await client.query("ROLLBACK");

    console.error(error);

    res.status(500).json({
      error: "Failed to withdraw money",
    });
  } finally {
    client.release();
  }
});
app.post("/users", async (req, res) => {
  try {
    const { name, email, password } = req.body;

    if (!name || !email || !password) {
      return res.status(400).json({
        error: "Name, email and password are required",
      });
    }

    if (password.length < 6) {
      return res.status(400).json({
        error: "Password must be at least 6 characters",
      });
    }

    // Hash the password before storing it
    const passwordHash = await bcrypt.hash(password, 10);

    const result = await pool.query(
      `INSERT INTO users
       (name, email, password_hash)
       VALUES ($1, $2, $3)
       RETURNING id, name, email, created_at`,
      [name, email, passwordHash]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to create user",
    });
  }
});


app.post("/wallets/transfer", authenticateToken, async (req, res) => {
  const client = await pool.connect();

  let idempotencyKey;
  let lockAcquired = false;

  try {
    // Get idempotency key from request header
    idempotencyKey = req.headers["idempotency-key"];

    if (!idempotencyKey) {
      return res.status(400).json({
        error: "Idempotency-Key header is required",
      });
    }

    // --------------------------------------------------
    // 1. CHECK REDIS FOR ALREADY COMPLETED PAYMENT
    // --------------------------------------------------

    const cachedResult = await getIdempotencyResult(idempotencyKey);

    if (cachedResult) {
      return res.status(200).json({
        message: "Payment already processed",
        transaction: cachedResult.transaction,
      });
    }

    // --------------------------------------------------
    // 2. ACQUIRE REDIS LOCK
    // --------------------------------------------------

    lockAcquired = await acquireIdempotencyLock(idempotencyKey);

    if (!lockAcquired) {
      return res.status(409).json({
        error: "Payment with this idempotency key is already being processed",
      });
    }

    // --------------------------------------------------
    // 3. CHECK POSTGRESQL
    // --------------------------------------------------

    const existingTransaction = await client.query(
      `SELECT *
       FROM transactions
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );

    if (existingTransaction.rows.length > 0) {
      const existingResult = {
        message: "Payment already processed",
        transaction: existingTransaction.rows[0],
      };

      // Store existing result in Redis
      await saveIdempotencyResult(
        idempotencyKey,
        existingResult
      );

      return res.status(200).json(existingResult);
    }

    // --------------------------------------------------
    // 4. GET REQUEST DATA
    // --------------------------------------------------

    const { receiver_user_id, amount } = req.body;

    // Sender comes from JWT
    const sender_user_id = req.user.userId;

    console.log("JWT USER ID:", sender_user_id);
    console.log("RECEIVER USER ID:", receiver_user_id);

    if (!receiver_user_id || amount === undefined) {
      return res.status(400).json({
        error: "receiver_user_id and amount are required",
      });
    }

    // Sender and receiver cannot be the same
    if (
      Number(sender_user_id) === Number(receiver_user_id)
    ) {
      return res.status(400).json({
        error: "Sender and receiver cannot be the same",
      });
    }

    // Convert amount to number
    const numericAmount = Number(amount);

    if (
      !Number.isFinite(numericAmount) ||
      numericAmount <= 0
    ) {
      return res.status(400).json({
        error: "Amount must be a valid number greater than zero",
      });
    }

    // --------------------------------------------------
    // 5. START DATABASE TRANSACTION
    // --------------------------------------------------

    await client.query("BEGIN");

    // --------------------------------------------------
    // 6. LOCK BOTH WALLETS
    // --------------------------------------------------

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

    // Find sender wallet
    const senderWallet = walletResult.rows.find(
      (wallet) =>
        Number(wallet.user_id) === Number(sender_user_id)
    );

    // Find receiver wallet
    const receiverWallet = walletResult.rows.find(
      (wallet) =>
        Number(wallet.user_id) === Number(receiver_user_id)
    );

    // --------------------------------------------------
    // 7. CHECK SENDER BALANCE
    // --------------------------------------------------

    if (Number(senderWallet.balance) < numericAmount) {
      await client.query("ROLLBACK");

      return res.status(400).json({
        error: "Insufficient wallet balance",
      });
    }

    // --------------------------------------------------
    // 8. DEDUCT MONEY FROM SENDER
    // --------------------------------------------------

    const senderUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance - $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, sender_user_id]
    );

    // --------------------------------------------------
    // 9. ADD MONEY TO RECEIVER
    // --------------------------------------------------

    const receiverUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance + $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, receiver_user_id]
    );

    // --------------------------------------------------
    // 10. CREATE TRANSACTION RECORD
    // --------------------------------------------------

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
    await createOutboxEvent(client, {
  eventType: "PAYMENT_COMPLETED",
  aggregateId: transactionResult.rows[0].id,
  payload: {
    event: "PAYMENT_COMPLETED",
    transactionId: transactionResult.rows[0].id,
    senderUserId: sender_user_id,
    receiverUserId: receiver_user_id,
    amount: numericAmount,
  },
});

    // --------------------------------------------------
    // 11. COMMIT DATABASE TRANSACTION
    // --------------------------------------------------

   await client.query("COMMIT");


  const responseData = {
    message: "Payment transferred successfully",
    sender_wallet: senderUpdate.rows[0],
    receiver_wallet: receiverUpdate.rows[0],
    transaction: transactionResult.rows[0],
  };

    // --------------------------------------------------
    // 12. PREPARE RESPONSE
    // --------------------------------------------------

  

    // --------------------------------------------------
    // 13. SAVE SUCCESSFUL RESULT IN REDIS
    // --------------------------------------------------

    await saveIdempotencyResult(
      idempotencyKey,
      responseData
    );

    // --------------------------------------------------
    // 14. SEND RESPONSE
    // --------------------------------------------------

    res.status(201).json(responseData);

  } catch (error) {

    // --------------------------------------------------
    // ROLLBACK DATABASE TRANSACTION
    // --------------------------------------------------

    try {
      await client.query("ROLLBACK");
    } catch (rollbackError) {
      console.error("Rollback failed:", rollbackError);
    }

    // --------------------------------------------------
    // HANDLE DUPLICATE IDEMPOTENCY KEY
    // --------------------------------------------------

    if (error.code === "23505") {
      try {
        const existingTransaction = await client.query(
          `SELECT *
           FROM transactions
           WHERE idempotency_key = $1`,
          [idempotencyKey]
        );

        if (existingTransaction.rows.length > 0) {
          const existingResult = {
            message: "Payment already processed",
            transaction: existingTransaction.rows[0],
          };

          await saveIdempotencyResult(
            idempotencyKey,
            existingResult
          );

          return res.status(200).json(existingResult);
        }
      } catch (duplicateCheckError) {
        console.error(
          "Duplicate transaction lookup failed:",
          duplicateCheckError
        );
      }
    }

    console.error(error);

    return res.status(500).json({
      error: "Failed to transfer payment",
    });

  } finally {

    // --------------------------------------------------
    // RELEASE REDIS LOCK
    // --------------------------------------------------

    if (lockAcquired && idempotencyKey) {
      try {
        await releaseIdempotencyLock(idempotencyKey);
      } catch (redisError) {
        console.error(
          "Failed to release Redis lock:",
          redisError
        );
      }
    }

    // --------------------------------------------------
    // RELEASE DATABASE CONNECTION
    // --------------------------------------------------

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
      `SELECT * FROM users
       WHERE email = $1`,
      [email]
    );

    if (result.rows.length === 0) {
      return res.status(401).json({
        error: "Invalid email or password",
      });
    }

    const user = result.rows[0];

    const passwordMatch = await bcrypt.compare(
      password,
      user.password_hash
    );

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
    });
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Login failed",
    });
  }
});
app.post("/users/:userId/wallet", async (req, res) => {
  try {
    const { userId } = req.params;

    const result = await pool.query(
      `INSERT INTO wallets (user_id)
       VALUES ($1)
       RETURNING *`,
      [userId]
    );

    res.status(201).json(result.rows[0]);
  } catch (error) {
    console.error(error);

    res.status(500).json({
      error: "Failed to create wallet",
    });
  }
});
app.get("/users/:userId/transactions", authenticateToken, async (req, res) => {
  try {
    const { userId } = req.params;

    // A user can only see their own transactions
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
    console.error(error);

    res.status(500).json({
      error: "Failed to fetch transactions",
    });
  }
});

app.post("/transactions/:transactionId/refund",
  authenticateToken,
  async (req, res) => {
    const client = await pool.connect();

    try {
      const { transactionId } = req.params;

      await client.query("BEGIN");

      // Find and lock the original transaction
      const transactionResult = await client.query(
        `SELECT *
         FROM transactions
         WHERE id = $1
         FOR UPDATE`,
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

      // Only the original sender can request the refund
      if (
        Number(originalTransaction.sender_user_id) !==
        Number(req.user.userId)
      ) {
        await client.query("ROLLBACK");

        return res.status(403).json({
          error: "Only the sender can request a refund",
        });
      }

      // Prevent double refund
      if (originalTransaction.refund_transaction_id) {
        await client.query("ROLLBACK");

        return res.status(400).json({
          error: "Transaction has already been refunded",
        });
      }

      const senderUserId = originalTransaction.sender_user_id;
      const receiverUserId = originalTransaction.receiver_user_id;
      const amount = Number(originalTransaction.amount);

      // Lock both wallets
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
        (wallet) =>
          Number(wallet.user_id) === Number(receiverUserId)
      );

      const senderWallet = walletResult.rows.find(
        (wallet) =>
          Number(wallet.user_id) === Number(senderUserId)
      );

      // Receiver must have enough money to return it
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

      // Create refund transaction
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

      await client.query("COMMIT");

      res.status(201).json({
        message: "Refund processed successfully",
        original_transaction: originalTransaction,
        refund_transaction: refundResult.rows[0],
        sender_wallet: senderUpdate.rows[0],
        receiver_wallet: receiverUpdate.rows[0],
      });
    } catch (error) {
      await client.query("ROLLBACK");

      console.error(error);

      res.status(500).json({
        error: "Failed to process refund",
      });
    } finally {
      client.release();
    }
  }
);

async function startServer() {
  try {
    await redisClient.connect();

    console.log("Redis connected");

    await producer.connect();

    console.log("Kafka producer connected");

    await startKafkaConsumer();

    app.listen(PORT, () => {
      console.log(`The server is running on PORT ${PORT}`);

    setInterval(() => {
      processOutboxEvents();
    }, 5000);
  });
  } catch (error) {
    console.error("Server startup failed:", error);
  }
}


startServer();

const PORT = process.env.PORT || 8000;


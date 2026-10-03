const express =require("express");
const bcrypt = require("bcrypt");
const jwt=require("jsonwebtoken");
const authenticateToken = require("./middleware/auth");
require("dotenv").config();
const pool =require('./config/db');
console.log("Database:", process.env.DB_NAME);


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


app.post("/wallets/transfer",authenticateToken, async (req, res) => {
  const client = await pool.connect();

  try {
    const idempotencyKey = req.headers["idempotency-key"];

    if (!idempotencyKey) {
      return res.status(400).json({
        error: "Idempotency-Key header is required",
      });
    }

    // Check if this payment was already processed
    const existingTransaction = await client.query(
      `SELECT * FROM transactions
       WHERE idempotency_key = $1`,
      [idempotencyKey]
    );

    if (existingTransaction.rows.length > 0) {
      return res.status(200).json({
        message: "Payment already processed",
        transaction: existingTransaction.rows[0],
      });
    }

    const { receiver_user_id, amount } = req.body;

    const sender_user_id=req.user.userId;

    
    console.log("RECEIVER USER ID:", receiver_user_id);

    // Basic validation
    if (!receiver_user_id || !amount) {
      return res.status(400).json({
        error: "sender_user_id, receiver_user_id and amount are required",
      });
    }

    if (sender_user_id === receiver_user_id) {
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

    await client.query("BEGIN");

    // Lock both wallets while payment is being processed
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

    // Check sender balance
    if (Number(senderWallet.balance) < numericAmount) {
      await client.query("ROLLBACK");

      return res.status(400).json({
        error: "Insufficient wallet balance",
      });
    }

    // Deduct money from sender
    const senderUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance - $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, sender_user_id]
    );

    // Add money to receiver
    const receiverUpdate = await client.query(
      `UPDATE wallets
       SET balance = balance + $1
       WHERE user_id = $2
       RETURNING *`,
      [numericAmount, receiver_user_id]
    );

    // Create transaction record
    const transactionResult = await client.query(
      `INSERT INTO transactions
       (user_id, sender_user_id, receiver_user_id, type, amount, status, idempotency_key)
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

    await client.query("COMMIT");

    res.status(201).json({
      message: "Payment transferred successfully",
      sender_wallet: senderUpdate.rows[0],
      receiver_wallet: receiverUpdate.rows[0],
      transaction: transactionResult.rows[0],
    });
  } catch (error) {
    await client.query("ROLLBACK");

    // Handle duplicate idempotency key
    if (error.code === "23505") {
      const existingTransaction = await client.query(
        `SELECT * FROM transactions
         WHERE idempotency_key = $1`,
        [idempotencyKey]
      );

      if (existingTransaction.rows.length > 0) {
        return res.status(200).json({
          message: "Payment already processed",
          transaction: existingTransaction.rows[0],
        });
      }
    }

    console.error(error);

    res.status(500).json({
      error: "Failed to transfer payment",
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

app.post(
  "/transactions/:transactionId/refund",
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

const PORT = process.env.PORT || 8000;

app.listen(PORT, () => {
  console.log(`The server is running on PORT ${PORT}`);
});
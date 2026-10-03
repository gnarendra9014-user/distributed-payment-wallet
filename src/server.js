const express =require("express");
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
      (user_id, sender_user_id, receiver_user_id, type, amount, status, idempotency_key)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING *`,
    [   
      sender_user_id,
      sender_user_id,
      receiver_user_id,
      "TRANSFER",
      amount,
      "SUCCESS",
      idempotencyKey,
      ]  
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
app.post('/users' ,async(req ,res) =>{
    try{
        const {name , email} =req.body;
        const result =await pool.query(
            "Insert into users(name,email) values($1,$2) Returning *",
            [name ,email]
        );
        res.status(201).json(result.rows[0]);
    }
    catch(error){
        console.error(error);

        res.status(500).json({
            error : "Failed to create user",
        });
    }
});


app.post("/wallets/transfer", async (req, res) => {
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

    const { sender_user_id, receiver_user_id, amount } = req.body;

    // Basic validation
    if (!sender_user_id || !receiver_user_id || !amount) {
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
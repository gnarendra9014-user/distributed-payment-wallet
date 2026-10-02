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
      `INSERT INTO transactions (user_id, type, amount, status)
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

app.listen(process.env.PORT ,() =>{
    console.log("The server is running on PORT 8000");
})
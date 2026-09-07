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
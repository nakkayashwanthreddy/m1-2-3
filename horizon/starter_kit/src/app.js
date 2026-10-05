const transferRoutes = require("./routes/transferRoutes");
const express = require("express");
const pool = require("./config/database");
const accountRoutes = require("./routes/accountRoutes");

const app = express();

app.use(express.json());

app.get("/health", async (req, res) => {
    try {
        await pool.query("SELECT 1");

        res.status(200).json({
            status: "UP",
            message: "Bank Account API is running",
            database: "UP"
        });
    } catch (error) {
        console.error("Database health check failed:", error.message);

        res.status(503).json({
            status: "DOWN",
            message: "Bank Account API is running",
            database: "DOWN"
        });
    }
});

app.use("/api/v1/accounts", accountRoutes);
app.use("/api/v1/transfers", transferRoutes);

module.exports = app;
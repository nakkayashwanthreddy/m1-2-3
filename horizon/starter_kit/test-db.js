const pool = require("./src/config/database");

async function testConnection() {
    try {
        const result = await pool.query("SELECT NOW()");
        console.log("PostgreSQL connected successfully!");
        console.log("Database time:", result.rows[0].now);
    } catch (error) {
        console.error("PostgreSQL connection failed!");
        console.error(error.message);
    } finally {
        await pool.end();
    }
}

testConnection();
const pool = require("../config/database");

async function findAccountByNumber(accountNumber) {
    const result = await pool.query(
        `SELECT
            a.account_number,
            c.customer_id,
            c.full_name,
            a.account_type,
            a.currency,
            a.balance,
            a.status,
            a.opened_at
         FROM account a
         JOIN customer c
           ON a.customer_id = c.customer_id
         WHERE a.account_number = $1`,
        [accountNumber]
    );

    return result.rows[0] || null;
}

module.exports = {
    findAccountByNumber
};
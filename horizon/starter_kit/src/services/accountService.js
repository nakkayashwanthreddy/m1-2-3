const pool = require("../config/database");

async function createAccount(data) {
    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        // 1. Create or find customer using PAN
        let customerResult = await client.query(
            `SELECT customer_id
             FROM customer
             WHERE pan = $1`,
            [data.pan]
        );

        let customerId;

        if (customerResult.rows.length > 0) {
            customerId = customerResult.rows[0].customer_id;
        } else {
            const insertCustomer = await client.query(
                `INSERT INTO customer
                 (full_name, email, phone, date_of_birth, pan)
                 VALUES ($1, $2, $3, $4, $5)
                 RETURNING customer_id`,
                [
                    data.fullName,
                    data.email,
                    data.phone,
                    data.dateOfBirth,
                    data.pan
                ]
            );

            customerId = insertCustomer.rows[0].customer_id;
        }

        // 2. Create account
        const accountResult = await client.query(
            `INSERT INTO account
             (customer_id, account_type, balance)
             VALUES ($1, $2, $3)
             RETURNING account_id, account_number, account_type,
                       currency, balance, status, opened_at`,
            [
                customerId,
                data.accountType,
                data.initialDeposit
            ]
        );

        const account = accountResult.rows[0];

        // 3. Create initial deposit ledger entry
        await client.query(
            `INSERT INTO account_transaction
             (account_id, txn_type, amount, balance_after, description)
             VALUES ($1, 'CREDIT', $2, $3, 'Initial deposit')`,
            [
                account.account_id,
                data.initialDeposit,
                account.balance
            ]
        );

        await client.query("COMMIT");

        return account;

    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

module.exports = {
    createAccount
};
async function getAccount(accountNumber) {
    const account = await require("../repositories/accountRepository")
        .findAccountByNumber(accountNumber);

    return account;
}

module.exports = {
    createAccount,
    getAccount
};
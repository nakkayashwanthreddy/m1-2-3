async function lockAccounts(client, fromAccountNumber, toAccountNumber) {
    const result = await client.query(
        `SELECT
            account_id,
            account_number,
            balance,
            status
         FROM account
         WHERE account_number = ANY($1)
         ORDER BY account_id
         FOR UPDATE`,
        [[fromAccountNumber, toAccountNumber]]
    );

    return result.rows;
}

async function getSentToday(client, accountId) {
    const result = await client.query(
        `SELECT COALESCE(SUM(amount), 0) AS sent_today
         FROM fund_transfer
         WHERE from_account_id = $1
           AND status = 'COMPLETED'
           AND created_at >= date_trunc(
               'day',
               now() AT TIME ZONE 'Asia/Kolkata'
           ) AT TIME ZONE 'Asia/Kolkata'`,
        [accountId]
    );

    return result.rows[0].sent_today;
}

async function createTransfer(
    client,
    idempotencyKey,
    fromAccountId,
    toAccountId,
    amount,
    remarks
) {
    const result = await client.query(
        `INSERT INTO fund_transfer
            (idempotency_key, from_account_id, to_account_id, amount, remarks)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING transfer_id, reference_no, created_at`,
        [
            idempotencyKey,
            fromAccountId,
            toAccountId,
            amount,
            remarks || null
        ]
    );

    return result.rows[0];
}
async function updateBalances(
    client,
    fromAccountId,
    toAccountId,
    amount
) {
    await client.query(
        `UPDATE account
         SET balance = balance - $1,
             version = version + 1,
             updated_at = now()
         WHERE account_id = $2`,
        [amount, fromAccountId]
    );

    await client.query(
        `UPDATE account
         SET balance = balance + $1,
             version = version + 1,
             updated_at = now()
         WHERE account_id = $2`,
        [amount, toAccountId]
    );
}
async function createLedgerEntries(
    client,
    transferId,
    fromAccountId,
    toAccountId,
    amount,
    fromBalanceAfter,
    toBalanceAfter,
    remarks
) {
    await client.query(
        `INSERT INTO account_transaction
            (account_id, transfer_id, txn_type, amount, balance_after, description)
         VALUES
            ($1, $2, 'DEBIT', $3, $4, $5),
            ($6, $2, 'CREDIT', $3, $7, $5)`,
        [
            fromAccountId,
            transferId,
            amount,
            fromBalanceAfter,
            remarks || "Fund transfer",
            toAccountId,
            toBalanceAfter
        ]
    );
}
async function findTransferByIdempotencyKey(client, idempotencyKey) {
    const result = await client.query(
        `SELECT
            ft.transfer_id,
            ft.reference_no,
            ft.idempotency_key,
            ft.amount,
            ft.remarks,
            ft.status,
            ft.created_at,
            fa.account_number AS from_account_number,
            ta.account_number AS to_account_number
         FROM fund_transfer ft
         JOIN account fa
           ON ft.from_account_id = fa.account_id
         JOIN account ta
           ON ft.to_account_id = ta.account_id
         WHERE ft.idempotency_key = $1`,
        [idempotencyKey]
    );

    return result.rows[0] || null;
}
module.exports = {
    lockAccounts,
    getSentToday,
    createTransfer,
    updateBalances,
    createLedgerEntries,
    findTransferByIdempotencyKey
};
const pool = require("../config/database");

const {
    lockAccounts,
    getSentToday,
    createTransfer,
    updateBalances,
    createLedgerEntries
} = require("../repositories/transferRepository");

async function createFundTransfer(data, idempotencyKey) {
    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        // 1. Validate same-account transfer
        if (data.fromAccountNumber === data.toAccountNumber) {
            const error = new Error("Cannot transfer to the same account");
            error.code = "SAME_ACCOUNT_TRANSFER";
            throw error;
        }

        // 2. Lock both accounts in account_id order
        const accounts = await lockAccounts(
            client,
            data.fromAccountNumber,
            data.toAccountNumber
        );

        // 3. Check both accounts exist
        if (accounts.length !== 2) {
            const error = new Error("One or both accounts were not found");
            error.code = "ACCOUNT_NOT_FOUND";
            throw error;
        }

        const fromAccount = accounts.find(
            account => account.account_number === data.fromAccountNumber
        );

        const toAccount = accounts.find(
            account => account.account_number === data.toAccountNumber
        );

        // 4. Check account status
        if (
            fromAccount.status !== "ACTIVE" ||
            toAccount.status !== "ACTIVE"
        ) {
            const error = new Error("Both accounts must be ACTIVE");
            error.code = "ACCOUNT_NOT_ACTIVE";
            throw error;
        }

        const amount = Number(data.amount);

        // 5. Check sufficient funds
        if (Number(fromAccount.balance) < amount) {
            const error = new Error("Insufficient funds");
            error.code = "INSUFFICIENT_FUNDS";
            throw error;
        }

        // 6. Check daily transfer limit
        const sentToday = await getSentToday(
            client,
            fromAccount.account_id
        );

        if (Number(sentToday) + amount > 500000) {
            const error = new Error("Daily transfer limit exceeded");
            error.code = "DAILY_LIMIT_EXCEEDED";
            throw error;
        }

        // 7. Create transfer record
        const transfer = await createTransfer(
            client,
            idempotencyKey,
            fromAccount.account_id,
            toAccount.account_id,
            data.amount,
            data.remarks
        );

        // 8. Calculate new balances
        const fromBalanceAfter =
            Number(fromAccount.balance) - amount;

        const toBalanceAfter =
            Number(toAccount.balance) + amount;

        // 9. Update both balances
        await updateBalances(
            client,
            fromAccount.account_id,
            toAccount.account_id,
            data.amount
        );

        // 10. Create debit + credit ledger entries
        await createLedgerEntries(
            client,
            transfer.transfer_id,
            fromAccount.account_id,
            toAccount.account_id,
            data.amount,
            fromBalanceAfter,
            toBalanceAfter,
            data.remarks
        );

        // 11. Commit everything
        await client.query("COMMIT");

        return {
            transferId: transfer.transfer_id,
            referenceNo: transfer.reference_no,
            fromAccountNumber: fromAccount.account_number,
            toAccountNumber: toAccount.account_number,
            amount: Number(data.amount).toFixed(2),
            status: "COMPLETED",
            remarks: data.remarks || null,
            createdAt: transfer.created_at
        };

    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

module.exports = {
    createFundTransfer
};
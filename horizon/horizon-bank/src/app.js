const express = require("express");
const { pool, redis, producer, consumer } = require("./config");

const app = express();

app.use(express.json());

const PORT = process.env.PORT || 8080;

const TRANSFER_TOPIC = "bank.transfer.completed.v1";
const ACCOUNT_TOPIC = "bank.account.opened.v1";
const FAILED_TOPIC = "bank.transfer.failed.v1";

function now() {
    return new Date().toISOString();
}

function accountResponse(account) {
    return {
        accountNumber: account.account_number,
        customerId: account.customer_id,
        fullName: account.full_name,
        accountType: account.account_type,
        currency: account.currency,
        balance: Number(account.balance),
        status: account.status,
        openedAt: account.opened_at
    };
}

function transferResponse(transfer) {
    return {
        referenceNo: transfer.reference_no,
        fromAccount: transfer.from_account,
        toAccount: transfer.to_account,
        amount: Number(transfer.amount),
        currency: "INR",
        status: transfer.status,
        createdAt: transfer.created_at
    };
}

async function sendKafka(topic, event) {
    await producer.send({
        topic,
        messages: [
            {
                value: JSON.stringify(event)
            }
        ]
    });
}

async function publishFailedTransfer(request, reason) {
    await sendKafka(FAILED_TOPIC, {
        fromAccount: request.fromAccount,
        toAccount: request.toAccount,
        amount: request.amount,
        currency: request.currency,
        reason,
        failedAt: now()
    });
}

function validateAccountRequest(request) {
    const {
        fullName,
        email,
        phone,
        dateOfBirth,
        pan,
        accountType,
        initialDeposit
    } = request;

    if (!fullName || fullName.trim().length > 100) {
        throw new Error("Invalid full name");
    }

    if (
        !email ||
        !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)
    ) {
        throw new Error("Invalid email");
    }

    if (!phone || !/^[6-9][0-9]{9}$/.test(phone)) {
        throw new Error("Invalid phone number");
    }

    if (!dateOfBirth) {
        throw new Error("Date of birth is required");
    }

    const dob = new Date(dateOfBirth);

    if (Number.isNaN(dob.getTime()) || dob >= new Date()) {
        throw new Error("Invalid date of birth");
    }

    const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]$/;

    if (!panRegex.test(pan)) {
        throw new Error("Invalid PAN");
    }

    if (!["SAVINGS", "CURRENT"].includes(accountType)) {
        throw new Error("Account type must be SAVINGS or CURRENT");
    }

    const amount = Number(initialDeposit);

    if (!Number.isFinite(amount) || amount <= 0) {
        throw new Error("Invalid initial deposit");
    }

    if (amount > 1000000) {
        throw new Error("Initial deposit cannot exceed 1000000.00");
    }
}

function calculateAge(dateOfBirth) {
    const dob = new Date(dateOfBirth);
    const today = new Date();

    let age = today.getFullYear() - dob.getFullYear();

    const monthDifference =
        today.getMonth() - dob.getMonth();

    if (
        monthDifference < 0 ||
        (
            monthDifference === 0 &&
            today.getDate() < dob.getDate()
        )
    ) {
        age--;
    }

    return age;
}

async function openAccount(request) {
    validateAccountRequest(request);

    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const email = request.email.trim().toLowerCase();
        const pan = request.pan.trim().toUpperCase();
        const phone = request.phone.trim();
        const accountType = request.accountType
            .trim()
            .toUpperCase();

        if (calculateAge(request.dateOfBirth) < 18) {
            throw new Error(
                "Customer must be at least 18 years old"
            );
        }

        const minimumDeposit =
            accountType === "SAVINGS"
                ? 1000
                : 5000;

        const initialDeposit =
            Number(request.initialDeposit);

        if (initialDeposit < minimumDeposit) {
            throw new Error(
                `Initial deposit for ${accountType} must be at least ${minimumDeposit}.00`
            );
        }

        let customerResult = await client.query(
            `SELECT *
             FROM customer
             WHERE pan = $1
             FOR UPDATE`,
            [pan]
        );

        let customer;

        if (customerResult.rows.length === 0) {
            const emailExists = await client.query(
                `SELECT customer_id
                 FROM customer
                 WHERE email = $1`,
                [email]
            );

            if (emailExists.rows.length > 0) {
                throw new Error(
                    "Customer with this email already exists"
                );
            }

            const phoneExists = await client.query(
                `SELECT customer_id
                 FROM customer
                 WHERE phone = $1`,
                [phone]
            );

            if (phoneExists.rows.length > 0) {
                throw new Error(
                    "Customer with this phone already exists"
                );
            }

            const result = await client.query(
                `INSERT INTO customer
                (
                    full_name,
                    email,
                    phone,
                    date_of_birth,
                    pan,
                    created_at
                )
                VALUES ($1,$2,$3,$4,$5,$6)
                RETURNING *`,
                [
                    request.fullName.trim(),
                    email,
                    phone,
                    request.dateOfBirth,
                    pan,
                    now()
                ]
            );

            customer = result.rows[0];
        } else {
            customer = customerResult.rows[0];

            if (customer.email !== email) {
                throw new Error(
                    "PAN already belongs to a customer with a different email"
                );
            }

            if (customer.phone !== phone) {
                throw new Error(
                    "PAN already belongs to a customer with a different phone"
                );
            }
        }

        const existingAccount = await client.query(
            `SELECT account_id
             FROM account
             WHERE customer_id = $1
             AND account_type = $2`,
            [
                customer.customer_id,
                accountType
            ]
        );

        if (existingAccount.rows.length > 0) {
            throw new Error(
                `Customer already has a ${accountType} account`
            );
        }

        const accountResult = await client.query(
            `INSERT INTO account
            (
                customer_id,
                account_type,
                currency,
                balance,
                status,
                version,
                opened_at,
                updated_at
            )
            VALUES ($1,$2,'INR',$3,'ACTIVE',0,$4,$4)
            RETURNING *`,
            [
                customer.customer_id,
                accountType,
                initialDeposit,
                now()
            ]
        );

        const account = accountResult.rows[0];

        await client.query(
            `INSERT INTO account_transaction
            (
                account_id,
                transfer_id,
                txn_type,
                amount,
                balance_after,
                description,
                txn_time
            )
            VALUES ($1,NULL,'CREDIT',$2,$2,$3,$4)`,
            [
                account.account_id,
                initialDeposit,
                "Initial deposit",
                now()
            ]
        );

        const event = {
            accountNumber: account.account_number,
            customerId: customer.customer_id,
            fullName: customer.full_name,
            accountType: account.account_type,
            currency: account.currency,
            initialDeposit,
            openedAt: account.opened_at
        };

        await sendKafka(
            ACCOUNT_TOPIC,
            event
        );

        await client.query("COMMIT");

        return accountResponse({
            ...account,
            customer_id: customer.customer_id,
            full_name: customer.full_name
        });

    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

async function getAccount(accountNumber) {
    const key =
        `horizon:account:${accountNumber}`;

    const cached = await redis.get(key);

    if (cached) {
        console.log("redis hit");
        return JSON.parse(cached);
    }

    console.log("redis miss");

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
           ON c.customer_id = a.customer_id
         WHERE a.account_number = $1`,
        [accountNumber]
    );

    if (result.rows.length === 0) {
        throw new Error(
            `Account not found: ${accountNumber}`
        );
    }

    const response =
        accountResponse(result.rows[0]);

    await redis.set(
        key,
        JSON.stringify(response)
    );

    return response;
}

async function getStatement(accountNumber) {
    const versionKey =
        `bank:stmt-ver:${accountNumber}`;

    let version =
        await redis.get(versionKey);

    if (version === null) {
        version = "0";
    }

    const statementKey =
        `bank:stmt:${accountNumber}:v${version}:all`;

    try {
        const cached =
            await redis.get(statementKey);

        if (cached) {
            console.log("statement redis hit");

            return {
                statements: JSON.parse(cached),
                cacheHit: true
            };
        }
    } catch (error) {
        console.log(
            "Redis unavailable while reading statement"
        );
    }

    console.log("statement redis miss");

    const accountResult = await pool.query(
        `SELECT account_id
         FROM account
         WHERE account_number = $1`,
        [accountNumber]
    );

    if (accountResult.rows.length === 0) {
        throw new Error(
            `Account not found: ${accountNumber}`
        );
    }

    const accountId =
        accountResult.rows[0].account_id;

    const result = await pool.query(
        `SELECT
            txn_id,
            txn_type,
            amount,
            balance_after,
            description,
            txn_time
         FROM account_transaction
         WHERE account_id = $1
         ORDER BY txn_time DESC, txn_id DESC`,
        [accountId]
    );

    const statements = result.rows.map(row => ({
        txnId: row.txn_id,
        txnType: row.txn_type,
        amount: Number(row.amount),
        balanceAfter: Number(row.balance_after),
        description: row.description,
        txnTime: row.txn_time
    }));

    try {
        await redis.set(
            statementKey,
            JSON.stringify(statements),
            "EX",
            300
        );
    } catch (error) {
        console.log(
            "Redis unavailable while storing statement"
        );
    }

    return {
        statements,
        cacheHit: false
    };
}

async function transfer(request, idempotencyKey) {
    if (
        !idempotencyKey ||
        !idempotencyKey.trim()
    ) {
        throw new Error(
            "Idempotency-Key is required"
        );
    }

    idempotencyKey =
        idempotencyKey.trim();

    if (idempotencyKey.length > 64) {
        throw new Error(
            "Idempotency-Key must not exceed 64 characters"
        );
    }

    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        const existingResult =
            await client.query(
                `SELECT
                    f.reference_no,
                    f.amount,
                    f.status,
                    f.created_at,
                    fa.account_number AS from_account,
                    ta.account_number AS to_account
                 FROM fund_transfer f
                 JOIN account fa
                   ON fa.account_id = f.from_account_id
                 JOIN account ta
                   ON ta.account_id = f.to_account_id
                 WHERE f.idempotency_key = $1`,
                [idempotencyKey]
            );

        if (existingResult.rows.length > 0) {
            await client.query("COMMIT");

            return transferResponse(
                existingResult.rows[0]
            );
        }

        if (
            !request.fromAccount ||
            !request.toAccount
        ) {
            throw new Error(
                "Sender and receiver accounts are required"
            );
        }

        if (
            request.fromAccount ===
            request.toAccount
        ) {
            await publishFailedTransfer(
                request,
                "Sender and receiver accounts must be different"
            );

            throw new Error(
                "Sender and receiver accounts must be different"
            );
        }

        if (
            !request.currency ||
            request.currency.toUpperCase() !== "INR"
        ) {
            await publishFailedTransfer(
                request,
                "Only INR transfers are supported"
            );

            throw new Error(
                "Only INR transfers are supported"
            );
        }

        const amount =
            Number(request.amount);

        if (
            !Number.isFinite(amount) ||
            amount < 1
        ) {
            await publishFailedTransfer(
                request,
                "Amount should be at least 1"
            );

            throw new Error(
                "Transfer amount must be at least one"
            );
        }

        if (
            Math.round(amount * 100) !==
            amount * 100
        ) {
            await publishFailedTransfer(
                request,
                "Amount should have only 2 decimal places"
            );

            throw new Error(
                "Transfer amount can have at most 2 decimal places"
            );
        }

        if (amount > 200000) {
            await publishFailedTransfer(
                request,
                "Max transfer is 200000.00"
            );

            throw new Error(
                "Maximum transfer amount is 200000.00"
            );
        }

        const accountsResult =
            await client.query(
                `SELECT *
                 FROM account
                 WHERE account_number = $1
                 OR account_number = $2
                 FOR UPDATE`,
                [
                    request.fromAccount,
                    request.toAccount
                ]
            );

        const accounts =
            accountsResult.rows;

        const fromAccount =
            accounts.find(
                a =>
                    a.account_number ===
                    request.fromAccount
            );

        const toAccount =
            accounts.find(
                a =>
                    a.account_number ===
                    request.toAccount
            );

        if (!fromAccount) {
            await publishFailedTransfer(
                request,
                "Sender account not found"
            );

            throw new Error(
                "Sender account not found"
            );
        }

        if (!toAccount) {
            await publishFailedTransfer(
                request,
                "Receiver account not found"
            );

            throw new Error(
                "Receiver account not found"
            );
        }

        if (fromAccount.status !== "ACTIVE") {
            await publishFailedTransfer(
                request,
                "Sender account is not ACTIVE"
            );

            throw new Error(
                "Sender account is not ACTIVE"
            );
        }

        if (toAccount.status !== "ACTIVE") {
            await publishFailedTransfer(
                request,
                "Receiver account is not ACTIVE"
            );

            throw new Error(
                "Receiver account is not ACTIVE"
            );
        }

        if (
            Number(fromAccount.balance) <
            amount
        ) {
            await publishFailedTransfer(
                request,
                "Insufficient account balance"
            );

            throw new Error(
                "Insufficient account balance"
            );
        }

        const todayStart =
            new Date();

        todayStart.setHours(
            0, 0, 0, 0
        );

        const tomorrow =
            new Date(todayStart);

        tomorrow.setDate(
            tomorrow.getDate() + 1
        );

        const outgoingResult =
            await client.query(
                `SELECT COALESCE(SUM(amount),0) AS total
                 FROM fund_transfer
                 WHERE from_account_id = $1
                 AND status = 'COMPLETED'
                 AND created_at >= $2
                 AND created_at < $3`,
                [
                    fromAccount.account_id,
                    todayStart,
                    tomorrow
                ]
            );

        const todayOutgoing =
            Number(
                outgoingResult.rows[0].total
            );

        if (
            todayOutgoing + amount >
            500000
        ) {
            await publishFailedTransfer(
                request,
                "Daily outgoing transfer limit exceeded"
            );

            throw new Error(
                "Daily outgoing transfer limit of 500000.00 exceeded"
            );
        }

        const senderNewBalance =
            Number(fromAccount.balance) -
            amount;

        const receiverNewBalance =
            Number(toAccount.balance) +
            amount;

        await client.query(
            `UPDATE account
             SET balance = $1,
                 updated_at = $2,
                 version = version + 1
             WHERE account_id = $3`,
            [
                senderNewBalance,
                now(),
                fromAccount.account_id
            ]
        );

        await client.query(
            `UPDATE account
             SET balance = $1,
                 updated_at = $2,
                 version = version + 1
             WHERE account_id = $3`,
            [
                receiverNewBalance,
                now(),
                toAccount.account_id
            ]
        );

        const transferResult =
            await client.query(
                `INSERT INTO fund_transfer
                (
                    idempotency_key,
                    from_account_id,
                    to_account_id,
                    amount,
                    status,
                    created_at
                )
                VALUES ($1,$2,$3,$4,'COMPLETED',$5)
                RETURNING *`,
                [
                    idempotencyKey,
                    fromAccount.account_id,
                    toAccount.account_id,
                    amount,
                    now()
                ]
            );

        const savedTransfer =
            transferResult.rows[0];

        await client.query(
            `INSERT INTO account_transaction
            (
                account_id,
                transfer_id,
                txn_type,
                amount,
                balance_after,
                description,
                txn_time
            )
            VALUES
            ($1,$2,'DEBIT',$3,$4,$5,$6),
            ($7,$2,'CREDIT',$3,$8,$9,$10)`,
            [
                fromAccount.account_id,
                savedTransfer.transfer_id,
                amount,
                senderNewBalance,
                `Fund transfer to ${toAccount.account_number}`,
                now(),
                toAccount.account_id,
                receiverNewBalance,
                `Fund transfer from ${fromAccount.account_number}`,
                now()
            ]
        );

        await redis.del(
            `horizon:account:${fromAccount.account_number}`,
            `horizon:account:${toAccount.account_number}`
        );

        const event = {
            referenceNo:
                savedTransfer.reference_no,
            fromAccount:
                fromAccount.account_number,
            toAccount:
                toAccount.account_number,
            amount,
            currency: "INR"
        };

        await sendKafka(
            TRANSFER_TOPIC,
            event
        );

        await client.query("COMMIT");

        return transferResponse({
            ...savedTransfer,
            from_account:
                fromAccount.account_number,
            to_account:
                toAccount.account_number
        });

    } catch (error) {
        await client.query("ROLLBACK");
        throw error;
    } finally {
        client.release();
    }
}

async function getTransfer(referenceNo) {
    const result = await pool.query(
        `SELECT
            f.reference_no,
            f.amount,
            f.status,
            f.created_at,
            fa.account_number AS from_account,
            ta.account_number AS to_account
         FROM fund_transfer f
         JOIN account fa
           ON fa.account_id = f.from_account_id
         JOIN account ta
           ON ta.account_id = f.to_account_id
         WHERE f.reference_no = $1`,
        [referenceNo]
    );

    if (result.rows.length === 0) {
        throw new Error(
            "Transfer not found"
        );
    }

    return transferResponse(
        result.rows[0]
    );
}

/* ACCOUNT APIs */

app.post(
    "/api/v1/accounts",
    async (req, res) => {
        try {
            const response =
                await openAccount(req.body);

            res.status(201).json(response);

        } catch (error) {
            res.status(400).json({
                message: error.message
            });
        }
    }
);

app.get(
    "/api/v1/accounts/:accountNumber",
    async (req, res) => {
        try {
            const response =
                await getAccount(
                    req.params.accountNumber
                );

            res.json(response);

        } catch (error) {
            res.status(404).json({
                message: error.message
            });
        }
    }
);

app.get(
    "/api/v1/accounts/:accountNumber/statement",
    async (req, res) => {
        try {
            const result =
                await getStatement(
                    req.params.accountNumber
                );

            res
                .set(
                    "X-Cache",
                    result.cacheHit
                        ? "HIT"
                        : "MISS"
                )
                .json(result.statements);

        } catch (error) {
            res.status(404).json({
                message: error.message
            });
        }
    }
);

/* TRANSFER APIs */

app.post(
    "/api/v1/transfers",
    async (req, res) => {
        try {
            const idempotencyKey =
                req.header(
                    "Idempotency-Key"
                );

            const response =
                await transfer(
                    req.body,
                    idempotencyKey
                );

            res.status(201).json(response);

        } catch (error) {
            res.status(400).json({
                message: error.message
            });
        }
    }
);

app.get(
    "/api/v1/transfers/:referenceNo",
    async (req, res) => {
        try {
            const response =
                await getTransfer(
                    req.params.referenceNo
                );

            res.json(response);

        } catch (error) {
            res.status(404).json({
                message: error.message
            });
        }
    }
);

/* KAFKA CONSUMER */

async function startKafkaConsumer() {
    await producer.connect();

    await consumer.connect();

    await consumer.subscribe({
        topic: TRANSFER_TOPIC,
        fromBeginning: false
    });

    await consumer.subscribe({
        topic: ACCOUNT_TOPIC,
        fromBeginning: false
    });

    await consumer.subscribe({
        topic: FAILED_TOPIC,
        fromBeginning: false
    });

    await consumer.run({
        eachMessage: async ({
            topic,
            message
        }) => {
            const value =
                message.value?.toString();

            if (topic === TRANSFER_TOPIC) {
                console.log(
                    "Received successful transfer event:",
                    value
                );
            }

            if (topic === ACCOUNT_TOPIC) {
                console.log(
                    "Received account opened event:",
                    value
                );
            }

            if (topic === FAILED_TOPIC) {
                console.log(
                    "Received failed transfer event:",
                    value
                );
            }
        }
    });
}

/* SERVER */

async function startServer() {
    try {
        await pool.query("SELECT 1");

        console.log(
            "PostgreSQL connected"
        );

        await redis.ping();

        console.log(
            "Redis connected"
        );

        await startKafkaConsumer();

        console.log(
            "Kafka connected"
        );

        app.listen(
            PORT,
            () => {
                console.log(
                    `Horizon Bank API running on port ${PORT}`
                );
            }
        );

    } catch (error) {
        console.error(
            "Startup failed:",
            error
        );

        process.exit(1);
    }
}

startServer();
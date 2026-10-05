const express = require("express");
const { z } = require("zod");
const { createFundTransfer } = require("../services/transferService");

const router = express.Router();

const transferSchema = z.object({
    fromAccountNumber: z.string().regex(/^\d{12}$/),
    toAccountNumber: z.string().regex(/^\d{12}$/),
    amount: z.string().regex(
        /^\d+(\.\d{1,2})?$/,
        "Amount must have at most 2 decimal places"
    ),
    remarks: z.string().max(100).optional()
});

router.post("/", async (req, res) => {
    try {
        const validatedData = transferSchema.parse(req.body);

        const idempotencyKey = req.headers["idempotency-key"];

        if (
            !idempotencyKey ||
            !/^[A-Za-z0-9_-]{1,64}$/.test(idempotencyKey)
        ) {
            return res.status(400).json({
                error: "MISSING_IDEMPOTENCY_KEY",
                message: "A valid Idempotency-Key header is required"
            });
        }

        const result = await createFundTransfer(
            validatedData,
            idempotencyKey
        );

        res.status(201).json(result);

    } catch (error) {

        if (error instanceof z.ZodError) {
            return res.status(400).json({
                error: "VALIDATION_ERROR",
                message: "Invalid transfer request",
                details: error.issues.map(issue => ({
                    field: issue.path.join("."),
                    message: issue.message
                }))
            });
        }

        const statusCodes = {
            SAME_ACCOUNT_TRANSFER: 422,
            ACCOUNT_NOT_FOUND: 404,
            ACCOUNT_NOT_ACTIVE: 422,
            INSUFFICIENT_FUNDS: 422,
            DAILY_LIMIT_EXCEEDED: 422
        };

        const status = statusCodes[error.code] || 500;

        res.status(status).json({
            error: error.code || "TRANSFER_FAILED",
            message: error.message
        });
    }
});

module.exports = router;
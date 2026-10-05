const express = require("express");
const { createAccount, getAccount } = require("../services/accountService");
const { z } = require("zod");

const router = express.Router();

const accountSchema = z.object({
    fullName: z.string().min(2).max(100),

    email: z.string().email().transform(value => value.toLowerCase()),

    phone: z.string().regex(/^[6-9][0-9]{9}$/),

    dateOfBirth: z.string().regex(
        /^\d{4}-\d{2}-\d{2}$/,
        "Date must be in yyyy-MM-dd format"
    ),

    pan: z.string().regex(
        /^[A-Z]{5}[0-9]{4}[A-Z]$/,
        "PAN must be in valid format"
    ),

    accountType: z.enum(["SAVINGS", "CURRENT"]),

    initialDeposit: z.string().regex(
        /^\d+(\.\d{2})$/,
        "Initial deposit must have exactly 2 decimal places"
    )
});

router.post("/", async (req, res) => {
    try {
        const validatedData = accountSchema.parse(req.body);

        const account = await createAccount(validatedData);

        res.status(201).json({
            accountNumber: account.account_number,
            accountType: account.account_type,
            currency: account.currency,
            balance: Number(account.balance).toFixed(2),
            status: account.status,
            openedAt: account.opened_at
        });
    } catch (error) {
        if (error instanceof z.ZodError) {
            return res.status(400).json({
                error: "VALIDATION_ERROR",
                message: "Invalid account creation request",
                details: error.issues.map(issue => ({
                    field: issue.path.join("."),
                    message: issue.message
            }))
        });
    }

      console.error("Account creation failed:", error.message);

      res.status(500).json({
          error: "ACCOUNT_CREATION_FAILED",
          message: error.message
    });
}});

module.exports = router;
router.get("/:accountNumber", async (req, res) => {
    try {
        const account = await getAccount(req.params.accountNumber);

        if (!account) {
            return res.status(404).json({
                error: "ACCOUNT_NOT_FOUND",
                message: "Account not found"
            });
        }

        res.status(200).json({
            accountNumber: account.account_number,
            customerId: account.customer_id,
            fullName: account.full_name,
            accountType: account.account_type,
            currency: account.currency,
            balance: Number(account.balance).toFixed(2),
            status: account.status,
            openedAt: account.opened_at
        });
    } catch (error) {
        console.error("Account enquiry failed:", error.message);

        res.status(500).json({
            error: "ACCOUNT_ENQUIRY_FAILED",
            message: error.message
        });
    }
});
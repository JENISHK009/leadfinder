import express from "express";
import { webhook } from "../controllers/index.js";

const router = express.Router();

router.post(
  "/",
  (req, res, next) => {
    console.log("Raw Body:", req.body.toString("utf8")); // Log the raw body for debugging
    next();
  },
  webhook.handleStripeWebhook // Pass control to the webhook handler
);

export default router;
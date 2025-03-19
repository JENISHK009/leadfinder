import stripe from "../config/stripe.js";
import pool from "../config/db.js";
import { errorResponse } from "../utils/index.js";

const handleSubscriptionPurchase = async (userId, planId, type) => {
  // Fetch the subscription plan details
  const planQuery = await pool.query(
    "SELECT points FROM subscription_plans WHERE id = $1",
    [planId]
  );
  const plan = planQuery.rows[0];

  if (!plan) {
    throw new Error("Subscription plan not found");
  }

  let planPoints = plan.points;

  // Adjust points based on the subscription type
  if (type === "yearly") {
    planPoints *= 12; // Multiply by 12 for yearly subscriptions
  }

  // Update the user's credits
  await pool.query(
    `UPDATE users 
     SET credits = credits + $1 
     WHERE id = $2`,
    [planPoints, userId]
  );

  // Determine the subscription duration based on the type
  const duration = type === "monthly" ? "1 month" : "1 year";

  // Insert the subscription into the user_subscriptions table
  await pool.query(
    `INSERT INTO user_subscriptions (user_id, plan_id, status, starts_at, ends_at, type)
     VALUES ($1, $2, $3, NOW(), NOW() + INTERVAL '${duration}', $4)`,
    [userId, planId, "active", type]
  );

  console.log(`Subscription successfully activated for user ${userId}`);
};

const handleExtraCreditPurchase = async (userId, credits) => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Update the user's credits
    await client.query(
      `UPDATE users 
       SET credits = credits + $1 
       WHERE id = $2`,
      [credits, userId]
    );

    // Insert the extra credit purchase into the user_subscriptions table
    await client.query(
      `INSERT INTO user_subscriptions (user_id, status, starts_at, ends_at, comment)
       VALUES ($1, $2, NOW(), NOW(), $3)`,
      [
        userId,
        "active", // Status for extra credit purchase
        `Extra credits purchased: ${credits} credits`, // Comment
      ]
    );

    await client.query("COMMIT");
    console.log(`Extra credits successfully added for user ${userId}`);
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error processing extra credit purchase:", error);
    throw error;
  } finally {
    client.release();
  }
};

const handleStripeWebhook = async (req, res) => {
  const sig = req.headers["stripe-signature"];
  const endpointSecret = process.env.STRIPE_WEBHOOK_SECRET;

  let event;

  try {
    // Verify the webhook signature using the raw body (Buffer)
    event = stripe.webhooks.constructEvent(req.body, sig, endpointSecret);
  } catch (err) {
    console.error("Webhook signature verification failed:", err);
    return errorResponse(res, "Webhook signature verification failed", 400);
  }

  // Handle the event
  switch (event.type) {
    case "checkout.session.completed":
      const session = event.data.object;

      // Extract metadata
      const { userId, planId, type, credits } = session.metadata;

      if (!userId || !planId || !type) {
        console.error("Missing metadata: userId, planId, or type");
        return errorResponse(
          res,
          "Missing metadata: userId, planId, or type",
          400
        );
      }

      try {
        if (type === "monthly" || type === "yearly") {
          // Handle subscription plan purchase
          await handleSubscriptionPurchase(userId, planId, type);
        } else if (type === "extra_credits") {
          // Handle extra credit plan purchase
          await handleExtraCreditPurchase(userId, credits);
        } else {
          console.error("Unknown purchase type:", type);
          return errorResponse(res, "Unknown purchase type", 400);
        }
      } catch (error) {
        console.error("Error processing purchase:", error);
        return errorResponse(
          res,
          error.message || "Error processing purchase",
          500
        );
      }
      break;

    case "payment_intent.succeeded":
      console.log("Payment succeeded:", event.data.object);
      break;

    default:
      console.log(`Unhandled event type: ${event.type}`);
  }

  // Return a response to acknowledge receipt of the event
  res.json({ received: true });
};
export { handleStripeWebhook };

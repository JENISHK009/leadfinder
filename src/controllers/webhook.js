import stripe from "../config/stripe.js";
import pool from "../config/db.js";
import { errorResponse } from "../utils/index.js";

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
      const { userId, planId } = session.metadata;

      try {
        // Fetch the plan details from the database
        const planQuery = await pool.query(
          "SELECT points FROM subscription_plans WHERE id = $1",
          [planId]
        );
        const plan = planQuery.rows[0];

        if (!plan) {
          console.error("Plan not found for ID:", planId);
          return errorResponse(res, "Plan not found", 404);
        }

        const planPoints = plan.points;

        // Update the user's credits in the database
        await pool.query(
          `UPDATE users 
           SET credits = credits + $1 
           WHERE id = $2`,
          [planPoints, userId]
        );

        console.log(
          `Added ${planPoints} points to user ${userId}. New credits: ${planPoints}`
        );

        // Insert the subscription into the user_subscriptions table
        await pool.query(
          `INSERT INTO user_subscriptions (user_id, plan_id, status, starts_at, ends_at)
           VALUES ($1, $2, $3, NOW(), NOW() + INTERVAL '1 month')`,
          [userId, planId, "active"]
        );

        console.log(`Subscription successfully activated for user ${userId}`);
      } catch (error) {
        console.error("Error updating user credits or subscription:", error);
        return errorResponse(res, "Error processing subscription", 500);
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
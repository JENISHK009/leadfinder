import pool from "../config/db.js";
import { successResponse, errorResponse } from "../utils/index.js";
import stripe from "../config/stripe.js";

// Helper function to validate plan data
const validatePlanData = (data) => {
  const { name, description, points, price, duration } = data;
  if (!name || !points || !price || !duration) {
    throw new Error("Missing required fields: name, points, price, duration");
  }
  if (typeof points !== "number" || points <= 0) {
    throw new Error("Points must be a positive number");
  }
  if (typeof price !== "number" || price <= 0) {
    throw new Error("Price must be a positive number");
  }
};

const createPlan = async (req, res) => {
  try {
    const { name, description, points, price, duration } = req.body;

    validatePlanData(req.body);

    const { rows } = await pool.query(
      `INSERT INTO subscription_plans (name, description, points, price, duration)
       VALUES ($1, $2, $3, $4, $5) RETURNING *`,
      [name, description, points, price, duration]
    );

    return successResponse(res, {
      message: "Plan created successfully",
      data: rows[0],
    });
  } catch (error) {
    console.error("Error creating plan:", error);
    return errorResponse(res, error.message || "Error creating plan", 400);
  }
};

// Get all subscription plans
const getAllPlans = async (req, res) => {
  try {
    const { rows } = await pool.query("SELECT * FROM subscription_plans");
    return successResponse(res, {
      message: "Plans fetched successfully",
      data: rows,
    });
  } catch (error) {
    console.error("Error fetching plans:", error);
    return errorResponse(res, "Error fetching plans", 500);
  }
};

// Get a specific subscription plan by ID
const getPlanById = async (req, res) => {
  try {
    const { id } = req.query; // Get ID from query parameters

    if (!id) {
      throw new Error("Plan ID is required");
    }

    const { rows } = await pool.query(
      "SELECT * FROM subscription_plans WHERE id = $1",
      [id]
    );

    if (rows.length === 0) {
      return errorResponse(res, "Plan not found", 404);
    }

    return successResponse(res, {
      message: "Plan fetched successfully",
      data: rows[0],
    });
  } catch (error) {
    console.error("Error fetching plan:", error);
    return errorResponse(res, error.message || "Error fetching plan", 400);
  }
};

// Update a subscription plan by ID (Admin only)
const updatePlan = async (req, res) => {
  try {
    const { id, name, description, points, price, duration } = req.body;

    if (!id) {
      throw new Error("Plan ID is required");
    }

    // Build the dynamic query based on provided fields
    let query = "UPDATE subscription_plans SET ";
    const values = [];
    let index = 1;

    if (name !== undefined) {
      query += `name = $${index}, `;
      values.push(name);
      index++;
    }
    if (description !== undefined) {
      query += `description = $${index}, `;
      values.push(description);
      index++;
    }
    if (points !== undefined) {
      query += `points = $${index}, `;
      values.push(points);
      index++;
    }
    if (price !== undefined) {
      query += `price = $${index}, `;
      values.push(price);
      index++;
    }
    if (duration !== undefined) {
      query += `duration = $${index}, `;
      values.push(duration);
      index++;
    }

    // Remove the trailing comma and space
    query = query.slice(0, -2);

    // Add the WHERE clause and RETURNING *
    query += ` WHERE id = $${index} RETURNING *`;
    values.push(id);

    // Execute the query
    const { rows } = await pool.query(query, values);

    if (rows.length === 0) {
      return errorResponse(res, "Plan not found", 404);
    }

    return successResponse(res, {
      message: "Plan updated successfully",
      data: rows[0],
    });
  } catch (error) {
    console.error("Error updating plan:", error);
    return errorResponse(res, error.message || "Error updating plan", 400);
  }
};

// Delete a subscription plan by ID (Admin only)
const deletePlan = async (req, res) => {
  try {
    const { id } = req.body; // Get ID from request body

    if (!id) {
      throw new Error("Plan ID is required");
    }

    // Delete the plan from the database
    const { rows } = await pool.query(
      "DELETE FROM subscription_plans WHERE id = $1 RETURNING *",
      [id]
    );

    if (rows.length === 0) {
      return errorResponse(res, "Plan not found", 404);
    }

    return successResponse(res, {
      message: "Plan deleted successfully",
      data: rows[0],
    });
  } catch (error) {
    console.error("Error deleting plan:", error);
    return errorResponse(res, error.message || "Error deleting plan", 400);
  }
};

const buySubscriptionPlan = async (req, res) => {
  try {
    const { planId } = req.body;
    const userId = req.currentUser.id;

    if (!planId || !userId) {
      throw new Error("Plan ID and User ID are required");
    }

    // Fetch the plan details from the database
    const planQuery = await pool.query(
      "SELECT * FROM subscription_plans WHERE id = $1",
      [planId]
    );
    const plan = planQuery.rows[0];

    if (!plan) {
      return errorResponse(res, "Plan not found", 404);
    }

    // Create a Stripe Checkout Session
    const session = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      line_items: [
        {
          price_data: {
            currency: "usd",
            product_data: {
              name: plan.name,
              description: plan.description,
            },
            unit_amount: plan.price * 100, // Stripe expects amount in cents
          },
          quantity: 1,
        },
      ],
      mode: "payment",
      success_url: `${process.env.FRONTEND_URL}/success?session_id={CHECKOUT_SESSION_ID}`,
      cancel_url: `${process.env.FRONTEND_URL}/cancel`,
      metadata: {
        userId,
        planId,
      },
    });

    return successResponse(res, {
      message: "Checkout session created successfully",
      sessionId: session.id,
      url: session.url, // Redirect the user to this URL for payment
    });
  } catch (error) {
    console.error("Error creating checkout session:", error);
    return errorResponse(
      res,
      error.message || "Error creating checkout session",
      400
    );
  }
};

export {
  createPlan,
  getAllPlans,
  getPlanById,
  updatePlan,
  deletePlan,
  buySubscriptionPlan,
};

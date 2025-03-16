import pool from "../config/db.js";
import { successResponse, errorResponse } from "../utils/index.js";
import stripe from "../config/stripe.js";

const validatePlanData = (data) => {
  const { name, description, points, price, duration, features } = data;
  if (!name || !points || !price || !duration) {
    throw new Error("Missing required fields: name, points, price, duration");
  }
  if (typeof points !== "number" || points <= 0) {
    throw new Error("Points must be a positive number");
  }
  if (typeof price !== "number" || price <= 0) {
    throw new Error("Price must be a positive number");
  }
  if (features && !Array.isArray(features)) {
    throw new Error("Features must be an array");
  }
};

const createPlan = async (req, res) => {
  try {
    const { name, description, points, price, duration, features } = req.body;

    validatePlanData(req.body);

    // Serialize the features array into a JSON string
    const featuresJson = JSON.stringify(features || []); // Default to an empty array if features is undefined

    const { rows } = await pool.query(
      `INSERT INTO subscription_plans (name, description, points, price, duration, features)
       VALUES ($1, $2, $3, $4, $5, $6) RETURNING *`,
      [name, description, points, price, duration, featuresJson] // Pass the serialized JSON string
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

const updatePlan = async (req, res) => {
  try {
    const { id, name, description, points, price, duration, features } =
      req.body;

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
    if (features !== undefined) {
      query += `features = $${index}, `;
      values.push(JSON.stringify(features)); // Serialize features array to JSON string
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
              metadata: {
                features: JSON.stringify(plan.features), // Include features in metadata
              },
            },
            unit_amount: plan.price * 100, // Stripe expects amount in cents
          },
          quantity: 1,
        },
      ],
      mode: "payment",
      success_url:
        "http://ec2-13-60-209-148.eu-north-1.compute.amazonaws.com/callback/success",
      cancel_url:
        "http://ec2-13-60-209-148.eu-north-1.compute.amazonaws.com/callback/fail",
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

const addCreditsToUser = async (req, res) => {
  try {
    const { userId, credits, comment } = req.body;

    // Check if the current user is an admin
    if (req.currentUser.roleName !== "admin") {
      return errorResponse(
        res,
        "Unauthorized: Only admins can add credits",
        403
      );
    }

    // Validate required fields
    if (!userId || !credits) {
      return errorResponse(res, "User ID and credits are required", 400);
    }

    // Check if the user exists
    const userQuery = await pool.query("SELECT * FROM users WHERE id = $1", [
      userId,
    ]);
    const user = userQuery.rows[0];

    if (!user) {
      return errorResponse(res, "User not found", 404);
    }

    // Update the user's credits by adding the provided credits
    const updatedCredits = user.credits + credits;
    await pool.query("UPDATE users SET credits = $1 WHERE id = $2", [
      updatedCredits,
      userId,
    ]);

    // Add a record to the user_subscriptions table with the credits and comment
    await pool.query(
      `INSERT INTO user_subscriptions (user_id, plan_id, status, starts_at, ends_at, comment)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        userId,
        null, // No plan_id since this is a manual credit addition
        "manual", // Status to indicate manual credit addition
        new Date(), // starts_at
        new Date(), // ends_at (same as starts_at for manual credits)
        comment || `Credits added manually by admin`, // Default comment if not provided
      ]
    );

    return successResponse(res, {
      message: "Credits added successfully",
      data: {
        userId,
        newCredits: updatedCredits,
        addedCredits: credits,
        comment: comment || "Credits added manually by admin",
      },
    });
  } catch (error) {
    console.error("Error adding credits:", error);
    return errorResponse(res, error.message || "Error adding credits", 500);
  }
};

export {
  createPlan,
  getAllPlans,
  getPlanById,
  updatePlan,
  deletePlan,
  buySubscriptionPlan,
  addCreditsToUser,
};

import pool from "../config/db.js";
import { successResponse, errorResponse } from "../utils/index.js";
import stripe from "../config/stripe.js";

const validatePlanData = (data) => {
  const {
    name,
    description,
    points,
    monthly_price,
    annual_price,
    duration,
    features,
  } = data;

  if (
    !name ||
    points === null ||
    points === undefined ||
    monthly_price === null ||
    monthly_price === undefined ||
    annual_price === null ||
    annual_price === undefined ||
    !duration
  ) {
    throw new Error(
      "Missing required fields: name, points, monthly_price, annual_price, duration"
    );
  }

  // Validate points
  if (typeof points !== "number" || points <= 0) {
    throw new Error("Points must be a positive number");
  }

  // Validate monthly_price
  if (typeof monthly_price !== "number") {
    throw new Error("Monthly price must be a positive number");
  }

  // Validate annual_price
  if (typeof annual_price !== "number") {
    throw new Error("Annual price must be a positive number");
  }

  // Validate features
  if (features && !Array.isArray(features)) {
    throw new Error("Features must be an array");
  }
};

const createPlan = async (req, res) => {
  try {
    const {
      name,
      description,
      points,
      monthly_price,
      annual_price,
      duration,
      features,
    } = req.body;

    // Validate the plan data
    validatePlanData(req.body);

    // Serialize the features array into a JSON string
    const featuresJson = JSON.stringify(features || []);

    // Insert the new plan into the database
    const { rows } = await pool.query(
      `INSERT INTO subscription_plans (name, description, points, monthly_price, annual_price, duration, features)
       VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
      [
        name,
        description,
        points,
        monthly_price,
        annual_price,
        duration,
        featuresJson,
      ]
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
    const {
      id,
      name,
      description,
      points,
      monthly_price,
      annual_price,
      duration,
      features,
    } = req.body;

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
    if (monthly_price !== undefined) {
      query += `monthly_price = $${index}, `;
      values.push(monthly_price);
      index++;
    }
    if (annual_price !== undefined) {
      query += `annual_price = $${index}, `;
      values.push(annual_price);
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
    const { planId, type } = req.body; // `type` can be "monthly", "yearly", or "extra_credits"
    const userId = req.currentUser.id;

    // Validate required fields
    if (!planId || !userId || !type) {
      throw new Error("Plan ID, User ID, and Type are required");
    }

    // Validate the type
    if (type !== "monthly" && type !== "yearly" && type !== "extra_credits") {
      throw new Error(
        "Type must be either 'monthly', 'yearly', or 'extra_credits'"
      );
    }

    // Fetch the plan details from the database
    let plan;
    if (type === "extra_credits") {
      const extraCreditPlanQuery = await pool.query(
        "SELECT * FROM extra_credit_plans WHERE id = $1",
        [planId]
      );
      plan = extraCreditPlanQuery.rows[0];
    } else {
      const subscriptionPlanQuery = await pool.query(
        "SELECT * FROM subscription_plans WHERE id = $1",
        [planId]
      );
      plan = subscriptionPlanQuery.rows[0];
    }

    if (!plan) {
      return errorResponse(res, "Plan not found", 404);
    }

    // Determine the price and description based on the selected type
    let price, description;
    if (type === "monthly") {
      price = plan.monthly_price;
      description = "Monthly Subscription";
    } else if (type === "yearly") {
      price = plan.annual_price;
      description = "Annual Subscription";
    } else if (type === "extra_credits") {
      price = plan.price;
      description = "Extra Credits Purchase";
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
              description: description, // Include the description based on the type
              metadata: {
                features: JSON.stringify(plan.features), // Include features in metadata
              },
            },
            unit_amount: price * 100, // Stripe expects amount in cents
          },
          quantity: 1,
        },
      ],
      mode: "payment",
      success_url:
        `${process.env.FRONTEND_URL}/callback/success`,
      cancel_url:
        `${process.env.FRONTEND_URL}/callback/fail`,
      metadata: {
        userId,
        planId,
        type, // Include the type in metadata
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
    if (!userId || credits === undefined || credits === null) {
      return errorResponse(res, "User ID and credits are required", 400);
    }

    // Convert credits to a number
    const creditsNumber = Number(credits);

    // Check if credits is a valid number
    if (isNaN(creditsNumber)) {
      return errorResponse(res, "Credits must be a valid number", 400);
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
    const updatedCredits = user.credits + creditsNumber;
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
        addedCredits: creditsNumber,
        comment: comment || "Credits added manually by admin",
      },
    });
  } catch (error) {
    console.error("Error adding credits:", error);
    return errorResponse(res, error.message || "Error adding credits", 500);
  }
};

const createExtraCreditPlan = async (req, res) => {
  try {
    const { name, description, price, credits } = req.body;

    if (!name || !price || !credits) {
      throw new Error("Missing required fields: name, price, credits");
    }

    if (typeof price !== "number" || price <= 0) {
      throw new Error("Price must be a positive number");
    }
    if (typeof credits !== "number" || credits <= 0) {
      throw new Error("Credits must be a positive number");
    }

    const { rows } = await pool.query(
      `INSERT INTO extra_credit_plans (name, description, price, credits)
       VALUES ($1, $2, $3, $4) RETURNING *`,
      [name, description, price, credits]
    );

    return successResponse(res, {
      message: "Extra credit plan created successfully",
      data: rows[0],
    });
  } catch (error) {
    console.error("Error creating extra credit plan:", error);
    return errorResponse(
      res,
      error.message || "Error creating extra credit plan",
      400
    );
  }
};

const updateExtraCreditPlan = async (req, res) => {
  try {
    const { id, name, description, price, credits } = req.body;

    // Validate required fields
    if (!id) {
      throw new Error("Plan ID is required");
    }

    // Build the dynamic query based on provided fields
    let query = "UPDATE extra_credit_plans SET ";
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
    if (price !== undefined) {
      query += `price = $${index}, `;
      values.push(price);
      index++;
    }
    if (credits !== undefined) {
      query += `credits = $${index}, `;
      values.push(credits);
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
      return errorResponse(res, "Extra credit plan not found", 404);
    }

    return successResponse(res, {
      message: "Extra credit plan updated successfully",
      data: rows[0],
    });
  } catch (error) {
    console.error("Error updating extra credit plan:", error);
    return errorResponse(
      res,
      error.message || "Error updating extra credit plan",
      400
    );
  }
};

const getAllExtraCreditPlans = async (req, res) => {
  try {
    const { rows } = await pool.query("SELECT * FROM extra_credit_plans");
    return successResponse(res, {
      message: "Extra credit plans fetched successfully",
      data: rows,
    });
  } catch (error) {
    console.error("Error fetching extra credit plans:", error);
    return errorResponse(res, "Error fetching extra credit plans", 500);
  }
};

const purchaseExtraCreditPlan = async (req, res) => {
  try {
    const { amount, credits } = req.body;
    const userId = req.currentUser.id;

    // Validate required fields
    if (!amount || !credits || !userId) {
      throw new Error("Amount, Credits, and User ID are required");
    }

    // Create a Stripe Checkout Session
    const session = await stripe.checkout.sessions.create({
      payment_method_types: ["card"],
      line_items: [
        {
          price_data: {
            currency: "usd",
            product_data: {
              name: `Extra Credits Purchase - ${credits} Credits`,
              description: `Purchase of ${credits} extra credits`,
            },
            unit_amount: amount * 100, // Stripe expects amount in cents
          },
          quantity: 1,
        },
      ],
      mode: "payment",
      success_url:
        `${process.env.FRONTEND_URL}/callback/success`,
      cancel_url:
        `${process.env.FRONTEND_URL}/callback/fail`,
      metadata: {
        userId,
        credits,
        type: "extra_credits", // Indicate that this is an extra credit purchase
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
  addCreditsToUser,
  createExtraCreditPlan,
  updateExtraCreditPlan,
  getAllExtraCreditPlans,
  purchaseExtraCreditPlan,
};

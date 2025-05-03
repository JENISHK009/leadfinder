import pool from "../config/db.js";
import bcrypt from "bcrypt";
import { successResponse, errorResponse } from "../utils/index.js";

export async function getUsersWithActivePlans(req, res) {
  const client = await pool.connect();

  try {
    // Pagination parameters
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;

    // Search filters
    const searchTerm = req.query.search ? req.query.search.trim() : "";

    console.log(
      `Fetching users with active plans (page: ${page}, limit: ${limit})`
    );

    // Build WHERE clause for search
    let whereClause = "";
    let params = [limit, offset];

    if (searchTerm) {
      whereClause = `
                WHERE u.name ILIKE $3 OR 
                      u.email ILIKE $3 OR 
                      u.mobile_number ILIKE $3
            `;
      params.push(`%${searchTerm}%`);
    }

    // Main query with pagination
    const query = `
            SELECT 
    u.id AS user_id,
    u.name,
    u.email,
    u.mobile_number,
    u.credits,
    u.created_at AS user_created_at,
    COALESCE(
        json_agg(
            json_build_object(
                'subscription_id', us.id,
                'plan_id', sp.id,
                'plan_name', sp.name,
                'plan_description', sp.description,
                'points', sp.points,
                'duration', sp.duration,
                'status', us.status,
                'starts_at', us.starts_at,
                'ends_at', us.ends_at
            ) 
        ) FILTER (WHERE us.id IS NOT NULL AND us.status = 'active' AND us.ends_at > CURRENT_TIMESTAMP),
        '[]'
    ) AS active_plans
FROM 
    public.users u
LEFT JOIN 
    user_subscriptions us 
    ON u.id = us.user_id
LEFT JOIN 
    subscription_plans sp 
    ON us.plan_id = sp.id
${whereClause}
GROUP BY 
    u.id
ORDER BY 
    u.created_at DESC
LIMIT $1 OFFSET $2
        `;

    // Count query for pagination
    const countQuery = `
            SELECT COUNT(DISTINCT u.id) AS total
            FROM public.users u
            ${whereClause}
        `;

    const result = await client.query(query, params);
    const countResult = await client.query(
      countQuery,
      searchTerm ? [`%${searchTerm}%`] : []
    );

    const total = parseInt(countResult.rows[0].total);
    const totalPages = Math.ceil(total / limit);

    console.log(`Retrieved ${result.rows.length} users (total: ${total})`);

    return successResponse(res, {
      users: result.rows,
      pagination: {
        total,
        page,
        limit,
        total_pages: totalPages,
        has_next_page: page < totalPages,
        has_prev_page: page > 1,
      },
    });
  } catch (error) {
    console.error("Error fetching users with active plans:", error);
    return errorResponse(res, "Error fetching users with active plans", 500);
  } finally {
    client.release();
  }
}

export async function getUserSubscriptionHistory(req, res) {
  const userId = req.currentUser.id;

  if (!userId) {
    return errorResponse(res, "User ID is required");
  }

  const client = await pool.connect();

  try {
    console.log(`Fetching subscription history for user ID: ${userId}`);

    // Verify the user exists
    const userQuery = "SELECT id, name, email FROM public.users WHERE id = $1";
    const userResult = await client.query(userQuery, [userId]);

    if (userResult.rows.length === 0) {
      return errorResponse(res, "User not found", 404);
    }

    // Get all subscription history for the user, ordered by created_at desc (latest first)
    const query = `
            SELECT 
                us.id AS subscription_id,
                us.status,
                us.starts_at,
                us.ends_at,
                us.created_at AS purchase_date,
                sp.id AS plan_id,
                sp.name AS plan_name,
                sp.description AS plan_description,
                sp.points,
                sp.price,
                sp.duration
            FROM 
                user_subscriptions us
            JOIN 
                subscription_plans sp ON us.plan_id = sp.id
            WHERE 
                us.user_id = $1
            ORDER BY 
                us.created_at DESC
        `;

    const result = await client.query(query, [userId]);
    console.log(
      `Retrieved ${result.rows.length} subscription records for user ID: ${userId}`
    );

    // Format the response with user info and subscription history
    const responseData = {
      user: {
        id: userResult.rows[0].id,
        name: userResult.rows[0].name,
        email: userResult.rows[0].email,
      },
      subscription_history: result.rows,
    };

    return successResponse(res, responseData);
  } catch (error) {
    console.error(
      `Error fetching subscription history for user ID ${userId}:`,
      error
    );
    return errorResponse(res, "Error fetching subscription history", 500);
  } finally {
    client.release();
  }
}

export async function getUserPoints(req, res) {
  const userId = req.currentUser.id;

  if (!userId) {
    return errorResponse(res, "User ID is required", 400);
  }

  const client = await pool.connect();

  try {
    console.log(`Fetching points and active plan for user ID: ${userId}`);

    // Query to fetch the user's credits (points)
    const pointsQuery = `
            SELECT 
                credits AS points
            FROM 
                public.users
            WHERE 
                id = $1
        `;

    const pointsResult = await client.query(pointsQuery, [userId]);

    if (pointsResult.rows.length === 0) {
      return errorResponse(res, "User not found", 404);
    }

    const userPoints = pointsResult.rows[0];

    // Query to fetch the user's active subscription plan
    const activePlanQuery = `
            SELECT 
                us.plan_id,
                sp.name AS plan_name,
                sp.points AS plan_points,
                us.starts_at,
                us.ends_at
            FROM 
                user_subscriptions us
            LEFT JOIN 
                subscription_plans sp ON us.plan_id = sp.id
            WHERE 
                us.user_id = $1
                AND us.status = 'active'
                AND NOW() BETWEEN us.starts_at AND us.ends_at
            ORDER BY 
                us.starts_at DESC
            LIMIT 1
        `;

    const activePlanResult = await client.query(activePlanQuery, [userId]);

    let activePlan = null;
    let freeUser = true; // Default to true (no active plan)

    if (activePlanResult.rows.length > 0) {
      activePlan = {
        plan_id: activePlanResult.rows[0].plan_id,
        plan_name: activePlanResult.rows[0].plan_name,
        plan_points: activePlanResult.rows[0].plan_points,
        starts_at: activePlanResult.rows[0].starts_at,
        ends_at: activePlanResult.rows[0].ends_at,
      };
      freeUser = false; // User has an active plan, so not a free user
    }

    console.log(`Retrieved points and active plan for user ID: ${userId}`);

    return successResponse(res, {
      points: userPoints.points,
      active_plan: activePlan, // Include active plan details
      freeUser: freeUser      // Include freeUser flag
    });
  } catch (error) {
    console.error(
      `Error fetching points and active plan for user ID ${userId}:`,
      error
    );
    return errorResponse(
      res,
      "Error fetching user points and active plan",
      500
    );
  } finally {
    client.release();
  }
}

export async function updatePassword(req, res) {
  const { newPassword } = req.body;

  // Validate required field
  if (!newPassword) {
    return errorResponse(res, "New password is required");
  }

  // Get the current user's ID from the JWT token
  const userId = req.currentUser.id;

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Fetch the current user's password from the database
    const user = await client.query(
      "SELECT password FROM public.users WHERE id = $1 FOR UPDATE",
      [userId]
    );

    if (user.rows.length === 0) {
      await client.query("ROLLBACK");
      return errorResponse(res, "User not found", 404);
    }

    const currentHashedPassword = user.rows[0].password;

    // Check if the new password is the same as the current password
    const isSamePassword = await bcrypt.compare(
      newPassword,
      currentHashedPassword
    );
    if (isSamePassword) {
      await client.query("ROLLBACK");
      return errorResponse(
        res,
        "New password cannot be the same as the current password"
      );
    }

    // Hash the new password
    const newHashedPassword = await bcrypt.hash(newPassword, 10);

    // Update the user's password in the database
    await client.query("UPDATE public.users SET password = $1 WHERE id = $2", [
      newHashedPassword,
      userId,
    ]);

    await client.query("COMMIT");
    return successResponse(res, { message: "Password updated successfully" });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error updating password:", error);
    return errorResponse(res, "Error updating password", 500);
  } finally {
    client.release();
  }
}

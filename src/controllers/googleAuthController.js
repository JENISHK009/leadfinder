import pool from "../config/db.js";
import { generateToken } from "../utils/jwtUtils.js";

const USER_ROLE_NAME = "user";

export const googleAuthCallback = async (req, accessToken, refreshToken, profile, done) => {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Check if user exists with this googleId
    const existingUser = await client.query(
      `SELECT u.id, u.email, u.role_id, u.credits, r.name as role_name 
       FROM public.users u 
       JOIN roles r ON u.role_id = r.id 
       WHERE u.google_id = $1 LIMIT 1`,
      [profile.id]
    );

    let userDetails;

    if (existingUser.rows.length > 0) {
      userDetails = {
        id: existingUser.rows[0].id,
        email: existingUser.rows[0].email,
        role_id: existingUser.rows[0].role_id,
        credits: existingUser.rows[0].credits,
        role: existingUser.rows[0].role_name,
        googleId: profile.id,
      };
    } else {
      // Validate required fields
      if (!profile.emails?.[0]?.value) {
        await client.query("ROLLBACK");
        return done(new Error("Email is required for registration"));
      }

      // Get user role
      const roleResult = await client.query(
        "SELECT id FROM roles WHERE name = $1 LIMIT 1",
        [USER_ROLE_NAME]
      );
      const roleId = roleResult.rows[0]?.id;
      if (!roleId) {
        await client.query("ROLLBACK");
        return done(new Error("Role not found"));
      }

      // Create new user
      const newUser = await client.query(
        `INSERT INTO public.users 
         (name, email, google_id, role_id, credits, otp_verified, mobile_number, password) 
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
         RETURNING id, email, role_id, credits`,
        [
          profile.displayName,
          profile.emails[0].value,
          profile.id,
          roleId,
          process.env.FREE_CREDIT, 
          true, // OTP verified for Google signups
          null, // Mobile number
          null, // Password
        ]
      );

      userDetails = {
        id: newUser.rows[0].id,
        email: newUser.rows[0].email,
        role_id: newUser.rows[0].role_id,
        credits: newUser.rows[0].credits,
        role: USER_ROLE_NAME,
        googleId: profile.id,
      };
    }

    // Generate JWT token
    const token = generateToken(userDetails);
    
    const user = {
      ...userDetails,
      displayName: profile.displayName,
      firstName: profile.name?.givenName,
      lastName: profile.name?.familyName,
      picture: profile.photos?.[0]?.value,
      accessToken,
      token
    };

    await client.query("COMMIT");
    return done(null, user);
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Google auth error:", error);
    return done(error);
  } finally {
    client.release();
  }
};
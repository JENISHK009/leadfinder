import bcrypt from "bcrypt";
import pool from "../config/db.js";
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import {
  sendOtpEmail,
  errorResponse,
  generateToken,
  successResponse,
  generateOTP,
  isOtpExpired
} from "../utils/index.js";
import { USER_ROLES,OTP_CONFIG } from "../enum/index.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export async function signup(req, res) {
  const { name, email, password, mobileNumber } = req.body;
  if (!name || !email || !password) {
    return errorResponse(res, "Name, email and password are required");
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const existingUserQuery = "SELECT id FROM public.users WHERE email = $1 LIMIT 1";
    const existingUser = await client.query(existingUserQuery, [email]);

    if (existingUser.rows.length > 0) {
      await client.query("ROLLBACK");
      return errorResponse(res, "Email already in use");
    }

    const roleResult = await client.query(
      "SELECT id FROM roles WHERE name = $1 LIMIT 1",
      [USER_ROLES.USER]
    );

    if (!roleResult.rows[0]?.id) {
      await client.query("ROLLBACK");
      return errorResponse(res, "Role not found", 500);
    }

    const hashedPassword = await bcrypt.hash(password, 10);
    const otp = generateOTP();
    const currentTime = new Date();

    const newUser = await client.query(
      `INSERT INTO public.users 
      (name, email, mobile_number, password, role_id, credits, otp, otp_timestamp) 
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8) 
      RETURNING id, email, role_id, credits`,
      [name, email, mobileNumber || null, hashedPassword, roleResult.rows[0].id, process.env.FREE_CREDIT, otp, currentTime]
    );

    const token = generateToken(newUser.rows[0]);
    await client.query("COMMIT");

    const templatePath = path.join(__dirname, '../templates/exellius_otp_email_template.html');
    let htmlTemplate = fs.readFileSync(templatePath, 'utf8');

    htmlTemplate = htmlTemplate
      .replace(/\[Client Name\]/g, name)
      .replace(/{{OTP_CODE}}/g, otp);

    const emailOptions = {
      subject: `Your Secure Verification Code for Exellius`,
      text: `Hi ${name},\n\nYour verification code is: ${otp}\n\nThis code will expire in 10 minutes.`,
      html: htmlTemplate
    };

    sendOtpEmail(email, emailOptions).catch(error =>
      console.error("Failed to send OTP email:", error)
    );

    return successResponse(res, {
      message: `Signup successful, OTP sent (valid for 10 minutes)`,
      token,
      user: newUser.rows[0],
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error(error);
    return errorResponse(res, "Error during signup", 500);
  } finally {
    client.release();
  }
}

export async function login(req, res) {
  const { email, password } = req.body;
  if (!email || !password) {
    return errorResponse(res, "Email and password are required");
  }

  try {
    // Join the users table with the roles table to get the role name
    const user = await pool.query(
      `
        SELECT u.id, u.email, u.password, u.google_id, u.role_id, u.credits, r.name as role_name 
        FROM public.users u
        JOIN public.roles r ON u.role_id = r.id
        WHERE u.email = $1
    `,
      [email]
    );

    if (user.rows.length === 0) {
      return errorResponse(res, "Invalid credentials");
    }

    if (user.rows[0].google_id) {
      return errorResponse(res, "Please login with Google");
    }

    if (!(await bcrypt.compare(password, user.rows[0].password))) {
      return errorResponse(res, "Invalid credentials");
    }

    // Extract user details including the role
    const userDetails = {
      id: user.rows[0].id,
      email: user.rows[0].email,
      role_id: user.rows[0].role_id,
      credits: user.rows[0].credits,
      role: user.rows[0].role_name, // Include the role name
    };

    const token = generateToken(userDetails);
    return successResponse(res, {
      message: "Login successful",
      token,
      user: userDetails,
    });
  } catch (error) {
    console.error(error);
    return errorResponse(res, "Error during login", 500);
  }
}

export async function verifyOtp(req, res) {
  const { email, otp } = req.body;
  if (!email || !otp) {
    return errorResponse(res, "Email and OTP are required");
  }

  try {
    const userQuery = await pool.query(
      "SELECT otp, otp_timestamp FROM public.users WHERE email = $1",
      [email]
    );

    if (userQuery.rows.length === 0) {
      return errorResponse(res, "User not found");
    }

    const userData = userQuery.rows[0];

    if (userData.otp !== otp) {
      return errorResponse(res, "Invalid OTP");
    }

    if (isOtpExpired(userData.otp_timestamp)) {
      return errorResponse(res, `OTP has expired. OTPs are valid for ${OTP_CONFIG.EXPIRY_MINUTES} minutes only.`);
    }

    await pool.query(
      "UPDATE public.users SET otp_verified = true, otp = NULL, otp_timestamp = NULL WHERE email = $1",
      [email]
    );

    return successResponse(res, { message: "OTP verified successfully" });
  } catch (error) {
    console.error(error);
    return errorResponse(res, "Error during OTP verification", 500);
  }
}

export async function forgotPassword(req, res) {
  const { email } = req.body;
  if (!email) {
    return errorResponse(res, "Email is required");
  }

  try {
    const user = await pool.query(
      'SELECT id, name, google_id FROM users WHERE email = $1',
      [email]
    );

    if (user.rows.length === 0) {
      return errorResponse(res, "User not found");
    }

    if (user.rows[0]?.google_id) {
      return errorResponse(res, 'Google-authenticated users must use Google login');
    }

    const otp = generateOTP();
    const currentTime = new Date();

    await pool.query(
      "UPDATE public.users SET otp = $1, otp_timestamp = $2 WHERE email = $3",
      [otp, currentTime, email]
    );

    const templatePath = path.join(__dirname, '../templates/exellius_otp_email_template.html');
    let htmlTemplate = fs.readFileSync(templatePath, 'utf8');

    htmlTemplate = htmlTemplate
      .replace(/\[Client Name\]/g, user.rows[0]?.name)
      .replace(/{{OTP_CODE}}/g, otp);

    const emailOptions = {
      subject: `Your Secure Verification Code for Exellius`,
      html: htmlTemplate
    };

    await sendOtpEmail(email, emailOptions);

    return successResponse(res, {
      message: `OTP sent to registered email (valid for ${OTP_CONFIG.EXPIRY_MINUTES} minutes)`
    });
  } catch (error) {
    console.error(error);
    return errorResponse(res, "Error sending OTP", 500);
  }
}

export async function resetPassword(req, res) {
  const { email, otp, newPassword } = req.body;
  if (!email || !otp || !newPassword) {
    return errorResponse(res, "Email, OTP, and new password are required");
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const user = await client.query(
      "SELECT id, password, otp, otp_timestamp FROM public.users WHERE email = $1 FOR UPDATE",
      [email]
    );

    if (user.rows.length === 0) {
      await client.query("ROLLBACK");
      return errorResponse(res, "User not found");
    }

    if (user.rows[0].otp !== otp) {
      await client.query("ROLLBACK");
      return errorResponse(res, "Invalid OTP");
    }

    if (isOtpExpired(user.rows[0].otp_timestamp)) {
      await client.query("ROLLBACK");
      return errorResponse(res, `OTP has expired. OTPs are valid for ${OTP_CONFIG.EXPIRY_MINUTES} minutes only.`);
    }

    const isSamePassword = await bcrypt.compare(
      newPassword,
      user.rows[0].password
    );

    if (isSamePassword) {
      await client.query("ROLLBACK");
      return errorResponse(
        res,
        "New password cannot be the same as the old password"
      );
    }

    const hashedPassword = await bcrypt.hash(newPassword, 10);
    await client.query(
      "UPDATE public.users SET password = $1, otp = NULL, otp_timestamp = NULL WHERE email = $2",
      [hashedPassword, email]
    );

    await client.query("COMMIT");
    return successResponse(res, { message: "Password reset successfully" });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error(error);
    return errorResponse(res, "Error resetting password", 500);
  } finally {
    client.release();
  }
}

export default {
  signup,
  login,
  verifyOtp,
  forgotPassword,
  resetPassword,
};
import bcrypt from 'bcrypt';
import pool from '../config/db.js';
import { sendOtpEmail } from '../utils/emailUtils.js';
import { generateToken } from '../utils/jwtUtils.js';
import { successResponse, errorResponse } from '../utils/responseUtil.js';

const OTP = '123456';

export async function signup(req, res) {
    const { name, email, mobileNumber, password } = req.body;
    if (!name || !email || !mobileNumber || !password) {
        return errorResponse(res, 'All fields are required');
    }
    try {
        const existingUser = await pool.query(
            'SELECT * FROM public.users WHERE email = $1 OR mobile_number = $2',
            [email, mobileNumber]
        );
        if (existingUser.rows.length > 0) {
            return errorResponse(res, 'Email or Mobile Number already in use');
        }
        const hashedPassword = await bcrypt.hash(password, 10);
        const roleResult = await pool.query("SELECT id FROM roles WHERE name = 'user' LIMIT 1");
        const roleId = roleResult.rows[0]?.id;
        const newUser = await pool.query(
            'INSERT INTO public.users (name, email, mobile_number, password, role_id) VALUES ($1, $2, $3, $4, $5) RETURNING id, email, role_id',
            [name, email, mobileNumber, hashedPassword, roleId]
        );
        await sendOtpEmail(email, OTP);
        const token = generateToken(newUser.rows[0]);
        return successResponse(res, { message: 'Signup successful, OTP sent', token, user: newUser.rows[0] });
    } catch (error) {
        console.error(error);
        return errorResponse(res, 'Error during signup', 500);
    }
}

export async function login(req, res) {
    const { email, password } = req.body;
    if (!email || !password) {
        return errorResponse(res, 'Email and password are required');
    }
    try {
        const user = await pool.query('SELECT * FROM public.users WHERE email = $1', [email]);
        if (user.rows.length === 0 || !(await bcrypt.compare(password, user.rows[0].password))) {
            return errorResponse(res, 'Invalid credentials');
        }
        const token = generateToken(user.rows[0]);
        return successResponse(res, { message: 'Login successful', token, user: user.rows[0] });
    } catch (error) {
        console.error(error);
        return errorResponse(res, 'Error during login', 500);
    }
}

export async function verifyOtp(req, res) {
    const { email, otp } = req.body;
    if (!email || !otp) {
        return errorResponse(res, 'Email and OTP are required');
    }
    if (otp !== OTP) {
        return errorResponse(res, 'Invalid OTP');
    }
    try {
        await pool.query('UPDATE public.users SET otp_verified = true WHERE email = $1', [email]);
        return successResponse(res, { message: 'OTP verified successfully' });
    } catch (error) {
        console.error(error);
        return errorResponse(res, 'Error during OTP verification', 500);
    }
}

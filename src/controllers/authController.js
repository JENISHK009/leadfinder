import bcrypt from 'bcrypt';
import pool from '../config/db.js';
import { sendOtpEmail } from '../utils/emailUtils.js';

// Static OTP for now
const OTP = '123456'; 

// Signup API
export async function signup(req, res) {
    const { name, email, mobileNumber, password } = req.body;

    if (!name || !email || !mobileNumber || !password) {
        return res.status(400).json({ message: 'All fields are required' });
    }

    try {
        const existingUserQuery = `
            SELECT * FROM public.users WHERE email = $1 OR mobile_number = $2;
        `;
        const existingUser = await pool.query(existingUserQuery, [email, mobileNumber]);

        if (existingUser.rows.length > 0) {
            return res.status(400).json({ message: 'Email or Mobile Number already in use' });
        }

        const hashedPassword = await bcrypt.hash(password, 10);

        const insertUserQuery = `
            INSERT INTO public.users (name, email, mobile_number, password) 
            VALUES ($1, $2, $3, $4) RETURNING id, email;
        `;
        const newUser = await pool.query(insertUserQuery, [name, email, mobileNumber, hashedPassword]);

        await sendOtpEmail(email, OTP);

        res.status(200).json({
            message: 'Signup successful, OTP sent to email',
            userId: newUser.rows[0].id,
        });
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: 'Error during signup', error });
    }
}

// Login API
export async function login(req, res) {
    const { email, password } = req.body;

    if (!email || !password) {
        return res.status(400).json({ message: 'Email and password are required' });
    }

    try {
        const getUserQuery = `
            SELECT * FROM public.users WHERE email = $1;
        `;
        const user = await pool.query(getUserQuery, [email]);

        if (user.rows.length === 0) {
            return res.status(400).json({ message: 'Invalid credentials' });
        }

        const validPassword = await bcrypt.compare(password, user.rows[0].password);

        if (!validPassword) {
            return res.status(400).json({ message: 'Invalid credentials' });
        }

        res.status(200).json({
            message: 'Login successful',
            userId: user.rows[0].id,
            name: user.rows[0].name,
        });
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: 'Error during login', error });
    }
}

// Verify OTP API
export async function verifyOtp(req, res) {
    const { email, otp } = req.body;

    if (!email || !otp) {
        return res.status(400).json({ message: 'Email and OTP are required' });
    }

    if (otp !== OTP) {
        return res.status(400).json({ message: 'Invalid OTP' });
    }

    try {
        const verifyOtpQuery = `
            UPDATE public.users SET otp_verified = true WHERE email = $1;
        `;
        await pool.query(verifyOtpQuery, [email]);

        res.status(200).json({ message: 'OTP verified successfully' });
    } catch (error) {
        console.error(error);
        res.status(500).json({ message: 'Error during OTP verification', error });
    }
}

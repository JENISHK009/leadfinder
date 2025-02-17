import jwt from 'jsonwebtoken';
import pool from '../config/db.js'; 
import { errorResponse } from '../utils/index.js'; 

const authenticateUser = async (req, res, next) => {
    try {
        const authHeader = req.headers['authorization'];
        const token = authHeader && authHeader.split(' ')[1];

        if (!token) {
            return errorResponse(res, 'Access denied.', 401);
        }

        const decoded = jwt.verify(token, process.env.JWT_SECRET);

        const userQuery = 'SELECT * FROM users WHERE id = $1';
        const { rows } = await pool.query(userQuery, [decoded.id]);

        if (rows.length === 0) {
            return errorResponse(res, 'User not found.', 404);
        }

        const user = rows[0];

        req.currentUser = {
            id: user.id,
            email: user.email,
            role: user.role,
        };

        next();
    } catch (error) {
        console.error('Error in authenticateUser middleware:', error);

        if (error.name === 'JsonWebTokenError') {
            return errorResponse(res, 'Invalid token.', 401);
        } else if (error.name === 'TokenExpiredError') {
            return errorResponse(res, 'Token expired.', 401);
        } else {
            return errorResponse(res, 'Authentication error.', 500);
        }
    }
};

export default authenticateUser;

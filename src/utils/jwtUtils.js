import jwt from 'jsonwebtoken';

const JWT_SECRET = process.env.JWT_SECRET || 'your_secret_key';

export const generateToken = (user) => {
    return jwt.sign(
        { id: user.id, email: user.email, roleId: user.role_id },
        JWT_SECRET,
        { expiresIn: '7d' }
    );
};
import jwt from 'jsonwebtoken';

const JWT_SECRET = process.env.JWT_SECRET?.trim() || 'your_secret_key';

export const generateToken = (user) => {
    return jwt.sign(
        { id: user.id, email: user.email, roleId: user.role_id },
        JWT_SECRET,
        { expiresIn: '7d' }
    );
};

// Generate a permanent API token that never expires
export const generatePermanentApiToken = (apiKeyName = 'system-api') => {
    return jwt.sign(
        { 
            type: 'api_key',
            name: apiKeyName,
            permanent: true,
            iat: Math.floor(Date.now() / 1000) // issued at time
        },
        JWT_SECRET
        // No expiresIn means it never expires
    );
};
import dotenv from 'dotenv';
import jwt from 'jsonwebtoken';

// Load environment variables FIRST before importing other modules
dotenv.config();

// Get JWT_SECRET directly here to ensure it's loaded
const JWT_SECRET = process.env.JWT_SECRET?.trim() || 'your_secret_key';

// Generate a permanent API token that never expires
const generatePermanentApiToken = (apiKeyName = 'system-api') => {
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

// Generate a permanent API token
const apiKeyName = process.argv[2] || 'default-api-key';
const permanentToken = generatePermanentApiToken(apiKeyName);

console.log('='.repeat(60));
console.log('PERMANENT API TOKEN GENERATED');
console.log('='.repeat(60));
console.log(`API Key Name: ${apiKeyName}`);
console.log(`Token: ${permanentToken}`);
console.log('='.repeat(60));
console.log('This token will NEVER expire and can be used for API access.');
console.log('Keep it secure and use it in your Authorization header as:');
console.log(`Authorization: Bearer ${permanentToken}`);
console.log('='.repeat(60)); 
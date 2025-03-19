CREATE TABLE IF NOT EXISTS subscription_plans (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    points INTEGER NOT NULL,
    monthly_price DECIMAL(10, 2) NOT NULL, -- New column for monthly price
    annual_price DECIMAL(10, 2) NOT NULL,  -- New column for annual price
    duration VARCHAR(50) NOT NULL,
    features JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
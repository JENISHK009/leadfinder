CREATE TABLE user_subscriptions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id),
    plan_id INTEGER REFERENCES subscription_plans(id),
    status VARCHAR(50) NOT NULL, -- e.g., "active", "canceled"
    starts_at TIMESTAMP NOT NULL,
    ends_at TIMESTAMP NOT NULL,
    comment TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
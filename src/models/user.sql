-- Create 'users' table with cleaned schema
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    mobile_number TEXT,
    password TEXT,
    credits INT DEFAULT 0,
    role_id INT NOT NULL DEFAULT 2,  -- Default role_id for 'user'
    otp TEXT,                        -- OTP column
    otp_verified BOOLEAN DEFAULT FALSE,
    google_id TEXT,                 -- Google ID directly in schema
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_role FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE RESTRICT
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_users_email ON public.users(email);
CREATE INDEX IF NOT EXISTS idx_users_mobile_number ON public.users(mobile_number);

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS update_user_timestamp ON users;

-- Create a trigger function to update 'updated_at'
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Attach the trigger to the 'users' table
CREATE TRIGGER update_user_timestamp
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

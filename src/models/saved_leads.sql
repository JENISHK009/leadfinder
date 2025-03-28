CREATE TABLE IF NOT EXISTS saved_leads (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL, -- ID of the user who saved the lead
    lead_id INT NOT NULL, -- ID of the saved lead (references leads table)
    type VARCHAR(50) NOT NULL, -- Type of saved lead (e.g., 'leads' or 'company')
    email BOOLEAN DEFAULT FALSE, -- Whether email is saved
    mobile BOOLEAN DEFAULT FALSE, -- Whether mobile is saved
    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the lead was saved
    CONSTRAINT unique_user_lead_type UNIQUE (user_id, lead_id, type) -- Unique constraint
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_saved_leads_user_id ON saved_leads (user_id);
CREATE INDEX IF NOT EXISTS idx_saved_leads_lead_id ON saved_leads (lead_id);
CREATE INDEX IF NOT EXISTS idx_saved_leads_type ON saved_leads (type);
CREATE INDEX IF NOT EXISTS idx_saved_leads_saved_at ON saved_leads (saved_at);
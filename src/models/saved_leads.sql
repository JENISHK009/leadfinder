CREATE TABLE IF NOT EXISTS saved_leads (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL, -- ID of the user who saved the lead
    lead_id INT NOT NULL, -- ID of the saved lead (references leads table)
    type VARCHAR(50) NOT NULL, -- Type of saved lead (e.g., 'leads' or 'company')
    email BOOLEAN DEFAULT FALSE, -- Whether email is saved
    mobile BOOLEAN DEFAULT FALSE, -- Whether mobile is saved
    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- Timestamp when the lead was saved
    CONSTRAINT fk_user
        FOREIGN KEY (user_id)
        REFERENCES users(id), -- Assuming you have a `users` table
    CONSTRAINT fk_lead
        FOREIGN KEY (lead_id)
        REFERENCES peopleLeads(id), -- Assuming you have a `leads` table
    CONSTRAINT unique_user_lead_type UNIQUE (user_id, lead_id, type) -- Unique constraint
);
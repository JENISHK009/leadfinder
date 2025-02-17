CREATE TABLE IF NOT EXISTS leads (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    title VARCHAR(255),
    company VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    email_status VARCHAR(100), -- New Column
    seniority VARCHAR(100), -- New Column
    departments VARCHAR(255), -- New Column
    work_direct_phone VARCHAR(50), -- New Column
    mobile_phone VARCHAR(50), -- New Column
    corporate_phone VARCHAR(50), -- New Column
    phone VARCHAR(50) UNIQUE,
    category VARCHAR(255),
    organization VARCHAR(255),
    position VARCHAR(255),
    country VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    company_address TEXT, -- New Column
    company_city VARCHAR(100), -- New Column
    company_state VARCHAR(100), -- New Column
    company_country VARCHAR(100), -- New Column
    industry VARCHAR(255),
    num_employees INT,
    linkedin_url VARCHAR(500),
    website VARCHAR(500),
    company_linkedin_url VARCHAR(500),
    facebook_url VARCHAR(500),
    twitter_url VARCHAR(500),
    keywords TEXT,
    technologies TEXT,
    seo_description TEXT, -- New Column
    annual_revenue VARCHAR(100),
    total_funding VARCHAR(100),
    latest_funding VARCHAR(100),
    latest_funding_amount VARCHAR(100),
    last_raised_at DATE,
    num_retail_locations INT, -- New Column
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX idx_first_name ON leads (first_name);
CREATE INDEX idx_last_name ON leads (last_name);
CREATE INDEX idx_email ON leads (email);
CREATE INDEX idx_company ON leads (company);
CREATE INDEX idx_industry ON leads (industry);
CREATE INDEX idx_country ON leads (country);
CREATE INDEX idx_state ON leads (state);
CREATE INDEX idx_city ON leads (city);
CREATE INDEX idx_seniority ON leads (seniority);
CREATE INDEX idx_departments ON leads (departments);
CREATE INDEX idx_num_employees ON leads (num_employees);

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS update_leads_timestamp ON leads;

-- Function to Update Timestamp
CREATE OR REPLACE FUNCTION update_leads_timestamp() 
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create Trigger
CREATE TRIGGER update_leads_timestamp
  BEFORE UPDATE ON leads
  FOR EACH ROW
  EXECUTE FUNCTION update_leads_timestamp();

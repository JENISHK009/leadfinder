CREATE TABLE IF NOT EXISTS peopleLeads (
    id SERIAL PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    title VARCHAR(255),
    company VARCHAR(255),
    email VARCHAR(255) UNIQUE,
    email_status VARCHAR(100),
    seniority VARCHAR(100),
    departments VARCHAR(255),
    work_direct_phone VARCHAR(50),
    mobile_phone VARCHAR(50),
    corporate_phone VARCHAR(50),
    phone VARCHAR(50) UNIQUE,
    category VARCHAR(255),
    organization VARCHAR(255),
    position VARCHAR(255),
    country VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    company_address TEXT,
    company_city VARCHAR(100),
    company_state VARCHAR(100),
    company_country VARCHAR(100),
    industry VARCHAR(255),
    num_employees INT,
    linkedin_url VARCHAR(500),
    website VARCHAR(500),
    company_linkedin_url VARCHAR(500),
    facebook_url VARCHAR(500),
    twitter_url VARCHAR(500),
    keywords TEXT,
    technologies TEXT,
    seo_description TEXT,
    annual_revenue VARCHAR(100),
    total_funding VARCHAR(100),
    latest_funding VARCHAR(100),
    latest_funding_amount VARCHAR(100),
    last_raised_at DATE,
    num_retail_locations INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX idx_first_name ON peopleLeads (first_name);
CREATE INDEX idx_last_name ON peopleLeads (last_name);
CREATE INDEX idx_email ON peopleLeads (email);
CREATE INDEX idx_company ON peopleLeads (company);
CREATE INDEX idx_industry ON peopleLeads (industry);
CREATE INDEX idx_country ON peopleLeads (country);
CREATE INDEX idx_state ON peopleLeads (state);
CREATE INDEX idx_city ON peopleLeads (city);
CREATE INDEX idx_seniority ON peopleLeads (seniority);
CREATE INDEX idx_departments ON peopleLeads (departments);
CREATE INDEX idx_num_employees ON peopleLeads (num_employees);

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS update_peopleLeads_timestamp ON peopleLeads;

-- Function to Update Timestamp
CREATE OR REPLACE FUNCTION update_peopleLeads_timestamp() 
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create Trigger
CREATE TRIGGER update_peopleLeads_timestamp
  BEFORE UPDATE ON peopleLeads
  FOR EACH ROW
  EXECUTE FUNCTION update_peopleLeads_timestamp();
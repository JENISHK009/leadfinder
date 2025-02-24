CREATE TABLE IF NOT EXISTS companies (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    num_employees INT,
    industry VARCHAR(255),
    website VARCHAR(500),
    company_linkedin_url VARCHAR(500),
    facebook_url VARCHAR(500),
    twitter_url VARCHAR(500),
    company_street VARCHAR(255),
    company_city VARCHAR(255),
    company_state VARCHAR(255),
    company_country VARCHAR(255),
    company_postal_code VARCHAR(50),
    company_address TEXT,
    keywords TEXT,
    company_phone VARCHAR(50),
    seo_description TEXT,
    technologies TEXT,
    total_funding VARCHAR(100),
    latest_funding VARCHAR(100),
    latest_funding_amount VARCHAR(100),
    last_raised_at DATE,
    annual_revenue VARCHAR(100),
    num_retail_locations INT,
    sic_codes VARCHAR(255),
    short_description TEXT,
    founded_year INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for faster querying
CREATE INDEX idx_company_name ON companies (company_name);
CREATE INDEX idx_company_industry ON companies (industry);
CREATE INDEX idx_company_country ON companies (company_country);
CREATE INDEX idx_company_state ON companies (company_state);
CREATE INDEX idx_company_city ON companies (company_city);
CREATE INDEX idx_company_num_employees ON companies (num_employees);
CREATE INDEX idx_founded_year ON companies (founded_year);

-- Drop existing trigger if it exists
DROP TRIGGER IF EXISTS update_companies_timestamp ON companies;

-- Function to Update Timestamp
CREATE OR REPLACE FUNCTION update_companies_timestamp() 
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = CURRENT_TIMESTAMP;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create Trigger
CREATE TRIGGER update_companies_timestamp
  BEFORE UPDATE ON companies
  FOR EACH ROW
  EXECUTE FUNCTION update_companies_timestamp();
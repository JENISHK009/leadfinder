import { Readable, Transform } from "stream";
import pool from "../config/db.js";
import {
  successResponse,
  errorResponse,
  sendCSVEmail,
} from "../utils/index.js";
import pkg from "pg-copy-streams";
const { from, from: copyFrom } = pkg;
import { deductCredits } from "../servies/userService.js";
import { uploadFileToS3 } from "../utils/index.js";

const jobTitleMappings = {
  'CEO': 'Chief Executive Officer',
  'CFO': 'Chief Financial Officer',
  'CMO': 'Chief Marketing Officer',
  'COO': 'Chief Operating Officer',
  'CTO': 'Chief Technology Officer',
  'CIO': 'Chief Information Officer',
  'CHRO': 'Chief Human Resources Officer',
  'CPO': 'Chief Product Officer',
  'CSO': 'Chief Strategy Officer',
  'CDO': 'Chief Data Officer',
  'CLO': 'Chief Legal Officer',
  'CAO': 'Chief Administrative Officer',
  'CCO': 'Chief Commercial Officer',
  'CXO': 'Chief Experience Officer',
  'CRO': 'Chief Revenue Officer',
  'CDAO': 'Chief Data & Analytics Officer',
  'HR': 'Human Resources',
  'IT': 'Information Technology',
  'PR': 'Public Relations',
  'R&D': 'Research and Development',
  'QA': 'Quality Assurance',
  'QC': 'Quality Control',
  'BD': 'Business Development',
  'PM': 'Project Manager',
  'UX': 'User Experience',
  'UI': 'User Interface',
  'CSR': 'Customer Service Representative',
  'PA': 'Personal Assistant',
  'EA': 'Executive Assistant',
  'MD': 'Managing Director',
  'VP': 'Vice President',
  'AVP': 'Assistant Vice President',
  'SE': 'Software Engineer',
  'SDE': 'Software Development Engineer',
  'ME': 'Mechanical Engineer',
  'EE': 'Electrical Engineer',
  'BA': 'Business Analyst',
  'DA': 'Data Analyst',
  'DS': 'Data Scientist',
  'DE': 'Data Engineer',
  'DBA': 'Database Administrator',
  'NOC': 'Network Operations Center',
  'SOC': 'Security Operations Center',
  'TAM': 'Technical Account Manager',
  'CA': 'Chartered Accountant',
  'CPA': 'Certified Public Accountant',
  'CFA': 'Chartered Financial Analyst',
  'FP&A': 'Financial Planning and Analysis',
  'AR': 'Accounts Receivable',
  'AP': 'Accounts Payable',
  'AE': 'Account Executive',
  'AM': 'Account Manager',
  'SDR': 'Sales Development Representative',
  'BDR': 'Business Development Representative',
  'KAM': 'Key Account Manager',
  'SEM': 'Search Engine Marketing',
  'SEO': 'Search Engine Optimization',
  'PPC': 'Pay-Per-Click',
  'SMM': 'Social Media Manager',
  'CRM': 'Customer Relationship Manager',
  'CSM': 'Customer Success Manager',
  'TSR': 'Technical Support Representative',
  'SCM': 'Supply Chain Manager',
  'OM': 'Operations Manager',
  'WHM': 'Warehouse Manager',
  'LOM': 'Logistics Manager',
  'PPM': 'Production Planning Manager',
  'MM': 'Materials Manager',
  'GC': 'General Counsel',
  'DPO': 'Data Protection Officer',
  'AML Officer': 'Anti-Money Laundering Officer',
  'CD': 'Creative Director',
  'AD': 'Art Director',
  'GD': 'Graphic Designer',
  'UXD': 'User Experience Designer',
  'UID': 'User Interface Designer',
  'VFX': 'Visual Effects Artist',
  'RN': 'Registered Nurse',
  'NP': 'Nurse Practitioner',
  'PT': 'Physical Therapist',
  'HOD': 'Head of Department',
  'PPT': 'Primary/Preschool Teacher',
  'PRT': 'Primary Teacher',
  'TGT': 'Trained Graduate Teacher',
  'PGT': 'Post Graduate Teacher',
  'LMS Admin': 'Learning Management System Administrator',
  'SME': 'Subject Matter Expert',
  'VP (Acad.)': 'Vice Principal (Academics)',
  'PE Teacher': 'Physical Education Teacher',
  'DOE': 'Director of Education',
  'QS': 'Quantity Surveyor',
  'CM': 'Construction Manager',
  'RE': 'Resident Engineer',
  'MEP Engineer': 'Mechanical, Electrical, and Plumbing Engineer',
  'BIM Manager': 'Building Information Modeling Manager',
  'HSE Officer': 'Health, Safety & Environment Officer',
  'DGM': 'Deputy General Manager',
  'SSE': 'Senior Site Engineer',
  'Sr. Architect': 'Senior Architect',
  'RM': 'Relationship Manager',
  'BM': 'Branch Manager',
  'FC': 'Finance Controller',
  'IB Analyst': 'Investment Banking Analyst',
  'AML Analyst': 'Anti-Money Laundering Analyst',
  'KYC Analyst': 'Know Your Customer Analyst',
  'Treasury Mgr': 'Treasury Manager',
  'PE': 'Process Engineer',
  'QE': 'Quality Engineer',
  'NPD': 'New Product Development',
  'MRO': 'Maintenance, Repair & Overhaul',
  'HOD (Prod.)': 'Head of Production',
  'QA/QC': 'Quality Assurance / Quality Control',
  'EHS Manager': 'Environment, Health & Safety Manager',
  'HOD (Med.)': 'Head of Department (Medical)',
  'LPN': 'Licensed Practical Nurse',
  'RT': 'Respiratory Therapist',
  'OT': 'Occupational Therapist',
  'MT': 'Medical Technologist',
  'Lab Tech': 'Laboratory Technician',
  'MO': 'Medical Officer',
  'DevOps': 'Development and Operations Engineer',
  'UI/UX Designer': 'User Interface / User Experience Designer',
  'SysAdmin': 'System Administrator',
  'ITSM': 'IT Service Management',
  'CISO': 'Chief Information Security Officer',
  'NOC Engineer': 'Network Operations Center Engineer',
  'SOC Analyst': 'Security Operations Center Analyst',
  'Cloud Arch.': 'Cloud Architect',
  'SVP': 'Senior Vice President'
};

// Create reverse mapping (long to short)
const reverseMappings = {};
Object.entries(jobTitleMappings).forEach(([short, long]) => {
  reverseMappings[long.toLowerCase()] = short;
});

// Helper function to expand job titles to include both short and long forms
const expandJobTitles = (jobTitles) => {
  const expandedTitles = new Set();

  jobTitles.forEach(title => {
    const titleLower = title.toLowerCase();

    // Add the original title
    expandedTitles.add(title);

    // Check if it's a short form and add long form
    const longForm = jobTitleMappings[title];
    if (longForm) {
      expandedTitles.add(longForm);
    }

    // Check if it's a long form and add short form
    const shortForm = reverseMappings[titleLower];
    if (shortForm) {
      expandedTitles.add(shortForm);
    }

    // Check for exact matches and reasonable partial matches only
    Object.entries(jobTitleMappings).forEach(([short, long]) => {
      const shortLower = short.toLowerCase();
      const longLower = long.toLowerCase();
      
      // Only expand if there's an exact match or the search term is clearly an abbreviation/partial match
      // Exact word boundary match for abbreviations (e.g., "CEO" in "Senior CEO")
      const exactWordRegex = new RegExp(`\\b${shortLower.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
      if (exactWordRegex.test(titleLower)) {
        expandedTitles.add(short);
        expandedTitles.add(long);
      }
      // Exact word boundary match for long forms (e.g., "Chief Executive" in "Chief Executive Officer")
      else if (longLower.includes(' ') && titleLower.includes(longLower)) {
        expandedTitles.add(short);
        expandedTitles.add(long);
      }
      // Only match if the search term exactly matches the start of an abbreviation (minimum 2 chars)
      else if (titleLower.length >= 2 && shortLower.startsWith(titleLower) && titleLower.length <= shortLower.length) {
        expandedTitles.add(short);
        expandedTitles.add(long);
      }
    });
  });

  return Array.from(expandedTitles);
};

const cleanPhone = (phone) => phone ? phone.replace(/[^\d+]/g, "") : null;

const parseInteger = (value) => {
  if (!value || value === "" || value === "\\N") return null;
  const num = parseInt(value, 10);
  return isNaN(num) ? null : num;
};

const safeInteger = (value) => {
  if (!value || value === "" || value === "\\N") return null;

  if (!/^\d+$/.test(value.toString().trim())) {
    return null;
  }

  const num = parseInt(value.toString().trim(), 10);
  return isNaN(num) ? null : num;
};

const escapeCopy = (value) => {
  if (value === null || value === undefined || value === "") return "\\N";
  return value.toString()
    .replace(/\\/g, "\\\\")
    .replace(/\t/g, "\\t")
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
};

class JsonToCopyTransform extends Transform {
  constructor(options) {
    super({ objectMode: true });
  }

  _transform(row, encoding, callback) {
    const values = [
      row["First Name"] || null,
      row["Last Name"] || null,
      row["Title"] || null,
      row["Company"] || null,
      row["Email"] || null,
      row["Email Status"] || null,
      row["Seniority"] || null,
      row["Departments"] || null,
      cleanPhone(row["Work Direct Phone"]),
      cleanPhone(row["Mobile Phone"]),
      cleanPhone(row["Corporate Phone"]),
      parseInteger(row["# Employees"]),
      row["Industry"] || null,
      row["Keywords"] || null,
      row["Person Linkedin Url"] || null,
      row["Website"] || null,
      row["Company Linkedin Url"] || null,
      row["Facebook Url"] || null,
      row["Twitter Url"] || null,
      row["City"] || null,
      row["State"] || null,
      row["Country"] || null,
      row["Company Address"] || null,
      row["Company City"] || null,
      row["Company State"] || null,
      row["Company Country"] || null,
      row["SEO Description"] || null,
      row["Technologies"] || null,
      row["Annual Revenue"] || null,
      row["Total Funding"] || null,
      row["Latest Funding"] || null,
      row["Latest Funding Amount"] || null,
      row["Last Raised At"] || null,
      parseInteger(row["Number of Retail Locations"])
    ].map(escapeCopy).join('\t');

    this.push(values + '\n');
    callback();
  }
}

const processAndInsertLeads = async (jsonData) => {
  const client = await pool.connect();
  const totalRows = jsonData.length;
  let rowCount = 0;

  try {
    await client.query("BEGIN");

    await client.query(`
      CREATE TEMP TABLE temp_leads (
        id SERIAL,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        title VARCHAR(255),
        company VARCHAR(255),
        email VARCHAR(255),
        email_status VARCHAR(100),
        seniority VARCHAR(100),
        departments VARCHAR(255),
        work_direct_phone VARCHAR(50),
        mobile_phone VARCHAR(50),
        corporate_phone VARCHAR(50),
        num_employees INTEGER,
        industry VARCHAR(255),
        keywords TEXT,
        linkedin_url VARCHAR(500),
        website VARCHAR(500),
        company_linkedin_url VARCHAR(500),
        facebook_url VARCHAR(500),
        twitter_url VARCHAR(500),
        city VARCHAR(100),
        state VARCHAR(100),
        country VARCHAR(100),
        company_address TEXT,
        company_city VARCHAR(100),
        company_state VARCHAR(100),
        company_country VARCHAR(100),
        seo_description TEXT,
        technologies TEXT,
        annual_revenue VARCHAR(100),
        total_funding VARCHAR(100),
        latest_funding VARCHAR(100),
        latest_funding_amount VARCHAR(100),
        last_raised_at VARCHAR(100),
        num_retail_locations INTEGER
      ) ON COMMIT DROP
    `);

    // Use COPY for bulk insert - much faster than individual inserts
    const copyStream = client.query(
      copyFrom(`COPY temp_leads (
        first_name, last_name, title, company, email, email_status, seniority, departments, 
        work_direct_phone, mobile_phone, corporate_phone, num_employees, industry, keywords, 
        linkedin_url, website, company_linkedin_url, facebook_url, twitter_url, city, state, 
        country, company_address, company_city, company_state, company_country, seo_description, 
        technologies, annual_revenue, total_funding, latest_funding, latest_funding_amount, 
        last_raised_at, num_retail_locations
      ) FROM STDIN WITH DELIMITER E'\\t' NULL '\\N'`)
    );

    const jsonStream = Readable.from(jsonData);
    const transformStream = new JsonToCopyTransform();
    jsonStream.pipe(transformStream).pipe(copyStream);

    await new Promise((resolve, reject) => {
      copyStream.on('error', reject);
      copyStream.on('finish', resolve);
    });

    rowCount = totalRows;

    // Create indexes in parallel using Promise.all
    await Promise.all([
      client.query(`
        CREATE INDEX temp_company_linkedin_idx ON temp_leads (company_linkedin_url) 
        WHERE company_linkedin_url IS NOT NULL AND company_linkedin_url != '';
      `),
      client.query(`
        CREATE INDEX temp_linkedin_idx ON temp_leads (linkedin_url) 
        WHERE linkedin_url IS NOT NULL AND linkedin_url != '';
      `)
    ]);

    // Perform both inserts in parallel
    await Promise.all([
      // Companies insert
      client.query(`
        WITH distinct_companies AS (
          SELECT DISTINCT ON (company_linkedin_url)
            company, num_employees, website, company_linkedin_url,
            company_address, company_city, company_state, company_country,
            total_funding, latest_funding, latest_funding_amount, last_raised_at,
            annual_revenue, num_retail_locations
          FROM temp_leads
          WHERE company_linkedin_url IS NOT NULL AND company_linkedin_url != ''
        )
        INSERT INTO companies (
          company_name, num_employees, website, company_linkedin_url,
          company_street, company_city, company_state, company_country, company_postal_code, company_address,
          company_phone, total_funding, latest_funding, latest_funding_amount,
          last_raised_at, annual_revenue, num_retail_locations, sic_codes, founded_year
        )
        SELECT 
          company, num_employees, website, company_linkedin_url,
          company_address, company_city, company_state, company_country, NULL, company_address,
          NULL, total_funding, latest_funding, latest_funding_amount,
          last_raised_at, annual_revenue, num_retail_locations,
          NULL, NULL
        FROM distinct_companies
        ON CONFLICT (company_linkedin_url) DO UPDATE SET
          company_name = EXCLUDED.company_name,
          num_employees = EXCLUDED.num_employees,
          website = EXCLUDED.website,
          company_street = EXCLUDED.company_street,
          company_city = EXCLUDED.company_city,
          company_state = EXCLUDED.company_state,
          company_country = EXCLUDED.company_country,
          company_address = EXCLUDED.company_address,
          total_funding = EXCLUDED.total_funding,
          latest_funding = EXCLUDED.latest_funding,
          latest_funding_amount = EXCLUDED.latest_funding_amount,
          last_raised_at = EXCLUDED.last_raised_at,
          annual_revenue = EXCLUDED.annual_revenue,
          num_retail_locations = EXCLUDED.num_retail_locations,
          updated_at = CURRENT_TIMESTAMP
      `),
      
      // People leads insert
      client.query(`
        INSERT INTO peopleLeads (
          first_name, last_name, title, company, email, email_status, seniority, departments, work_direct_phone,
          mobile_phone, corporate_phone, num_employees, industry, keywords, linkedin_url, website, company_linkedin_url,
          facebook_url, twitter_url, city, state, country, company_address, company_city, company_state, company_country,
          seo_description, technologies, annual_revenue, total_funding, latest_funding, latest_funding_amount,
          last_raised_at, num_retail_locations
        )
        SELECT 
          first_name, last_name, title, company, email, email_status, seniority, departments, work_direct_phone,
          mobile_phone, corporate_phone, num_employees, industry, keywords, linkedin_url, website, company_linkedin_url,
          facebook_url, twitter_url, city, state, country, company_address, company_city, company_state, company_country,
          seo_description, technologies, annual_revenue, total_funding, latest_funding, latest_funding_amount,
          last_raised_at, num_retail_locations
        FROM temp_leads
        WHERE linkedin_url IS NOT NULL AND linkedin_url != ''
        ON CONFLICT (linkedin_url) DO UPDATE SET
          first_name = EXCLUDED.first_name,
          last_name = EXCLUDED.last_name,
          title = EXCLUDED.title,
          company = EXCLUDED.company,
          email = EXCLUDED.email,
          email_status = EXCLUDED.email_status,
          seniority = EXCLUDED.seniority,
          departments = EXCLUDED.departments,
          work_direct_phone = EXCLUDED.work_direct_phone,
          mobile_phone = EXCLUDED.mobile_phone,
          corporate_phone = EXCLUDED.corporate_phone,
          num_employees = EXCLUDED.num_employees,
          industry = EXCLUDED.industry,
          keywords = EXCLUDED.keywords,
          website = EXCLUDED.website,
          company_linkedin_url = EXCLUDED.company_linkedin_url,
          facebook_url = EXCLUDED.facebook_url,
          twitter_url = EXCLUDED.twitter_url,
          city = EXCLUDED.city,
          state = EXCLUDED.state,
          country = EXCLUDED.country,
          company_address = EXCLUDED.company_address,
          company_city = EXCLUDED.company_city,
          company_state = EXCLUDED.company_state,
          company_country = EXCLUDED.company_country,
          seo_description = EXCLUDED.seo_description,
          technologies = EXCLUDED.technologies,
          annual_revenue = EXCLUDED.annual_revenue,
          total_funding = EXCLUDED.total_funding,
          latest_funding = EXCLUDED.latest_funding,
          latest_funding_amount = EXCLUDED.latest_funding_amount,
          last_raised_at = EXCLUDED.last_raised_at,
          num_retail_locations = EXCLUDED.num_retail_locations,
          updated_at = CURRENT_TIMESTAMP
      `)
    ]);

    await client.query("COMMIT");
    return rowCount;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

const addPeopleLeadsData = async (req, res) => {
  if (!req.body || !Array.isArray(req.body) || req.body.length === 0) {
    return errorResponse(res, "Valid JSON data array is required", 400);
  }

  try {
    const startTime = Date.now();

    try {
      const processedCount = await processAndInsertLeads(req.body);

      const endTime = Date.now();
      const timeTaken = (endTime - startTime) / 1000;

      return successResponse(res, {
        message: "Data inserted or updated successfully",
        count: processedCount,
        time_taken: `${timeTaken} seconds`,
      });
    } catch (error) {
      throw error;
    }
  } catch (error) {
    console.error("Error processing JSON data:", error);
    return errorResponse(res, error.message, 500);
  }
};

const editPeopleLeadsData = async (req, res) => {
  const { id, ...updateFields } = req.body;

  if (!id) return errorResponse(res, "ID is required");
  if (Object.keys(updateFields).length === 0)
    return errorResponse(res, "No fields provided for update");

  try {
    const keys = Object.keys(updateFields);
    const values = Object.values(updateFields);
    const setClause = keys
      .map((key, index) => `${key} = $${index + 1}`)
      .join(", ");

    const query = `UPDATE peopleLeads SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $${keys.length + 1
      } RETURNING *`;

    const { rows } = await pool.query(query, [...values, id]);

    if (rows.length === 0)
      return errorResponse(res, "No record found with the given ID");

    return successResponse(res, rows[0]);
  } catch (error) {
    console.error("Error updating people data:", error);
    return errorResponse(res, "Failed to update data", 500);
  }
};

const getPeopleLeads = async (req, res) => {
  try {
    const {
      includeIndustry,
      excludeIndustry,
      includeemployeeCount,
      includeRevenue,
      includemanagmentRole,
      includeCompany,
      excludeCompany,
      includedepartmentKeyword,
      includePersonalCountry,
      excludePersonalCountry,
      includecompanyLocation,
      excludeCompanyLocation,
      includejobTitles,
      excludeJobTitles,
      includetechnology,
      search,
      page = 1,
      limit = 10,
      funding = null,
      foundingYear = null,
    } = req.body;

    // Pre-expand job titles once to avoid repeated calculations
    const expandedIncludeTitles = includejobTitles && includejobTitles.length > 0 ? expandJobTitles(includejobTitles) : null;
    const expandedExcludeTitles = excludeJobTitles && excludeJobTitles.length > 0 ? expandJobTitles(excludeJobTitles) : null;

    // Build optimized query
    let baseQuery = `
      SELECT pl.*
      FROM peopleLeads pl
      LEFT JOIN companies c ON pl.company_linkedin_url = c.company_linkedin_url
      WHERE 1=1`;

    let values = [];
    let index = 1;

    // Optimized helper functions for better index usage - avoid LOWER() in queries
    const addIncludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        if (valueArray.length === 1) {
          // Single value - use ILIKE for case-insensitive matching with index support
          baseQuery += ` AND ${field} ILIKE $${index}`;
          values.push(valueArray[0]);
          index++;
        } else {
          // Multiple values - use OR conditions for better index usage
          const conditions = valueArray.map(() => `${field} ILIKE $${index++}`);
          baseQuery += ` AND (${conditions.join(' OR ')})`;
          values.push(...valueArray);
        }
      }
    };

    const addExcludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        if (valueArray.length === 1) {
          // Single value - use NOT ILIKE
          baseQuery += ` AND ${field} NOT ILIKE $${index}`;
          values.push(valueArray[0]);
          index++;
        } else {
          // Multiple values - use AND conditions
          const conditions = valueArray.map(() => `${field} NOT ILIKE $${index++}`);
          baseQuery += ` AND (${conditions.join(' AND ')})`;
          values.push(...valueArray);
        }
      }
    };

    // Add search functionality with optimized OR conditions
    if (search) {
      baseQuery += ` AND (
        pl.company ILIKE $${index} OR 
        pl.first_name ILIKE $${index} OR 
        pl.last_name ILIKE $${index} OR 
        pl.email ILIKE $${index} OR
        pl.title ILIKE $${index} OR
        pl.industry ILIKE $${index}
      )`;
      values.push(`%${search}%`);
      index++;
    }

    // Apply filters in optimal order (most selective first)
    // Industry filter - usually very selective
    addIncludeFilter("pl.industry", includeIndustry);
    addExcludeFilter("pl.industry", excludeIndustry);

    // Country filters - usually selective
    addIncludeFilter("pl.country", includePersonalCountry);
    addExcludeFilter("pl.country", excludePersonalCountry);

    // Company location filters
    addIncludeFilter("pl.company_country", includecompanyLocation);
    addExcludeFilter("pl.company_country", excludeCompanyLocation);

    // Employee count filter - optimized with proper numeric comparison
    if (includeemployeeCount && includeemployeeCount.length > 0) {
      const employeeConditions = [];
      for (const range of includeemployeeCount) {
        const [min, max] = range.split("-").map(Number);
        if (!isNaN(min) && !isNaN(max)) {
          employeeConditions.push(`(pl.num_employees >= $${index} AND pl.num_employees <= $${index + 1})`);
          values.push(min, max);
          index += 2;
        } else if (!isNaN(min) && range.includes("+")) {
          employeeConditions.push(`(pl.num_employees >= $${index})`);
          values.push(min);
          index++;
        } else {
          employeeConditions.push(`(pl.num_employees::text ILIKE $${index})`);
          values.push(`%${range}%`);
          index++;
        }
      }
      
      if (employeeConditions.length > 0) {
        baseQuery += ` AND (${employeeConditions.join(" OR ")})`;
      }
    }

    // Revenue filter - optimized with proper numeric comparison
    if (includeRevenue && includeRevenue.length > 0) {
      const revenueConditions = [];
      for (const range of includeRevenue) {
        const [min, max] = range.split("-").map((value) => {
          if (value.endsWith("M")) {
            return parseFloat(value) * 1000000;
          } else if (value.endsWith("B")) {
            return parseFloat(value) * 1000000000;
          } else {
            return parseFloat(value);
          }
        });

        if (!isNaN(min) && !isNaN(max)) {
          revenueConditions.push(
            `(CAST(REGEXP_REPLACE(pl.annual_revenue, '[^0-9.]', '', 'g') AS NUMERIC) >= $${index} AND CAST(REGEXP_REPLACE(pl.annual_revenue, '[^0-9.]', '', 'g') AS NUMERIC) <= $${index + 1})`
          );
          values.push(min, max);
          index += 2;
        } else if (!isNaN(min) && range.includes("+")) {
          revenueConditions.push(`(CAST(REGEXP_REPLACE(pl.annual_revenue, '[^0-9.]', '', 'g') AS NUMERIC) >= $${index})`);
          values.push(min);
          index++;
        }
      }

      if (revenueConditions.length > 0) {
        baseQuery += ` AND pl.annual_revenue IS NOT NULL AND pl.annual_revenue != '' AND pl.annual_revenue ~ '[0-9]' AND (${revenueConditions.join(" OR ")})`;
      }
    }

    // Management role filter
    addIncludeFilter("pl.seniority", includemanagmentRole);

    // Company filter
    addIncludeFilter("pl.company", includeCompany);
    addExcludeFilter("pl.company", excludeCompany);

    // Department keyword filter
    addIncludeFilter("pl.departments", includedepartmentKeyword);

    // Job title filters - optimized for better index usage
    if (expandedIncludeTitles) {
      if (expandedIncludeTitles.length === 1) {
        baseQuery += ` AND pl.title ILIKE $${index}`;
        values.push(`%${expandedIncludeTitles[0]}%`);
        index++;
      } else {
        const titleConditions = expandedIncludeTitles.map(() => `pl.title ILIKE $${index++}`);
        baseQuery += ` AND (${titleConditions.join(' OR ')})`;
        values.push(...expandedIncludeTitles.map(title => `%${title}%`));
      }
    }

    // Handle exclude job titles
    if (expandedExcludeTitles) {
      const excludeTitleConditions = expandedExcludeTitles.map(() => `pl.title NOT ILIKE $${index++}`);
      baseQuery += ` AND (${excludeTitleConditions.join(' AND ')})`;
      values.push(...expandedExcludeTitles.map(title => `%${title}%`));
    }

    // Technology filter
    if (includetechnology && includetechnology.length > 0) {
      if (includetechnology.length === 1) {
        baseQuery += ` AND pl.technologies ILIKE $${index}`;
        values.push(`%${includetechnology[0]}%`);
        index++;
      } else {
        const techConditions = includetechnology.map(() => `pl.technologies ILIKE $${index++}`);
        baseQuery += ` AND (${techConditions.join(' OR ')})`;
        values.push(...includetechnology.map(tech => `%${tech}%`));
      }
    }

    // Funding filter
    if (funding && funding.length > 0) {
      if (funding.length === 1) {
        baseQuery += ` AND pl.latest_funding ILIKE $${index}`;
        values.push(`%${funding[0]}%`);
        index++;
      } else {
        const fundingConditions = funding.map(() => `pl.latest_funding ILIKE $${index++}`);
        baseQuery += ` AND (${fundingConditions.join(' OR ')})`;
        values.push(...funding.map(fund => `%${fund}%`));
      }
    }

    // Handle founding year filter (multiple values)
    if (foundingYear && foundingYear.length > 0) {
      const placeholders = foundingYear.map(() => `$${index++}`).join(', ');
      baseQuery += ` AND c.founded_year = ANY(ARRAY[${placeholders}]::integer[])`;
      values.push(...foundingYear);
    }

    // Fast count estimation for large datasets
    const offset = (page - 1) * limit;
    let totalCount = 0;

    // For first page, get a fast estimate, for subsequent pages use a simple calculation
    if (page === 1) {
      try {
        // Use EXPLAIN to get row count estimate (very fast)
        const explainQuery = `EXPLAIN (FORMAT JSON) ${baseQuery}`;
        const explainResult = await pool.query(explainQuery, values);
        const planRows = explainResult.rows[0]['QUERY PLAN'][0]['Plan']['Plan Rows'] || 
                        explainResult.rows[0]['QUERY PLAN'][0]['Plan Rows'] || 0;
        totalCount = Math.ceil(planRows);
      } catch (explainError) {
        console.log("Explain failed, using fallback estimation");
        // Fallback: estimate based on sample
        totalCount = limit * 100; // Conservative estimate
      }
    } else {
      // For subsequent pages, estimate based on page number
      totalCount = page * limit + 1; // Ensure there's always a "next page" indication
    }

    // Add ordering and pagination
    baseQuery += `
      ORDER BY pl.id DESC
      LIMIT $${index} OFFSET $${index + 1}
    `;
    
    values.push(limit, offset);

    console.log("finalQuery>", baseQuery);
    console.log("values>", values);

    const { rows } = await pool.query(baseQuery, values);
    const perPageCount = rows.length;

    // Adjust total count based on actual results
    if (rows.length < limit && page > 1) {
      totalCount = (page - 1) * limit + rows.length;
    } else if (rows.length < limit && page === 1) {
      totalCount = rows.length;
    }

    return successResponse(res, {
      message: "Data fetched successfully",
      total_count: totalCount,
      per_page_count: perPageCount,
      data: rows,
    });
  } catch (error) {
    console.error("Error fetching people leads:", error);
    return errorResponse(res, "Error fetching data", 500);
  }
};

const exportPeopleLeadsToCSV = async (req, res) => {
  const client = await pool.connect();

  try {
    const {
      leadsId,
      includeIndustry,
      excludeIndustry,
      includeemployeeCount,
      includeRevenue,
      includemanagmentRole,
      includeCompany,
      excludeCompany,
      includedepartmentKeyword,
      includePersonalCountry,
      excludePersonalCountry,
      includecompanyLocation,
      excludeCompanyLocation,
      includejobTitles,
      excludeJobTitles,
      includetechnology,
      search,
      limit = 1000,
      perCompany = null,
      funding = null,
      foundingYear = null,
    } = req.body;

    const userId = req.currentUser.id;
    const userEmail = req.currentUser.email;
    const userRole = req.currentUser.roleName;

    if (!userId || !userEmail)
      return errorResponse(res, "User information not found", 400);

    await client.query("BEGIN");

    // Build query with parameterized values
    let whereClause = "WHERE 1=1";
    let values = [];
    let index = 1;

    if (leadsId?.length > 0) {
      whereClause = `WHERE id = ANY($${index})`;
      values.push(leadsId);
      index++;
    } else {
      // Helper functions for filters
      const addArrayFilter = (field, valueArray, operator = '=') => {
        if (valueArray?.length) {
          whereClause += ` AND ${field} ${operator === '=' ? '= ANY' : '<> ALL'}($${index})`;
          values.push(valueArray);
          index++;
        }
      };

      const addStringFilter = (field, value) => {
        if (value) {
          whereClause += ` AND ${field} ILIKE $${index}`;
          values.push(`%${value}%`);
          index++;
        }
      };

      // Search functionality
      if (search) {
        whereClause += ` AND (
          company ILIKE $${index} OR 
          first_name ILIKE $${index} OR 
          last_name ILIKE $${index} OR 
          email ILIKE $${index} OR
          title ILIKE $${index} OR
          industry ILIKE $${index}
        )`;
        values.push(`%${search}%`);
        index++;
      }

      // Apply filters
      addArrayFilter("industry", includeIndustry);
      addArrayFilter("industry", excludeIndustry, '<>');
      addArrayFilter("seniority", includemanagmentRole);
      addArrayFilter("company", includeCompany);
      addArrayFilter("company", excludeCompany, '<>');
      addArrayFilter("departments", includedepartmentKeyword);
      addArrayFilter("country", includePersonalCountry);
      addArrayFilter("country", excludePersonalCountry, '<>');
      addArrayFilter("company_country", includecompanyLocation);
      addArrayFilter("company_country", excludeCompanyLocation, '<>');
      addArrayFilter("technologies", includetechnology);

      // Job title filters - UPDATED SECTION with expanded matching (same as getPeopleLeads)
      if (includejobTitles?.length) {
        const expandedIncludeTitles = expandJobTitles(includejobTitles);

        whereClause += ` AND (`;
        const titleConditions = expandedIncludeTitles.map(() => `title ILIKE $${index++}`);
        whereClause += titleConditions.join(" OR ");
        whereClause += `)`;

        expandedIncludeTitles.forEach((title) => {
          values.push(`%${title}%`);
        });
      }

      if (excludeJobTitles?.length) {
        const expandedExcludeTitles = expandJobTitles(excludeJobTitles);

        whereClause += ` AND (`;
        const excludeTitleConditions = expandedExcludeTitles.map(() => `title NOT ILIKE $${index++}`);
        whereClause += excludeTitleConditions.join(" AND ");
        whereClause += `)`;

        expandedExcludeTitles.forEach((title) => {
          values.push(`%${title}%`);
        });
      }

      // Funding filter
      if (funding?.length) {
        whereClause += ` AND (${funding.map(() => `latest_funding ILIKE $${index++}`).join(" OR ")})`;
        values.push(...funding.map(fund => `%${fund}%`));
      }

      // Founding year filter
      if (foundingYear?.length) {
        whereClause += ` AND (${foundingYear.map(() => `EXTRACT(YEAR FROM last_raised_at) = $${index++}`).join(" OR ")})`;
        values.push(...foundingYear);
      }
    }

    // Final query with limit
    const finalQuery = `SELECT * FROM peopleLeads ${whereClause} LIMIT $${index}`;
    values.push(limit);

    console.log("finalQuery>", finalQuery);
    console.log("values>", values);

    const { rows } = await client.query(finalQuery, values);

    if (rows.length === 0) {
      await client.query("ROLLBACK");
      return errorResponse(res, "No data found to export", 404);
    }

    // Deduct credits for non-admin users
    if (userRole !== "admin") {
      const creditsToDeduct = rows.length;
      const deductionResult = await deductCredits(userId, creditsToDeduct);

      if (!deductionResult.success) {
        await client.query("ROLLBACK");
        return errorResponse(res, "Insufficient credits to export leads", 403);
      }
    }

    // Save exported leads to saved_leads table in batches
    if (rows.length > 0) {
      const batchSize = 1000;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        const placeholders = batch.map((_, idx) =>
          `($${idx * 5 + 1}, $${idx * 5 + 2}, $${idx * 5 + 3}, $${idx * 5 + 4}, $${idx * 5 + 5}, CURRENT_TIMESTAMP)`
        ).join(', ');

        const saveQuery = `
          INSERT INTO saved_leads (user_id, lead_id, type, email, mobile, saved_at)
          VALUES ${placeholders}
          ON CONFLICT (user_id, lead_id, type) 
          DO UPDATE SET 
            email = EXCLUDED.email,
            mobile = EXCLUDED.mobile,
            saved_at = EXCLUDED.saved_at
        `;

        const saveValues = batch.flatMap(row => [
          userId,
          row.id,
          'leads',
          'email' in row,
          'mobile_phone' in row ||
          'phone' in row
        ]);

        await client.query(saveQuery, saveValues);
      }
    }

    // Remove createdAt and updatedAt from all rows
    const cleanRows = rows.map(({ createdAt, updatedAt, created_at, updated_at, ...rest }) => rest);

    // Generate CSV - ensure header also excludes createdAt and updatedAt
    const csvHeader = Object.keys(cleanRows[0]).join(",") + "\n";
    const csvRows = cleanRows
      .map(row => Object.values(row)
        .map(value => (value == null ? "" : typeof value === "string" ? `"${value.replace(/"/g, '""')}"` : value))
        .join(","))
      .join("\n");
    const csvData = csvHeader + csvRows;
    const fileName = `people_leads_export_${Date.now()}.csv`;
    const bucketName = process.env.S3_BUCKET_NAME;
    const { fileUrl } = await uploadFileToS3(Buffer.from(csvData, "utf-8"), fileName, bucketName);

    // Record export
    const filters = {
      leadsId,
      includeIndustry,
      excludeIndustry,
      includeemployeeCount,
      includeRevenue,
      includemanagmentRole,
      includeCompany,
      excludeCompany,
      includedepartmentKeyword,
      includePersonalCountry,
      excludePersonalCountry,
      includecompanyLocation,
      excludeCompanyLocation,
      includejobTitles,
      excludeJobTitles,
      includetechnology,
      search,
      limit,
      perCompany,
      funding,
      foundingYear,
    };

    await client.query(`
      INSERT INTO exported_files (user_id, type, export_row_count, file_name, file_url, filters)
      VALUES ($1, $2, $3, $4, $5, $6)`,
      [userId, "leads", rows.length, fileName, fileUrl, filters]
    );

    // Handle response
    if (rows.length > 1000) {
      await sendCSVEmail(userEmail, csvData);
      await client.query("COMMIT");
      return successResponse(res, {
        message: "CSV file has been sent to your email.",
        remaining_credits: userRole !== "admin" ? deductionResult.remainingCredits : "N/A"
      });
    } else {
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      await client.query("COMMIT");
      return res.send(csvData);
    }
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error exporting leads:", error);
    return errorResponse(res, "Error exporting data", 500);
  } finally {
    client.release();
  }
};


const deductCreditsFromUser = async (req, res) => {
  try {
    const { count } = req.body;
    const { id: userId, roleName } = req.currentUser;

    if (!userId) {
      return errorResponse(res, "User information not found", 400);
    }

    if (roleName === "admin") {
      return successResponse(res, {
        message: "Admins are not required to deduct credits.",
      });
    }

    if (!count || typeof count !== "number" || count <= 0) {
      return errorResponse(res, "Invalid credit count provided", 400);
    }

    const deductionResult = await deductCredits(userId, count);

    if (!deductionResult.success) {
      return errorResponse(
        res,
        deductionResult.message || "Failed to deduct credits",
        403
      );
    }

    return successResponse(res, {
      message: `${count} credit(s) deducted successfully.`,
      remaining_credits: deductionResult.remainingCredits,
    });
  } catch (error) {
    console.error("Error deducting credits:", error);
    return errorResponse(res, "Error deducting credits", 500);
  }
};


const processAndInsertCompanies = async (results) => {
  console.log(`Starting to process ${results.length} companies`);
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Create the temp table with minimal columns - only what we need
    await client.query(`
      CREATE TEMP TABLE temp_companies (
        company_name TEXT,
        num_employees INTEGER,
        industry TEXT,
        website TEXT,
        company_linkedin_url TEXT,
        facebook_url TEXT,
        twitter_url TEXT,
        company_street TEXT,
        company_city TEXT,
        company_state TEXT,
        company_country TEXT,
        company_postal_code TEXT,
        company_address TEXT,
        keywords TEXT,
        company_phone TEXT,
        seo_description TEXT,
        technologies TEXT,
        total_funding TEXT,
        latest_funding TEXT,
        latest_funding_amount TEXT,
        last_raised_at TEXT,
        annual_revenue TEXT,
        num_retail_locations INTEGER,
        sic_codes TEXT,
        short_description TEXT,
        founded_year INTEGER
      ) ON COMMIT DROP
    `);

    const BATCH_SIZE = 200;
    const batches = [];

    for (let i = 0; i < results.length; i += BATCH_SIZE) {
      batches.push(results.slice(i, i + BATCH_SIZE));
    }

    // Prepare all batches in parallel for better memory efficiency
    const processedBatches = batches.map(batch => {
      return batch.map(row => {
        return [
          row["Company"] || "\\N",
          row["# Employees"] || "\\N",
          row["Industry"] || "\\N",
          row["Website"] || "\\N",
          row["Company Linkedin Url"] || "\\N",
          row["Facebook Url"] || "\\N",
          row["Twitter Url"] || "\\N",
          row["Company Street"] || "\\N",
          row["Company City"] || "\\N",
          row["Company State"] || "\\N",
          row["Company Country"] || "\\N",
          row["Company Postal Code"] || "\\N",
          row["Company Address"] || "\\N",
          row["Keywords"] || "\\N",
          cleanPhone(row["Company Phone"]) || "\\N",
          row["SEO Description"] || "\\N",
          row["Technologies"] || "\\N",
          row["Total Funding"] || "\\N",
          row["Latest Funding"] || "\\N",
          row["Latest Funding Amount"] || "\\N",
          row["Last Raised At"] || "\\N",
          row["Annual Revenue"] || "\\N",
          row["Number of Retail Locations"] || "\\N",
          row["SIC Codes"] || "\\N",
          row["Short Description"] || "\\N",
          row["Founded Year"] || "\\N",
        ]
          .map(val => {
            if (val === null || val === undefined || val === "" || val === "\\N") {
              return "\\N";
            } else {
              return val
                .toString()
                .replace(/\\/g, "\\\\")
                .replace(/[\r\n\t]/g, m => m === '\r' ? '\\r' : m === '\n' ? '\\n' : '\\t');
            }
          })
          .join("\t");
      }).join("\n");
    });

    for (let batchIndex = 0; batchIndex < processedBatches.length; batchIndex++) {
      const batchData = processedBatches[batchIndex];

      const copyStream = client.query(
        from(`COPY temp_companies (
              company_name, num_employees, industry, website, company_linkedin_url, facebook_url, twitter_url,
              company_street, company_city, company_state, company_country, company_postal_code, company_address,
              keywords, company_phone, seo_description, technologies, total_funding, latest_funding,
              latest_funding_amount, last_raised_at, annual_revenue, num_retail_locations, sic_codes,
              short_description, founded_year
          ) FROM STDIN WITH (FORMAT text, NULL '\\N')`)
      );

      const readable = new Readable();
      readable.push(batchData);
      readable.push(null);
      await new Promise((resolve, reject) => {
        readable
          .pipe(copyStream)
          .on("finish", resolve)
          .on("error", reject);
      });
    }

    const linkedInResult = await client.query(`
      WITH deduplicated AS (
        SELECT DISTINCT ON (company_linkedin_url) * 
        FROM temp_companies
        WHERE company_linkedin_url IS NOT NULL AND company_linkedin_url != ''
        ORDER BY company_linkedin_url, company_name
      )
      INSERT INTO companies (
          company_name, num_employees, industry, website, company_linkedin_url, facebook_url, twitter_url,
          company_street, company_city, company_state, company_country, company_postal_code, company_address,
          keywords, company_phone, seo_description, technologies, total_funding, latest_funding,
          latest_funding_amount, last_raised_at, annual_revenue, num_retail_locations, sic_codes,
          short_description, founded_year, created_at, updated_at
      )
      SELECT 
          company_name, 
          num_employees, 
          industry, 
          website, 
          company_linkedin_url, 
          facebook_url, 
          twitter_url,
          company_street, 
          company_city, 
          company_state, 
          company_country, 
          company_postal_code, 
          company_address,
          keywords, 
          company_phone, 
          seo_description, 
          technologies, 
          total_funding, 
          latest_funding,
          latest_funding_amount, 
          last_raised_at, 
          annual_revenue, 
          num_retail_locations,
          sic_codes, 
          short_description, 
          founded_year,
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
      FROM deduplicated
      ON CONFLICT (company_linkedin_url) 
      DO UPDATE SET
          company_name = EXCLUDED.company_name,
          num_employees = EXCLUDED.num_employees,
          industry = EXCLUDED.industry,
          website = EXCLUDED.website,
          facebook_url = EXCLUDED.facebook_url,
          twitter_url = EXCLUDED.twitter_url,
          company_street = EXCLUDED.company_street,
          company_city = EXCLUDED.company_city,
          company_state = EXCLUDED.company_state,
          company_country = EXCLUDED.company_country,
          company_postal_code = EXCLUDED.company_postal_code,
          company_address = EXCLUDED.company_address,
          keywords = EXCLUDED.keywords,
          company_phone = EXCLUDED.company_phone,
          seo_description = EXCLUDED.seo_description,
          technologies = EXCLUDED.technologies,
          total_funding = EXCLUDED.total_funding,
          latest_funding = EXCLUDED.latest_funding,
          latest_funding_amount = EXCLUDED.latest_funding_amount,
          last_raised_at = EXCLUDED.last_raised_at, 
          annual_revenue = EXCLUDED.annual_revenue,
          num_retail_locations = EXCLUDED.num_retail_locations,
          sic_codes = EXCLUDED.sic_codes,
          short_description = EXCLUDED.short_description,
          founded_year = EXCLUDED.founded_year,
          updated_at = CURRENT_TIMESTAMP
    `);

    // Create a temporary index for the next step (helps with the join)
    await client.query(`
      CREATE INDEX temp_comp_name_addr ON temp_companies (company_name, company_address) 
      WHERE company_linkedin_url IS NULL OR company_linkedin_url = ''
    `);

    // Handle non-LinkedIn records with a more optimized query using DISTINCT ON
    const noLinkedInResult = await client.query(`
      WITH distinct_companies AS (
        SELECT DISTINCT ON (company_name, company_address) *
        FROM temp_companies
        WHERE company_linkedin_url IS NULL OR company_linkedin_url = ''
        ORDER BY company_name, company_address, company_name
      )
      INSERT INTO companies (
          company_name, num_employees, industry, website, company_linkedin_url, facebook_url, twitter_url,
          company_street, company_city, company_state, company_country, company_postal_code, company_address,
          keywords, company_phone, seo_description, technologies, total_funding, latest_funding,
          latest_funding_amount, last_raised_at, annual_revenue, num_retail_locations, sic_codes,
          short_description, founded_year, created_at, updated_at
      )
      SELECT 
          dc.company_name, 
          dc.num_employees, 
          dc.industry, 
          dc.website, 
          dc.company_linkedin_url, 
          dc.facebook_url, 
          dc.twitter_url,
          dc.company_street, 
          dc.company_city, 
          dc.company_state, 
          dc.company_country, 
          dc.company_postal_code, 
          dc.company_address,
          dc.keywords, 
          dc.company_phone, 
          dc.seo_description, 
          dc.technologies, 
          dc.total_funding, 
          dc.latest_funding,
          dc.latest_funding_amount, 
          dc.last_raised_at, 
          dc.annual_revenue, 
          dc.num_retail_locations,
          dc.sic_codes, 
          dc.short_description, 
          dc.founded_year,
          CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
      FROM distinct_companies dc
      LEFT JOIN companies c ON 
          c.company_name = dc.company_name AND
          COALESCE(c.company_address, '') = COALESCE(dc.company_address, '')
      WHERE c.id IS NULL
    `);

    const totalCount = linkedInResult.rowCount + noLinkedInResult.rowCount;

    await client.query("COMMIT");
    return totalCount;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

const addCompaniesData = async (req, res) => {
  if (!req.body || !Array.isArray(req.body) || req.body.length === 0) {
    return errorResponse(res, "Valid JSON data array is required", 400);
  }

  const results = [...req.body];

  try {
    const startTime = Date.now();

    // Process in parallel with a worker pool for maximum throughput
    const insertedCount = await processAndInsertCompanies(results);

    const timeTaken = (Date.now() - startTime) / 1000;

    return successResponse(res, {
      message: "Data inserted or updated successfully",
      total_rows: results.length,
      inserted_rows: insertedCount,
      time_taken: `${timeTaken} seconds`,
    });
  } catch (error) {
    return errorResponse(res, `Error processing data: ${error.message}`, 500, {
      details: error.detail || error.hint || null
    });
  }
};

const getCompanies = async (req, res) => {
  try {
    const {
      employeeCount,
      companyRevenue,
      includeCompanyLocation,
      excludeCompanyLocation,
      includeIndustry,
      excludeIndustry,
      includeCompany,
      excludeCompany,
      includeTechnology,
      includeCompanyKeyword,
      search,
      page = 1,
      limit = 10,
      funding = null,
      foundingYear = null,
    } = req.body;

    // Build WHERE clause and parameters for both queries
    let whereClause = ` WHERE 1=1`;
    let countValues = [];
    let dataValues = [];

    // Build filters and collect parameters
    const filters = [];

    // Add search functionality
    if (search) {
      filters.push({
        type: 'search',
        clause: ` AND (company_name ILIKE ? OR industry ILIKE ? OR company_address ILIKE ? OR company_phone ILIKE ? OR seo_description ILIKE ? OR technologies ILIKE ?)`,
        params: [`%${search}%`, `%${search}%`, `%${search}%`, `%${search}%`, `%${search}%`, `%${search}%`]
      });
    }

    // Handle employee count filter
    if (employeeCount && employeeCount.length > 0) {
      const employeeConditions = [];
      const employeeParams = [];

      for (const range of employeeCount) {
        const [min, max] = range.split("-").map(Number);
        if (!isNaN(min) && !isNaN(max)) {
          employeeConditions.push(`(num_employees >= ? AND num_employees <= ?)`);
          employeeParams.push(min, max);
        } else if (!isNaN(min) && range.includes("+")) {
          employeeConditions.push(`num_employees >= ?`);
          employeeParams.push(min);
        } else {
          employeeConditions.push(`num_employees::text ILIKE ?`);
          employeeParams.push(`%${range}%`);
        }
      }

      if (employeeConditions.length > 0) {
        filters.push({
          type: 'employee',
          clause: ` AND (${employeeConditions.join(" OR ")})`,
          params: employeeParams
        });
      }
    }

    // Handle company revenue filter
    if (companyRevenue && companyRevenue.length > 0) {
      const revenueConditions = [];
      const revenueParams = [];

      for (const range of companyRevenue) {
        const [min, max] = range.split("-").map((value) => {
          if (value.endsWith("M")) {
            return parseFloat(value) * 1000000;
          } else if (value.endsWith("B")) {
            return parseFloat(value) * 1000000000;
          } else {
            return parseFloat(value);
          }
        });

        if (!isNaN(min) && !isNaN(max)) {
          revenueConditions.push(`(annual_revenue::numeric >= ? AND annual_revenue::numeric <= ?)`);
          revenueParams.push(min, max);
        } else if (!isNaN(min) && range.includes("+")) {
          revenueConditions.push(`annual_revenue::numeric >= ?`);
          revenueParams.push(min);
        }
      }

      if (revenueConditions.length > 0) {
        filters.push({
          type: 'revenue',
          clause: ` AND (${revenueConditions.join(" OR ")})`,
          params: revenueParams
        });
      }
    }

    // Handle include/exclude filters
    const addIncludeFilter = (field, valueArray, filterType) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(() => `LOWER(${field}) = LOWER(?)`);
        filters.push({
          type: filterType,
          clause: ` AND (${conditions.join(" OR ")})`,
          params: valueArray
        });
      }
    };

    const addExcludeFilter = (field, valueArray, filterType) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(() => `LOWER(${field}) <> LOWER(?)`);
        filters.push({
          type: filterType,
          clause: ` AND (${conditions.join(" AND ")})`,
          params: valueArray
        });
      }
    };

    const addStringFilter = (field, value, filterType) => {
      if (value) {
        filters.push({
          type: filterType,
          clause: ` AND ${field} ILIKE ?`,
          params: [`%${value}%`]
        });
      }
    };

    // Apply all filters
    addIncludeFilter("company_country", includeCompanyLocation, 'include_location');
    addExcludeFilter("company_country", excludeCompanyLocation, 'exclude_location');
    addIncludeFilter("industry", includeIndustry, 'include_industry');
    addExcludeFilter("industry", excludeIndustry, 'exclude_industry');
    addIncludeFilter("company_name", includeCompany, 'include_company');
    addExcludeFilter("company_name", excludeCompany, 'exclude_company');

    // Handle technology filters
    if (includeTechnology && includeTechnology.length > 0) {
      const techConditions = includeTechnology.map(() => `technologies ILIKE ?`);
      filters.push({
        type: 'technology',
        clause: ` AND (${techConditions.join(" OR ")})`,
        params: includeTechnology.map(tech => `%${tech}%`)
      });
    }

    // Handle company keyword filter
    addStringFilter("company_name", includeCompanyKeyword, 'company_keyword');

    // Handle funding filter
    if (funding && funding.length > 0) {
      const fundingConditions = funding.map(() => `latest_funding ILIKE ?`);
      filters.push({
        type: 'funding',
        clause: ` AND (${fundingConditions.join(" OR ")})`,
        params: funding.map(fund => `%${fund}%`)
      });
    }

    // Handle founding year filter
    if (foundingYear && foundingYear.length > 0) {
      const yearConditions = foundingYear.map(() => `founded_year = ?`);
      filters.push({
        type: 'founding_year',
        clause: ` AND (${yearConditions.join(" OR ")})`,
        params: foundingYear
      });
    }

    // Build final queries with proper parameter indexing
    let countQuery = `SELECT COUNT(*) as total_count FROM companies${whereClause}`;
    let dataQuery = `SELECT * FROM companies${whereClause}`;

    // Add all filter clauses and collect parameters
    filters.forEach(filter => {
      whereClause += filter.clause;
      countValues.push(...filter.params);
      dataValues.push(...filter.params);
    });

    // Replace placeholders with proper parameter numbers
    let countIndex = 1;
    let dataIndex = 1;

    countQuery = whereClause.replace(/\?/g, () => `$${countIndex++}`);
    let dataClause = whereClause.replace(/\?/g, () => `$${dataIndex++}`);

    countQuery = `SELECT COUNT(*) as total_count FROM companies${countQuery}`;
    dataQuery = `SELECT * FROM companies${dataClause}`;

    // Add pagination to data query
    const offset = (page - 1) * limit;
    dataQuery += ` ORDER BY id DESC LIMIT $${dataIndex++} OFFSET $${dataIndex++}`;
    dataValues.push(limit, offset);

    console.log("countQuery>", countQuery);
    console.log("dataQuery>", dataQuery);
    console.log("countValues>", countValues);
    console.log("values>", dataValues);

    // Execute count query first
    const countResult = await pool.query(countQuery, countValues);
    const totalCount = parseInt(countResult.rows[0].total_count, 10);

    // If no results, return early
    if (totalCount === 0) {
      return res.status(200).json({
        message: "Data fetched successfully",
        total_count: 0,
        per_page_count: 0,
        data: [],
      });
    }

    // Execute data query
    const dataResult = await pool.query(dataQuery, dataValues);
    const perPageCount = dataResult.rows.length;

    return res.status(200).json({
      message: "Data fetched successfully",
      total_count: totalCount,
      per_page_count: perPageCount,
      data: dataResult.rows,
    });
  } catch (error) {
    console.error("Error fetching companies:", error);
    return res.status(500).json({ error: "Error fetching data" });
  }
};

const exportCompaniesToCSV = async (req, res) => {
  const client = await pool.connect();

  try {
    const {
      employeeCount,
      companyRevenue,
      includeCompanyLocation,
      excludeCompanyLocation,
      includeIndustry,
      excludeIndustry,
      includeCompany,
      excludeCompany,
      includeTechnology,
      includeCompanyKeyword,
      search,
      limit = 1000,
      funding = null,
      foundingYear = null,
      leadsId = null,
    } = req.body;

    const userId = req.currentUser.id;
    const userEmail = req.currentUser.email;
    const userRole = req.currentUser.roleName;

    if (!userId || !userEmail) {
      return res.status(400).json({ error: "User information not found" });
    }

    await client.query("BEGIN");

    // Build the query with parameterized values
    let baseQuery = `SELECT * FROM companies WHERE 1=1`;
    const values = [];
    let index = 1;

    // Handle leadsId filter
    if (leadsId && leadsId.length > 0) {
      baseQuery = `SELECT * FROM companies WHERE id = ANY($${index})`;
      values.push(leadsId);
      index++;
      baseQuery += ` LIMIT $${index}`;
      values.push(limit);
    } else {
      // Search functionality
      if (search) {
        baseQuery += ` AND (
          company_name ILIKE $${index} OR 
          industry ILIKE $${index} OR 
          company_address ILIKE $${index} OR 
          company_phone ILIKE $${index} OR
          seo_description ILIKE $${index} OR
          technologies ILIKE $${index}
        )`;
        values.push(`%${search}%`);
        index++;
      }

      // Employee count filter
      if (employeeCount && employeeCount.length > 0) {
        const conditions = [];
        for (const range of employeeCount) {
          const [min, max] = range.split("-").map(Number);
          if (!isNaN(min) && !isNaN(max)) {
            conditions.push(`(num_employees >= $${index} AND num_employees <= $${index + 1})`);
            values.push(min, max);
            index += 2;
          } else if (!isNaN(min) && range.includes("+")) {
            conditions.push(`num_employees >= $${index}`);
            values.push(min);
            index++;
          } else {
            conditions.push(`num_employees::text ILIKE $${index}`);
            values.push(`%${range}%`);
            index++;
          }
        }
        if (conditions.length) baseQuery += ` AND (${conditions.join(" OR ")})`;
      }

      // Company revenue filter
      if (companyRevenue && companyRevenue.length > 0) {
        const conditions = [];
        for (const range of companyRevenue) {
          const [min, max] = range.split("-").map(value => {
            if (value.endsWith("M")) return parseFloat(value) * 1000000;
            if (value.endsWith("B")) return parseFloat(value) * 1000000000;
            return parseFloat(value);
          });

          if (!isNaN(min) && !isNaN(max)) {
            conditions.push(`(CAST(annual_revenue AS NUMERIC) >= $${index} AND CAST(annual_revenue AS NUMERIC) <= $${index + 1})`);
            values.push(min, max);
            index += 2;
          } else if (!isNaN(min) && range.includes("+")) {
            conditions.push(`(CAST(annual_revenue AS NUMERIC) >= $${index})`);
            values.push(min);
            index++;
          }
        }
        if (conditions.length) baseQuery += ` AND (${conditions.join(" OR ")})`;
      }

      // Other filters
      const addArrayFilter = (field, valuesArray, operator = '=') => {
        if (valuesArray?.length) {
          const placeholders = valuesArray.map((_, i) =>
            `${field} ILIKE $${index + i}`
          ).join(` ${operator} `);
          baseQuery += ` AND (${placeholders})`;
          values.push(...valuesArray.map(v => `%${v}%`));
          index += valuesArray.length;
        }
      };

      addArrayFilter("company_country", includeCompanyLocation);
      addArrayFilter("company_country", excludeCompanyLocation, "<>");
      addArrayFilter("industry", includeIndustry);
      addArrayFilter("industry", excludeIndustry, "<>");
      addArrayFilter("company_name", includeCompany);
      addArrayFilter("company_name", excludeCompany, "<>");

      if (includeCompanyKeyword) {
        baseQuery += ` AND company_name ILIKE $${index}`;
        values.push(`%${includeCompanyKeyword}%`);
        index++;
      }

      // Technology filters
      if (includeTechnology?.length) {
        baseQuery += ` AND (${includeTechnology.map(() =>
          `technologies ILIKE $${index++}`
        ).join(" OR ")})`;
        values.push(...includeTechnology.map(t => `%${t}%`));
      }

      // Funding filter
      if (funding?.length) {
        baseQuery += ` AND (${funding.map(() =>
          `latest_funding ILIKE $${index++}`
        ).join(" OR ")})`;
        values.push(...funding.map(f => `%${f}%`));
      }

      // Founding year filter
      if (foundingYear?.length) {
        baseQuery += ` AND (${foundingYear.map(() =>
          `founded_year = $${index++}`
        ).join(" OR ")})`;
        values.push(...foundingYear);
      }

      baseQuery += ` LIMIT $${index}`;
      values.push(limit);
    }

    console.log("baseQuery>", baseQuery);
    console.log("values>", values);

    const { rows } = await client.query(baseQuery, values);

    if (rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "No data found to export" });
    }
    console.log("rows>>", rows)
    // Deduct credits for non-admin users
    if (userRole !== "admin") {
      const creditsToDeduct = rows.length;
      const deductionResult = await deductCredits(userId, creditsToDeduct);

      if (!deductionResult.success) {
        await client.query("ROLLBACK");
        return res.status(403).json({ error: "Insufficient credits to export data" });
      }
    }

    if (rows.length > 0) {
      const batchSize = 1000;
      for (let i = 0; i < rows.length; i += batchSize) {
        const batch = rows.slice(i, i + batchSize);
        const placeholders = batch.map((_, idx) =>
          `($${idx * 5 + 1}, $${idx * 5 + 2}, $${idx * 5 + 3}, $${idx * 5 + 4}, $${idx * 5 + 5}, CURRENT_TIMESTAMP)`
        ).join(', ');

        const query = `
          INSERT INTO saved_leads (user_id, lead_id, type, email, mobile, saved_at)
          VALUES ${placeholders}
          ON CONFLICT (user_id, lead_id, type) 
          DO UPDATE SET
            email = EXCLUDED.email,
            mobile = EXCLUDED.mobile,
            saved_at = EXCLUDED.saved_at
        `;

        const batchValues = batch.flatMap(row => [
          userId,
          row.id,
          'company',
          'email' in row,
          'company_phone' in row
        ]);

        await client.query(query, batchValues);
      }
    }

    // Generate CSV data with optimized string handling - Remove createdAt and updatedAt
    const headers = Object.keys(rows[0])
      .filter(key => !['createdAt', 'updatedAt', 'created_at', 'updated_at', 'row_num'].includes(key));

    const csvRows = new Array(rows.length + 1);
    csvRows[0] = headers.join(',');

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const values = headers.map(header => {
        const value = row[header];
        if (value == null) return '';
        if (typeof value === 'string') {
          return `"${value.replace(/"/g, '""')}"`;
        }
        return value;
      });
      csvRows[i + 1] = values.join(',');
    }

    const csvData = csvRows.join('\n');

    // Upload CSV to S3
    const fileName = `companies_export_${Date.now()}.csv`;
    const bucketName = process.env.S3_BUCKET_NAME;
    const { fileUrl } = await uploadFileToS3(
      Buffer.from(csvData, "utf-8"),
      fileName,
      bucketName
    );

    // Record export in database
    await client.query(`
      INSERT INTO exported_files (user_id, type, export_row_count, file_name, file_url, filters)
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      userId,
      "company",
      rows.length,
      fileName,
      fileUrl,
      req.body
    ]);

    // Handle response based on size
    if (rows.length > 1000) {
      await sendCSVEmail(userEmail, csvData);
      await client.query("COMMIT");
      return res.status(200).json({
        message: `CSV file has been sent to your email.`,
        remaining_credits: userRole !== "admin" ? deductionResult.remainingCredits : "N/A",
      });
    } else {
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
      await client.query("COMMIT");
      return res.send(csvData);
    }
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error exporting company data:", error);
    return res.status(500).json({ error: "Error exporting data" });
  } finally {
    client.release();
  }
};

const editCompanyLeadData = async (req, res) => {
  const { id, ...updateFields } = req.body;

  if (!id) return errorResponse(res, "ID is required");
  if (Object.keys(updateFields).length === 0)
    return errorResponse(res, "No fields provided for update");

  try {
    const keys = Object.keys(updateFields);
    const values = Object.values(updateFields);
    const setClause = keys
      .map((key, index) => `${key} = $${index + 1}`)
      .join(", ");

    const query = `UPDATE companies SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $${keys.length + 1
      } RETURNING *`;

    const { rows } = await pool.query(query, [...values, id]);

    if (rows.length === 0)
      return errorResponse(res, "No record found with the given ID");

    return successResponse(res, rows[0]);
  } catch (error) {
    console.error("Error updating company data:", error);
    return errorResponse(res, "Failed to update data", 500);
  }
};

const getCompanyChartData = async (req, res) => {
  try {
    const client = await pool.connect();

    // Fetch location data (group by company_city)
    const locationQuery = `
      SELECT company_city AS location, COUNT(*) AS count
      FROM companies
      WHERE company_city IS NOT NULL
      GROUP BY company_city
      ORDER BY count DESC;  -- Limit to top 10 locations
    `;

    // Fetch industry data (group by industry)
    const industryQuery = `
      SELECT industry, COUNT(*) AS count
      FROM companies
      WHERE industry IS NOT NULL
      GROUP BY industry
      ORDER BY count DESC;  -- Limit to top 10 industries
    `;

    // Fetch employee size data (group by num_employees into ranges)
    const employeeSizeQuery = `
      SELECT 
        CASE
          WHEN num_employees BETWEEN 1 AND 10 THEN '1-10'
          WHEN num_employees BETWEEN 11 AND 50 THEN '11-50'
          WHEN num_employees BETWEEN 51 AND 200 THEN '51-200'
          WHEN num_employees BETWEEN 201 AND 500 THEN '201-500'
          WHEN num_employees BETWEEN 501 AND 1000 THEN '501-1000'
          WHEN num_employees > 1000 THEN '1000+'
          ELSE 'Unknown'
        END AS employee_size,
        COUNT(*) AS count
      FROM companies
      WHERE num_employees IS NOT NULL
      GROUP BY employee_size
      ORDER BY employee_size;
    `;

    // Execute all queries in parallel
    const [locationResult, industryResult, employeeSizeResult] =
      await Promise.all([
        client.query(locationQuery),
        client.query(industryQuery),
        client.query(employeeSizeQuery),
      ]);

    client.release();

    // Format the data as arrays of arrays
    const locationData = locationResult.rows.map((row) => [
      row.location,
      row.count,
    ]);

    const industryData = industryResult.rows.map((row) => [
      row.industry,
      row.count,
    ]);

    const employeeSizeData = employeeSizeResult.rows.map((row) => [
      row.employee_size,
      row.count,
    ]);

    return successResponse(res, {
      message: "Chart data fetched successfully",
      data: {
        location: locationData,
        industry: industryData,
        employee_size: employeeSizeData,
      },
    });
  } catch (error) {
    console.error("Error fetching chart data:", error);
    return errorResponse(res, "Error fetching chart data", 500);
  }
};

const getPeopleLeadsDepartmentChartData = async (req, res) => {
  try {
    const client = await pool.connect();

    // Fetch department data (group by departments)
    const departmentQuery = `
      SELECT departments, COUNT(*) AS count
      FROM peopleLeads
      WHERE departments IS NOT NULL
      GROUP BY departments
      ORDER BY count DESC
      LIMIT 10;  -- Limit to top 10 departments
    `;

    // Fetch country data (group by country)
    const countryQuery = `
      SELECT country, COUNT(*) AS count
      FROM peopleLeads
      WHERE country IS NOT NULL
      GROUP BY country
      ORDER BY count DESC
      LIMIT 10;  -- Limit to top 10 countries
    `;

    // Fetch industry data (group by industry)
    const industryQuery = `
      SELECT industry, COUNT(*) AS count
      FROM peopleLeads
      WHERE industry IS NOT NULL
      GROUP BY industry
      ORDER BY count DESC
      LIMIT 10;  -- Limit to top 10 industries
    `;

    // Fetch employee size data (group by num_employees into ranges)
    const employeeSizeQuery = `
      SELECT 
        CASE
          WHEN num_employees BETWEEN 1 AND 10 THEN '1-10'
          WHEN num_employees BETWEEN 11 AND 50 THEN '11-50'
          WHEN num_employees BETWEEN 51 AND 200 THEN '51-200'
          WHEN num_employees BETWEEN 201 AND 500 THEN '201-500'
          WHEN num_employees BETWEEN 501 AND 1000 THEN '501-1000'
          WHEN num_employees > 1000 THEN '1000+'
          ELSE 'Unknown'
        END AS employee_size,
        COUNT(*) AS count
      FROM peopleLeads
      WHERE num_employees IS NOT NULL
      GROUP BY employee_size
      ORDER BY employee_size;
    `;

    // Execute all queries in parallel
    const [
      departmentResult,
      countryResult,
      industryResult,
      employeeSizeResult,
    ] = await Promise.all([
      client.query(departmentQuery),
      client.query(countryQuery),
      client.query(industryQuery),
      client.query(employeeSizeQuery),
    ]);

    client.release();

    // Format the data as arrays of arrays
    const departmentData = departmentResult.rows.map((row) => [
      row.departments,
      row.count,
    ]);

    const countryData = countryResult.rows.map((row) => [
      row.country,
      row.count,
    ]);

    const industryData = industryResult.rows.map((row) => [
      row.industry,
      row.count,
    ]);

    const employeeSizeData = employeeSizeResult.rows.map((row) => [
      row.employee_size,
      row.count,
    ]);

    return successResponse(res, {
      message: "Chart data fetched successfully",
      data: {
        departments: departmentData,
        countries: countryData,
        industries: industryData,
        employee_sizes: employeeSizeData,
      },
    });
  } catch (error) {
    console.error("Error fetching chart data:", error);
    return errorResponse(res, "Error fetching chart data", 500);
  }
};

const getExportedFiles = async (req, res) => {
  const client = await pool.connect();

  try {
    const { type, page = 1, limit = 10 } = req.query;
    const userId = req.currentUser.id; // Get userId from the authenticated user

    if (!userId) {
      return errorResponse(res, "User ID is required", 400);
    }

    // Calculate offset for pagination
    const offset = (page - 1) * limit;

    // Build the base query
    let query = `
      SELECT *
      FROM exported_files
      WHERE user_id = $1
    `;
    const queryParams = [userId];

    // Add type filter if provided
    if (type) {
      query += ` AND type = $${queryParams.length + 1}`;
      queryParams.push(type);
    }

    // Add pagination
    query += `
      ORDER BY export_date DESC
      LIMIT $${queryParams.length + 1}
      OFFSET $${queryParams.length + 2}
    `;
    queryParams.push(limit, offset);

    // Execute the query
    const { rows } = await client.query(query, queryParams);

    // Get total count for pagination metadata
    let countQuery = `
      SELECT COUNT(*) as total
      FROM exported_files
      WHERE user_id = $1
    `;
    const countParams = [userId];

    if (type) {
      countQuery += ` AND type = $${countParams.length + 1}`;
      countParams.push(type);
    }

    const countResult = await client.query(countQuery, countParams);
    const total = parseInt(countResult.rows[0].total, 10);

    // Prepare response
    const response = {
      data: rows,
      pagination: {
        total,
        page: parseInt(page, 10),
        limit: parseInt(limit, 10),
        totalPages: Math.ceil(total / limit),
      },
    };

    return successResponse(res, response);
  } catch (error) {
    console.error("Error fetching exported files:", error);
    return errorResponse(res, "Error fetching exported files", 500);
  } finally {
    client.release();
  }
};

const saveLeads = async (req, res) => {
  const client = await pool.connect();

  try {
    const userId = req.currentUser.id;
    const { leads, type } = req.body;

    if (!userId) {
      return errorResponse(res, "User ID is required", 400);
    }

    if (!Array.isArray(leads) || leads.length === 0) {
      return errorResponse(
        res,
        "Invalid input: expected a non-empty array of leads",
        400
      );
    }

    if (!type) {
      return errorResponse(res, "Type is required", 400);
    }

    // Validate all leads before processing
    for (const lead of leads) {
      const { leadId } = lead;
      if (!leadId) {
        return errorResponse(res, "leadId is required for each lead", 400);
      }
    }

    await client.query("BEGIN");

    // Process leads in chunks to maintain optimal performance
    const CHUNK_SIZE = 500; // Adjust based on your database capabilities
    let processedCount = 0;

    // Process in chunks for better performance with large datasets
    for (let i = 0; i < leads.length; i += CHUNK_SIZE) {
      const chunk = leads.slice(i, i + CHUNK_SIZE);

      const valuePlaceholders = [];
      const values = [];
      let placeholderIndex = 1;

      for (const lead of chunk) {
        const { leadId, email, mobile } = lead;

        valuePlaceholders.push(`($${placeholderIndex++}, $${placeholderIndex++}, $${placeholderIndex++}, $${placeholderIndex++}, $${placeholderIndex++})`);
        values.push(userId, leadId, type, email || null, mobile || null);
      }

      const batchQuery = `
        INSERT INTO saved_leads (user_id, lead_id, type, email, mobile)
        VALUES ${valuePlaceholders.join(', ')}
        ON CONFLICT (user_id, lead_id, type) 
        DO UPDATE SET 
          email = EXCLUDED.email,
          mobile = EXCLUDED.mobile
      `;

      await client.query(batchQuery, values);
      processedCount += chunk.length;
    }

    await client.query("COMMIT");

    return successResponse(res, {
      message: `${processedCount} leads saved successfully`,
      count: processedCount
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error saving leads:", error);
    return errorResponse(res, "Error saving leads", 500);
  } finally {
    client.release();
  }
};

const unsaveLeads = async (req, res) => {
  const client = await pool.connect();

  try {
    const userId = req.currentUser.id;
    const { leads, type } = req.body;

    if (!userId) {
      return errorResponse(res, "User ID is required", 400);
    }

    if (!Array.isArray(leads) || leads.length === 0) {
      return errorResponse(
        res,
        "Invalid input: expected a non-empty array of leads",
        400
      );
    }

    if (!type) {
      return errorResponse(res, "Type is required", 400);
    }

    for (const lead of leads) {
      const { leadId } = lead;
      if (!leadId) {
        return errorResponse(res, "leadId is required for each lead", 400);
      }
    }

    await client.query("BEGIN");

    const CHUNK_SIZE = 500;
    let processedCount = 0;

    for (let i = 0; i < leads.length; i += CHUNK_SIZE) {
      const chunk = leads.slice(i, i + CHUNK_SIZE);

      const leadIds = chunk.map(lead => lead.leadId);

      const paramPlaceholders = leadIds.map((_, index) => `$${index + 2}`).join(',');

      const batchQuery = `
        DELETE FROM saved_leads 
        WHERE user_id = $1 
        AND type = $${leadIds.length + 2}
        AND lead_id IN (${paramPlaceholders})
      `;

      const params = [userId, ...leadIds, type];

      await client.query(batchQuery, params);
      processedCount += (await client.query(batchQuery, params)).rowCount;
    }

    await client.query("COMMIT");

    return successResponse(res, {
      message: `leads unsaved successfully`
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error unsaving leads:", error);
    return errorResponse(res, "Error unsaving leads", 500);
  } finally {
    client.release();
  }
};

const getSavedLeads = async (req, res) => {
  const client = await pool.connect();

  try {
    const { type, page = 1, limit = 10 } = req.query;
    const userId = req.currentUser.id;

    if (!userId) {
      return errorResponse(res, "User ID is required", 400);
    }

    const offset = (page - 1) * limit;

    // Determine which table to join based on type
    const joinTable = type === 'company' ? 'companies' : 'peopleLeads';
    const tableAlias = type === 'company' ? 'c' : 'l';

    let query = `
      SELECT 
        sl.id AS saved_lead_id, 
        sl.type, 
        sl.email as "isEmail", 
        sl.mobile as "isMobile", 
        sl.saved_at,
        ${tableAlias}.* -- Select all columns from the appropriate table
      FROM saved_leads sl
      INNER JOIN ${joinTable} ${tableAlias} ON sl.lead_id = ${tableAlias}.id
      WHERE sl.user_id = $1
    `;
    const queryParams = [userId];

    if (type) {
      query += ` AND sl.type = $${queryParams.length + 1}`;
      queryParams.push(type);
    }

    query += `
      ORDER BY sl.saved_at DESC
      LIMIT $${queryParams.length + 1}
      OFFSET $${queryParams.length + 2}
    `;
    queryParams.push(limit, offset);

    // Execute the query
    const { rows } = await client.query(query, queryParams);

    // Get total count for pagination metadata
    let countQuery = `
      SELECT COUNT(*) as total
      FROM saved_leads sl
      WHERE sl.user_id = $1
    `;
    const countParams = [userId];

    if (type) {
      countQuery += ` AND sl.type = $${countParams.length + 1}`;
      countParams.push(type);
    }

    const countResult = await client.query(countQuery, countParams);
    const total = parseInt(countResult.rows[0].total, 10);

    // Prepare response
    const response = {
      data: rows,
      pagination: {
        total,
        page: parseInt(page, 10),
        limit: parseInt(limit, 10),
        totalPages: Math.ceil(total / limit),
      },
    };

    return successResponse(res, response);
  } catch (error) {
    console.error("Error fetching saved leads:", error);
    return errorResponse(res, "Error fetching saved leads", 500);
  } finally {
    client.release();
  }
};

const getselectedLeads = async (req, res) => {
  const client = await pool.connect();

  try {
    const { type, filters, rowSelection, percomponyContact, isSaved } = req.body;
    const user_id = req.currentUser.id;

    console.log("req.body>>", req.body)

    if (!type || (type !== 'people' && type !== 'company')) {
      return errorResponse(res, "Invalid type provided. Type must be 'people' or 'company'", 400);
    }

    // Check if isSaved is true - handle saved leads logic
    if (isSaved === true) {
      if (!user_id) {
        return errorResponse(res, "user_id is required when isSaved is true", 400);
      }

      await client.query("BEGIN");

      // Determine the saved leads type based on request type
      const savedLeadsType = type === 'people' ? 'leads' : 'company';
      const limit = rowSelection || 10; // Default to 10 if not specified

      const savedLeadsQuery = `
        SELECT lead_id as id 
        FROM saved_leads 
        WHERE user_id = $1 AND type = $2 
        ORDER BY saved_at DESC 
        LIMIT $3
      `;

      const { rows } = await client.query(savedLeadsQuery, [user_id, savedLeadsType, limit]);

      if (rows.length === 0) {
        await client.query("ROLLBACK");
        return errorResponse(res, "No saved leads found for this user", 404);
      }

      await client.query("COMMIT");

      // Extract IDs from the result
      const leadIds = rows.map(row => row.id);

      return successResponse(res, {
        leadIds,
        totalSelected: leadIds.length,
        message: `Selected ${leadIds.length} saved leads`,
        isSaved: true
      });
    }

    await client.query("BEGIN");

    let tableName;
    if (type === 'people') {
      tableName = 'peopleLeads pl LEFT JOIN companies c ON pl.company_linkedin_url = c.company_linkedin_url';
    } else if (type === 'company') {
      tableName = 'companies c';  // Add alias 'c' for companies table
    }

    // Build the WHERE clause with all filters
    let whereClause = "WHERE 1=1";
    let values = [];
    let index = 1;

    // Helper functions for case-insensitive include/exclude array filters
    const addIncludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) = LOWER($${index + i})`
        );
        whereClause += ` AND (${conditions.join(" OR ")})`;
        values.push(...valueArray);
        index += valueArray.length;
      }
    };

    const addExcludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) <> LOWER($${index + i})`
        );
        whereClause += ` AND (${conditions.join(" AND ")})`;
        values.push(...valueArray);
        index += valueArray.length;
      }
    };

    const addStringFilter = (field, value) => {
      if (value) {
        whereClause += ` AND ${field} ILIKE $${index}`;
        values.push(`%${value}%`);
        index++;
      }
    };

    // Apply filters based on type
    if (type === 'people') {
      const {
        includeIndustry,
        excludeIndustry,
        includeemployeeCount,
        includeRevenue,
        includemanagmentRole,
        includeCompany,
        excludeCompany,
        includedepartmentKeyword,
        includePersonalCountry,
        excludePersonalCountry,
        includecompanyLocation,
        excludeCompanyLocation,
        includejobTitles,
        excludeJobTitles,
        includetechnology,
        search,
        funding,
        foundingYear,
      } = filters;

      // Add search functionality
      if (search) {
        whereClause += ` AND (
          pl.company ILIKE $${index} OR 
          pl.first_name ILIKE $${index} OR 
          pl.last_name ILIKE $${index} OR 
          pl.email ILIKE $${index} OR
          pl.title ILIKE $${index} OR
          pl.industry ILIKE $${index}
        )`;
        values.push(`%${search}%`);
        index++;
      }

      // Handle industry filters
      addIncludeFilter("pl.industry", includeIndustry);
      addExcludeFilter("pl.industry", excludeIndustry);

      // Handle employee count filter
      if (includeemployeeCount && includeemployeeCount.length > 0) {
        for (const range of includeemployeeCount) {
          const [min, max] = range.split("-").map(Number);
          if (!isNaN(min) && !isNaN(max)) {
            whereClause += ` AND (pl.num_employees >= $${index} AND pl.num_employees <= $${index + 1})`;
            values.push(min, max);
            index += 2;
          } else if (!isNaN(min) && range.includes("+")) {
            whereClause += ` AND pl.num_employees >= $${index}`;
            values.push(min);
            index++;
          } else {
            // Handle specific values or other formats
            whereClause += ` AND pl.num_employees::text ILIKE $${index}`;
            values.push(`%${range}%`);
            index++;
          }
        }
      }

      // Handle revenue filter
      if (includeRevenue && includeRevenue.length > 0) {
        const revenueConditions = [];
        for (const range of includeRevenue) {
          const [min, max] = range.split("-").map((value) => {
            if (value.endsWith("M")) {
              return parseFloat(value) * 1000000;
            } else if (value.endsWith("B")) {
              return parseFloat(value) * 1000000000;
            } else {
              return parseFloat(value);
            }
          });

          if (!isNaN(min) && !isNaN(max)) {
            revenueConditions.push(
              `(pl.annual_revenue >= $${index} AND pl.annual_revenue <= $${index + 1})`
            );
            values.push(min, max);
            index += 2;
          } else if (!isNaN(min) && range.includes("+")) {
            revenueConditions.push(`(pl.annual_revenue >= $${index})`);
            values.push(min);
            index++;
          }
        }

        if (revenueConditions.length > 0) {
          whereClause += ` AND (` + revenueConditions.join(" OR ") + `)`;
        }
      }

      // Handle management role filter
      addIncludeFilter("pl.seniority", includemanagmentRole);

      // Handle company filter
      addIncludeFilter("pl.company", includeCompany);
      addExcludeFilter("pl.company", excludeCompany);

      // Handle department keyword filter
      addIncludeFilter("pl.departments", includedepartmentKeyword);

      // Handle job title filters with expanded matching
      if (includejobTitles && includejobTitles.length > 0) {
        const titlesToUse = typeof expandJobTitles === 'function' ? expandJobTitles(includejobTitles) : includejobTitles;

        whereClause += ` AND (`;
        const titleConditions = titlesToUse.map(
          (_, i) => `pl.title ILIKE $${index + i}`
        );
        whereClause += titleConditions.join(" OR ");
        whereClause += `)`;
        values.push(...titlesToUse.map(title => `%${title}%`));
        index += titlesToUse.length;
      }

      if (excludeJobTitles && excludeJobTitles.length > 0) {
        const titlesToExclude = typeof expandJobTitles === 'function' ? expandJobTitles(excludeJobTitles) : excludeJobTitles;

        whereClause += ` AND (`;
        const excludeTitleConditions = titlesToExclude.map(
          (_, i) => `pl.title NOT ILIKE $${index + i}`
        );
        whereClause += excludeTitleConditions.join(" AND ");
        whereClause += `)`;
        values.push(...titlesToExclude.map(title => `%${title}%`));
        index += titlesToExclude.length;
      }

      // Handle technology filter
      if (includetechnology && includetechnology.length > 0) {
        whereClause += ` AND (`;
        const techConditions = includetechnology.map(
          (_, i) => `pl.technologies ILIKE $${index + i}`
        );
        whereClause += techConditions.join(" OR ");
        whereClause += `)`;
        values.push(...includetechnology.map(tech => `%${tech}%`));
        index += includetechnology.length;
      }

      // Handle personal country filters
      addIncludeFilter("pl.country", includePersonalCountry);
      addExcludeFilter("pl.country", excludePersonalCountry);

      // Handle company location filters
      addIncludeFilter("pl.company_country", includecompanyLocation);
      addExcludeFilter("pl.company_country", excludeCompanyLocation);

      // Handle funding filter
      if (funding && funding.length > 0) {
        whereClause += ` AND (`;
        const fundingConditions = funding.map(
          (_, i) => `pl.latest_funding ILIKE $${index + i}`
        );
        whereClause += fundingConditions.join(" OR ");
        whereClause += `)`;
        values.push(...funding.map(fund => `%${fund}%`));
        index += funding.length;
      }

      // Handle founding year filter
      if (foundingYear && foundingYear.length > 0) {
        whereClause += ` AND (`;
        const yearConditions = foundingYear.map(
          (_, i) => `c.founded_year = $${index + i}`
        );
        whereClause += yearConditions.join(" OR ");
        whereClause += `)`;
        values.push(...foundingYear);
        index += foundingYear.length;
      }
    } else if (type === 'company') {
      const {
        employeeCount,
        companyRevenue,
        includeCompanyLocation,
        excludeCompanyLocation,
        includeIndustry,
        excludeIndustry,
        includeCompany,
        excludeCompany,
        includeTechnology,
        includeCompanyKeyword,
        search,
        funding,
        foundingYear,
      } = filters;

      // Add search functionality
      if (search) {
        whereClause += ` AND (
          company_name ILIKE $${index} OR 
          industry ILIKE $${index} OR 
          company_address ILIKE $${index} OR 
          company_phone ILIKE $${index} OR
          seo_description ILIKE $${index} OR
          technologies ILIKE $${index}
        )`;
        values.push(`%${search}%`);
        index++;
      }

      // Handle employee count filter
      if (employeeCount && employeeCount.length > 0) {
        for (const range of employeeCount) {
          const [min, max] = range.split("-").map(Number);
          if (!isNaN(min) && !isNaN(max)) {
            whereClause += ` AND (num_employees >= $${index} AND num_employees <= $${index + 1})`;
            values.push(min, max);
            index += 2;
          } else if (!isNaN(min) && range.includes("+")) {
            whereClause += ` AND num_employees >= $${index}`;
            values.push(min);
            index++;
          }
        }
      }

      // Handle company revenue filter
      if (companyRevenue && companyRevenue.length > 0) {
        const revenueConditions = [];
        for (const range of companyRevenue) {
          const [min, max] = range.split("-").map((value) => {
            if (value.endsWith("M")) {
              return parseFloat(value) * 1000000;
            } else if (value.endsWith("B")) {
              return parseFloat(value) * 1000000000;
            } else {
              return parseFloat(value);
            }
          });

          if (!isNaN(min) && !isNaN(max)) {
            revenueConditions.push(
              `(CAST(annual_revenue AS NUMERIC) >= $${index} AND CAST(annual_revenue AS NUMERIC) <= $${index + 1})`
            );
            values.push(min, max);
            index += 2;
          } else if (!isNaN(min) && range.includes("+")) {
            revenueConditions.push(
              `(CAST(annual_revenue AS NUMERIC) >= $${index})`
            );
            values.push(min);
            index++;
          }
        }

        if (revenueConditions.length > 0) {
          whereClause += ` AND (` + revenueConditions.join(" OR ") + `)`;
        }
      }

      // Handle company location filters
      addIncludeFilter("company_country", includeCompanyLocation);
      addExcludeFilter("company_country", excludeCompanyLocation);

      // Handle industry filters
      addIncludeFilter("industry", includeIndustry);
      addExcludeFilter("industry", excludeIndustry);

      // Handle company name filters
      addIncludeFilter("company_name", includeCompany);
      addExcludeFilter("company_name", excludeCompany);

      // Handle company keyword filter
      addStringFilter("company_name", includeCompanyKeyword);

      // Handle technology filters
      if (includeTechnology && includeTechnology.length > 0) {
        whereClause += ` AND (`;
        const techConditions = includeTechnology.map(
          (_, i) => `technologies ILIKE $${index + i}`
        );
        whereClause += techConditions.join(" OR ");
        whereClause += `)`;
        values.push(...includeTechnology.map(tech => `%${tech}%`));
        index += includeTechnology.length;
      }

      // Handle funding filter
      if (funding && funding.length > 0) {
        whereClause += ` AND (`;
        const fundingConditions = funding.map(
          (_, i) => `latest_funding ILIKE $${index + i}`
        );
        whereClause += fundingConditions.join(" OR ");
        whereClause += `)`;
        values.push(...funding.map(fund => `%${fund}%`));
        index += funding.length;
      }

      // Handle founding year filter
      if (foundingYear && foundingYear.length > 0) {
        whereClause += ` AND (`;
        const yearConditions = foundingYear.map(
          (_, i) => `founded_year = $${index + i}`
        );
        whereClause += yearConditions.join(" OR ");
        whereClause += `)`;
        values.push(...foundingYear);
        index += foundingYear.length;
      }
    }

    // Build final query based on rowSelection and percomponyContact logic
    let finalQuery;
    let finalValues = [...values];

    if (type === 'people' && percomponyContact && percomponyContact > 0) {
      // First get all filtered leads ordered by id DESC
      // Then apply per-company limit while maintaining the original order
      finalQuery = `
        WITH ordered_leads AS (
          SELECT pl.id, pl.company_linkedin_url
          FROM ${tableName}
          ${whereClause}
          ORDER BY pl.id DESC
        ),
        ranked_leads AS (
          SELECT 
            id,
            company_linkedin_url,
            ROW_NUMBER() OVER (PARTITION BY company_linkedin_url ORDER BY id DESC) as company_row_num
          FROM ordered_leads
        )
        SELECT id
        FROM ranked_leads
        WHERE company_row_num <= $${index}
        ORDER BY id DESC
        ${rowSelection ? `LIMIT $${index + 1}` : ''}
      `;

      finalValues.push(percomponyContact);
      if (rowSelection) {
        finalValues.push(rowSelection);
      }
    } else {
      // Simple case - just apply rowSelection limit with id DESC sorting
      const limit = rowSelection || 1000; // Default limit if not specified

      let sorting = type == 'people' ? 'pl.id' : 'c.id'

      finalQuery = `
        SELECT ${sorting} 
        FROM ${tableName}
        ${whereClause}
        ORDER BY ${sorting} desc
        LIMIT $${index}
      `;
      finalValues.push(limit);
    }

    console.log("finalQuery>>", finalQuery);
    console.log("finalValues>>", finalValues);

    const { rows } = await client.query(finalQuery, finalValues);

    if (rows.length === 0) {
      await client.query("ROLLBACK");
      return errorResponse(res, "No leads found with the provided filters", 404);
    }

    await client.query("COMMIT");

    // Extract IDs from the result
    const leadIds = rows.map(row => row.id);

    return successResponse(res, {
      leadIds,
      totalSelected: leadIds.length,
      message: `Selected ${leadIds.length} leads${percomponyContact ? ` (max ${percomponyContact} per company)` : ''}`,
      isSaved: false
    });

  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error fetching selected leads:", error);
    return errorResponse(res, "Error fetching selected leads", 500);
  } finally {
    client.release();
  }
};

const deletePeopleLeads = async (req, res) => {
  const client = await pool.connect();
  
  try {
    const { ids } = req.body;

    if (!Array.isArray(ids) || ids.length === 0) {
      return errorResponse(res, "Valid array of IDs is required", 400);
    }

    await client.query("BEGIN");

    // First delete from saved_leads table to maintain referential integrity
    await client.query(
      `DELETE FROM saved_leads WHERE lead_id = ANY($1) AND type = 'leads'`,
      [ids]
    );

    // Then delete from peopleLeads table
    const { rowCount } = await client.query(
      `DELETE FROM peopleLeads WHERE id = ANY($1)`,
      [ids]
    );

    await client.query("COMMIT");

    return successResponse(res, {
      message: `${rowCount} people leads deleted successfully`,
      count: rowCount
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error deleting people leads:", error);
    return errorResponse(res, "Error deleting people leads", 500);
  } finally {
    client.release();
  }
};

const deleteCompanies = async (req, res) => {
  const client = await pool.connect();
  
  try {
    const { ids } = req.body;

    if (!Array.isArray(ids) || ids.length === 0) {
      return errorResponse(res, "Valid array of IDs is required", 400);
    }

    await client.query("BEGIN");

    // First delete from saved_leads table to maintain referential integrity
    await client.query(
      `DELETE FROM saved_leads WHERE lead_id = ANY($1) AND type = 'company'`,
      [ids]
    );

    // Then delete from companies table
    const { rowCount } = await client.query(
      `DELETE FROM companies WHERE id = ANY($1)`,
      [ids]
    );

    await client.query("COMMIT");

    return successResponse(res, {
      message: `${rowCount} companies deleted successfully`,
      count: rowCount
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error deleting companies:", error);
    return errorResponse(res, "Error deleting companies", 500);
  } finally {
    client.release();
  }
};


export {
  addPeopleLeadsData,
  getPeopleLeads,
  exportPeopleLeadsToCSV,
  deductCreditsFromUser,
  addCompaniesData,
  getCompanies,
  exportCompaniesToCSV,
  editPeopleLeadsData,
  editCompanyLeadData,
  getCompanyChartData,
  getPeopleLeadsDepartmentChartData,
  getExportedFiles,
  saveLeads,
  unsaveLeads,
  getSavedLeads,
  getselectedLeads,
  deletePeopleLeads,
  deleteCompanies
};

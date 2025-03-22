import csvParser from "csv-parser";
import fs from "fs";
import { Readable } from "stream";
import pool from "../config/db.js";
import {
  successResponse,
  errorResponse,
  sendCSVEmail,
} from "../utils/index.js";
import pkg from "pg-copy-streams";
const { from } = pkg;
import { deductCredits } from "../servies/userService.js";
import { uploadFileToS3 } from "../utils/index.js";

const cleanPhoneNumber = (phone) => {
  if (!phone) return null;
  return phone.replace(/[^\d+]/g, "");
};

const processAndInsertLeads = async (results) => {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Create a temporary table to stage the data
    await client.query(
      "CREATE TEMP TABLE temp_leads (LIKE peopleLeads INCLUDING ALL) ON COMMIT DROP"
    );

    // Prepare the data for COPY
    const data = results
      .map((row) => {
        const values = [
          row["First Name"],
          row["Last Name"],
          row["Title"],
          row["Company"],
          row["Email"],
          row["Email Status"],
          row["Seniority"],
          row["Departments"],
          cleanPhoneNumber(row["Work Direct Phone"]),
          cleanPhoneNumber(row["Mobile Phone"]),
          cleanPhoneNumber(row["Corporate Phone"]),
          row["# Employees"],
          row["Industry"],
          row["Keywords"],
          row["Person Linkedin Url"],
          row["Website"],
          row["Company Linkedin Url"],
          row["Facebook Url"],
          row["Twitter Url"],
          row["City"],
          row["State"],
          row["Country"],
          row["Company Address"],
          row["Company City"],
          row["Company State"],
          row["Company Country"],
          row["SEO Description"],
          row["Technologies"],
          row["Annual Revenue"],
          row["Total Funding"],
          row["Latest Funding"],
          row["Latest Funding Amount"],
          row["Last Raised At"],
          row["Number of Retail Locations"],
        ]
          .map((val) =>
            val === null || val === undefined || val === ""
              ? "\\N"
              : val.toString().replace(/[\t\n\\]/g, " ")
          )
          .join("\t");

        return values;
      })
      .join("\n");

    // Use COPY to insert data into the temporary table
    const copyStream = client.query(
      from(`COPY temp_leads (
            first_name, last_name, title, company, email, email_status, seniority, departments, work_direct_phone,
            mobile_phone, corporate_phone, num_employees, industry, keywords, linkedin_url, website, company_linkedin_url,
            facebook_url, twitter_url, city, state, country, company_address, company_city, company_state, company_country,
            seo_description, technologies, annual_revenue, total_funding, latest_funding, latest_funding_amount,
            last_raised_at, num_retail_locations
        ) FROM STDIN WITH (FORMAT text, NULL '\\N')`)
    );

    const readable = new Readable();
    readable.push(data);
    readable.push(null);
    await new Promise((resolve, reject) => {
      readable.pipe(copyStream).on("finish", resolve).on("error", reject);
    });

    // Update or insert company data based on company_linkedin_url
    await client.query(`
      -- Deduplicate the data in the temp_leads table
      CREATE TEMP TABLE deduplicated_temp_leads AS
      SELECT DISTINCT ON (company_linkedin_url) *
      FROM temp_leads
      WHERE company_linkedin_url IS NOT NULL
      ORDER BY company_linkedin_url, id; -- Use a unique field like 'id' to determine which row to keep
    `);

    await client.query(`
      INSERT INTO companies (
        company_name, num_employees, industry, website, company_linkedin_url, facebook_url, twitter_url,
        company_street, company_city, company_state, company_country, company_postal_code, company_address,
        keywords, company_phone, seo_description, technologies, total_funding, latest_funding, latest_funding_amount,
        last_raised_at, annual_revenue, num_retail_locations, sic_codes, short_description, founded_year
      )
      SELECT 
        company, num_employees::integer, industry, website, company_linkedin_url, facebook_url, twitter_url,
        company_address, company_city, company_state, company_country, NULL, company_address,
        keywords, NULL, seo_description, technologies, total_funding, latest_funding, latest_funding_amount,
        last_raised_at::date, annual_revenue, num_retail_locations::integer, NULL, NULL, NULL
      FROM deduplicated_temp_leads
      ON CONFLICT (company_linkedin_url) DO UPDATE SET
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
        company_address = EXCLUDED.company_address,
        keywords = EXCLUDED.keywords,
        seo_description = EXCLUDED.seo_description,
        technologies = EXCLUDED.technologies,
        total_funding = EXCLUDED.total_funding,
        latest_funding = EXCLUDED.latest_funding,
        latest_funding_amount = EXCLUDED.latest_funding_amount,
        last_raised_at = EXCLUDED.last_raised_at,
        annual_revenue = EXCLUDED.annual_revenue,
        num_retail_locations = EXCLUDED.num_retail_locations,
        updated_at = CURRENT_TIMESTAMP
    `);

    // Insert or update people leads data based on linkedin_url
    await client.query(`
      INSERT INTO peopleLeads (
        first_name, last_name, title, company, email, email_status, seniority, departments, work_direct_phone,
        mobile_phone, corporate_phone, num_employees, industry, keywords, linkedin_url, website, company_linkedin_url,
        facebook_url, twitter_url, city, state, country, company_address, company_city, company_state, company_country,
        seo_description, technologies, annual_revenue, total_funding, latest_funding, latest_funding_amount,
        last_raised_at, num_retail_locations
      )
      SELECT 
        first_name, last_name, title, company, email, email_status, seniority, departments, work_direct_phone,
        mobile_phone, corporate_phone, num_employees::integer, industry, keywords, linkedin_url, website, company_linkedin_url,
        facebook_url, twitter_url, city, state, country, company_address, company_city, company_state, company_country,
        seo_description, technologies, annual_revenue, total_funding, latest_funding, latest_funding_amount,
        last_raised_at::date, num_retail_locations::integer
      FROM temp_leads
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
    `);

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
};

const addPeopleLeadsData = async (req, res) => {
  if (!req.file) return errorResponse(res, "CSV file is required");

  const filePath = req.file.path;
  const results = [];

  fs.createReadStream(filePath)
    .pipe(csvParser())
    .on("data", (data) => results.push(data))
    .on("end", async () => {
      try {
        const startTime = Date.now();
        await processAndInsertLeads(results);
        const endTime = Date.now();
        const timeTaken = (endTime - startTime) / 1000;
        fs.unlinkSync(filePath);
        return successResponse(res, {
          message: "Data inserted or updated successfully",
          count: results.length,
          time_taken: `${timeTaken} seconds`,
        });
      } catch (error) {
        fs.unlinkSync(filePath);
        console.error("Error processing data:", error);
        return errorResponse(res, error.message, 500);
      }
    });
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

    const query = `UPDATE peopleLeads SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $${
      keys.length + 1
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
      perCompany = null,
      funding = null,
      foundingYear = null, // Add foundingYear filter (array of years)
    } = req.body;

    let baseQuery = `
      SELECT pl.*, COUNT(*) OVER() AS total_count 
      FROM peopleLeads pl
      LEFT JOIN companies c ON pl.company_linkedin_url = c.company_linkedin_url
      WHERE 1=1
    `;
    let values = [];
    let index = 1;

    // Helper functions for case-insensitive include/exclude array filters
    const addIncludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) = LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" OR ")})`;
        values.push(...valueArray);
        index += valueArray.length;
      }
    };

    const addExcludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) <> LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" AND ")})`;
        values.push(...valueArray);
        index += valueArray.length;
      }
    };

    const addStringFilter = (field, value) => {
      if (value) {
        baseQuery += ` AND ${field} ILIKE $${index}`;
        values.push(`%${value}%`);
        index++;
      }
    };

    // Add search functionality
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

    // Handle industry filters
    addIncludeFilter("pl.industry", includeIndustry);
    addExcludeFilter("pl.industry", excludeIndustry);

    // Handle employee count filter - map to num_employees
    if (includeemployeeCount && includeemployeeCount.length > 0) {
      for (const range of includeemployeeCount) {
        const [min, max] = range.split("-").map(Number);
        if (!isNaN(min) && !isNaN(max)) {
          baseQuery += ` AND (pl.num_employees >= $${index} AND pl.num_employees <= $${index + 1})`;
          values.push(min, max);
          index += 2;
        } else if (!isNaN(min) && range.includes("+")) {
          baseQuery += ` AND pl.num_employees >= $${index}`;
          values.push(min);
          index++;
        } else {
          // Handle specific values or other formats
          addStringFilter("pl.num_employees::text", range);
        }
      }
    }

    // Handle revenue filter - map to annual_revenue
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
        baseQuery += ` AND (` + revenueConditions.join(" OR ") + `)`;
      }
    }

    // Handle management role filter - map to seniority
    addIncludeFilter("pl.seniority", includemanagmentRole);

    // Handle company filter
    addIncludeFilter("pl.company", includeCompany);
    addExcludeFilter("pl.company", excludeCompany);

    // Handle department keyword filter
    addIncludeFilter("pl.departments", includedepartmentKeyword);

    // Handle job title filters - map to title
    if (includejobTitles && includejobTitles.length > 0) {
      baseQuery += ` AND (`;
      const titleConditions = includejobTitles.map(
        (_, i) => `pl.title ILIKE $${index + i}`
      );
      baseQuery += titleConditions.join(" OR ");
      baseQuery += `)`;

      includejobTitles.forEach((title) => {
        values.push(`%${title}%`);
      });
      index += includejobTitles.length;
    }

    if (excludeJobTitles && excludeJobTitles.length > 0) {
      baseQuery += ` AND (`;
      const excludeTitleConditions = excludeJobTitles.map(
        (_, i) => `pl.title NOT ILIKE $${index + i}`
      );
      baseQuery += excludeTitleConditions.join(" AND ");
      baseQuery += `)`;

      excludeJobTitles.forEach((title) => {
        values.push(`%${title}%`);
      });
      index += excludeJobTitles.length;
    }

    // Handle technology filter
    if (includetechnology && includetechnology.length > 0) {
      baseQuery += ` AND (`;
      const techConditions = includetechnology.map(
        (_, i) => `pl.technologies ILIKE $${index + i}`
      );
      baseQuery += techConditions.join(" OR ");
      baseQuery += `)`;

      includetechnology.forEach((tech) => {
        values.push(`%${tech}%`);
      });
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
      baseQuery += ` AND (`;
      const fundingConditions = funding.map(
        (_, i) => `pl.latest_funding ILIKE $${index + i}`
      );
      baseQuery += fundingConditions.join(" OR ");
      baseQuery += `)`;

      funding.forEach((fund) => {
        values.push(`%${fund}%`);
      });
      index += funding.length;
    }

    // Handle founding year filter (multiple values)
    if (foundingYear && foundingYear.length > 0) {
      baseQuery += ` AND (`;
      const yearConditions = foundingYear.map(
        (_, i) => `c.founded_year = $${index + i}`
      );
      baseQuery += yearConditions.join(" OR ");
      baseQuery += `)`;

      foundingYear.forEach((year) => {
        values.push(year);
      });
      index += foundingYear.length;
    }

    // Pagination
    const offset = (page - 1) * limit;

    let finalQuery;

    if (perCompany && perCompany > 0) {
      // If perCompany is specified, use a window function to limit records per company
      finalQuery = `
        WITH filtered_leads AS (
          ${baseQuery}
        ),
        company_grouped AS (
          SELECT 
            fl.*,
            ROW_NUMBER() OVER (PARTITION BY fl.company_linkedin_url ORDER BY fl.id) as row_num
          FROM 
            filtered_leads fl
        )
        SELECT * FROM company_grouped 
        WHERE row_num <= $${index}
        ORDER BY company_linkedin_url, row_num
        LIMIT $${index + 1} OFFSET $${index + 2}
      `;
      values.push(perCompany); // Add perCompany value
      values.push(limit); // Add limit value
      values.push(offset); // Add offset value
    } else {
      // If perCompany is not specified, use the base query with pagination
      finalQuery = `
        ${baseQuery}
        LIMIT $${index} OFFSET $${index + 1}
      `;
      values.push(limit); // Add limit value
      values.push(offset); // Add offset value
    }

    console.log("finalQuery>", finalQuery);
    console.log("values>", values);

    const { rows } = await pool.query(finalQuery, values);
    const totalCount = rows.length > 0 ? parseInt(rows[0].total_count, 10) : 0;
    const perPageCount = rows.length;

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
      limit = 1000, // Default limit is 1000
      perCompany = null, // Parameter to limit records per company
      funding = null, // Add funding filter
      foundingYear = null, // Add foundingYear filter (array of years)
    } = req.body;

    console.log("perCompany", perCompany);
    const userId = req.currentUser.id;
    const userEmail = req.currentUser.email;
    const userRole = req.currentUser.roleName;

    if (!userId || !userEmail)
      return errorResponse(res, "User information not found", 400);

    await client.query("BEGIN");

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

    // Add search functionality
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

    // Handle industry filters
    addIncludeFilter("industry", includeIndustry);
    addExcludeFilter("industry", excludeIndustry);

    // Handle employee count filter - map to num_employees
    if (includeemployeeCount && includeemployeeCount.length > 0) {
      const employeeRanges = [];

      for (const range of includeemployeeCount) {
        const [min, max] = range.split("-").map(Number);
        if (!isNaN(min) && !isNaN(max)) {
          whereClause += ` AND (num_employees >= $${index} AND num_employees <= $${
            index + 1
          })`;
          values.push(min, max);
          index += 2;
        } else if (!isNaN(min) && range.includes("+")) {
          whereClause += ` AND num_employees >= $${index}`;
          values.push(min);
          index++;
        } else {
          // Handle specific values or other formats
          addStringFilter("num_employees::text", range);
        }
      }
    }

    // Handle revenue filter - map to annual_revenue
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
            `(annual_revenue >= $${index} AND annual_revenue <= $${index + 1})`
          );
          values.push(min, max);
          index += 2;
        } else if (!isNaN(min) && range.includes("+")) {
          revenueConditions.push(`(annual_revenue >= $${index})`);
          values.push(min);
          index++;
        }
      }

      if (revenueConditions.length > 0) {
        whereClause += ` AND (` + revenueConditions.join(" OR ") + `)`;
      }
    }

    // Handle management role filter - map to seniority
    addIncludeFilter("seniority", includemanagmentRole);

    // Handle company filter
    addIncludeFilter("company", includeCompany);
    addExcludeFilter("company", excludeCompany);

    // Handle department keyword filter
    addIncludeFilter("departments", includedepartmentKeyword);

    // Handle job title filters - map to title
    if (includejobTitles && includejobTitles.length > 0) {
      whereClause += ` AND (`;
      const titleConditions = includejobTitles.map(
        (_, i) => `title ILIKE $${index + i}`
      );
      whereClause += titleConditions.join(" OR ");
      whereClause += `)`;

      includejobTitles.forEach((title) => {
        values.push(`%${title}%`);
      });
      index += includejobTitles.length;
    }

    if (excludeJobTitles && excludeJobTitles.length > 0) {
      whereClause += ` AND (`;
      const excludeTitleConditions = excludeJobTitles.map(
        (_, i) => `title NOT ILIKE $${index + i}`
      );
      whereClause += excludeTitleConditions.join(" AND ");
      whereClause += `)`;

      excludeJobTitles.forEach((title) => {
        values.push(`%${title}%`);
      });
      index += excludeJobTitles.length;
    }

    // Handle technology filter
    if (includetechnology && includetechnology.length > 0) {
      whereClause += ` AND (`;
      const techConditions = includetechnology.map(
        (_, i) => `technologies ILIKE $${index + i}`
      );
      whereClause += techConditions.join(" OR ");
      whereClause += `)`;

      includetechnology.forEach((tech) => {
        values.push(`%${tech}%`);
      });
      index += includetechnology.length;
    }

    // Handle personal country filters
    addIncludeFilter("country", includePersonalCountry);
    addExcludeFilter("country", excludePersonalCountry);

    // Handle company location filters
    addIncludeFilter("company_country", includecompanyLocation);
    addExcludeFilter("company_country", excludeCompanyLocation);

    // Handle funding filter
    if (funding && funding.length > 0) {
      whereClause += ` AND (`;
      const fundingConditions = funding.map(
        (_, i) => `latest_funding ILIKE $${index + i}`
      );
      whereClause += fundingConditions.join(" OR ");
      whereClause += `)`;

      funding.forEach((fund) => {
        values.push(`%${fund}%`);
      });
      index += funding.length;
    }

    // Handle founding year filter (multiple values)
    if (foundingYear && foundingYear.length > 0) {
      whereClause += ` AND (`;
      const yearConditions = foundingYear.map(
        (_, i) => `EXTRACT(YEAR FROM last_raised_at) = $${index + i}`
      );
      whereClause += yearConditions.join(" OR ");
      whereClause += `)`;

      foundingYear.forEach((year) => {
        values.push(year);
      });
      index += foundingYear.length;
    }

    let finalQuery;

    if (perCompany && perCompany > 0) {
      // If perCompany is specified, use a window function to limit records per company
      finalQuery = `
        WITH filtered_leads AS (
          SELECT pl.*
          FROM peopleLeads pl
          ${whereClause}
        ),
        limited_leads AS (
          SELECT * FROM filtered_leads
          LIMIT $${index}
        ),
        company_grouped AS (
          SELECT 
            fl.*,
            ROW_NUMBER() OVER (PARTITION BY fl.company_linkedin_url ORDER BY fl.id) as row_num
          FROM 
            limited_leads fl
        )
        SELECT * FROM company_grouped 
        WHERE row_num <= $${index + 1}
        ORDER BY company_linkedin_url, row_num
      `;
      values.push(limit); // Add limit value
      index++;
      values.push(perCompany); // Add perCompany value
    } else {
      // If perCompany is not specified, use a simpler query
      finalQuery = `
        SELECT pl.* 
        FROM peopleLeads pl
        ${whereClause}
        LIMIT $${index}
      `;
      values.push(limit); // Add limit value
    }

    console.log("finalQuery>", finalQuery);
    console.log("values>", values);

    const { rows } = await client.query(finalQuery, values);

    if (rows.length === 0) {
      await client.query("ROLLBACK");
      return errorResponse(res, "No data found to export", 404);
    }

    if (userRole !== "admin") {
      // Deduct credits only if the user is NOT an admin
      const creditsToDeduct = rows.length;
      const deductionResult = await deductCredits(userId, creditsToDeduct);

      if (!deductionResult.success) {
        await client.query("ROLLBACK");
        return errorResponse(res, "Insufficient credits to export leads", 403);
      }
    }

    // Generate CSV
    const cleanRows = rows.map((row) => {
      const cleanRow = { ...row };
      if ("row_num" in cleanRow) {
        delete cleanRow.row_num;
      }
      return cleanRow;
    });

    const csvHeader = Object.keys(cleanRows[0]).join(",") + "\n";

    const csvRows = cleanRows
      .map((row) =>
        Object.values(row)
          .map((value) => {
            if (value === null || value === undefined) return "";
            if (typeof value === "string") {
              const escaped = value.replace(/"/g, '""');
              return value.includes(",") ||
                value.includes('"') ||
                value.includes("\n")
                ? `"${escaped}"`
                : escaped;
            }
            return value;
          })
          .join(",")
      )
      .join("\n");

    const csvData = csvHeader + csvRows;

    // Upload CSV to S3
    const fileName = `people_leads_export_${Date.now()}.csv`;
    const bucketName = process.env.S3_BUCKET_NAME;
    const { fileUrl } = await uploadFileToS3(
      Buffer.from(csvData, "utf-8"),
      fileName,
      bucketName
    );

    // Prepare filters object
    const filters = {
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

    // Insert into exported_files with filters
    const insertQuery = `
      INSERT INTO exported_files (user_id, type, export_row_count, file_name, file_url, filters)
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id;
    `;
    const insertValues = [
      userId,
      "leads",
      rows.length,
      fileName,
      fileUrl,
      filters,
    ];
    await client.query(insertQuery, insertValues);

    if (rows.length > 1000) {
      // Send CSV via email
      await sendCSVEmail(userEmail, csvData);
      await client.query("COMMIT");

      return successResponse(res, {
        message: `CSV file has been sent to your email.`,
        remaining_credits:
          userRole !== "admin" ? deductionResult.remainingCredits : "N/A",
      });
    } else {
      // Send CSV as downloadable response
      res.setHeader("Content-Type", "text/csv");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${fileName}"`
      );

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
    console.log("Transaction started");

    // Create a temporary table to stage the data
    await client.query(
      "CREATE TEMP TABLE temp_companies (LIKE companies INCLUDING ALL) ON COMMIT DROP"
    );
    console.log("Temporary table created");

    // Prepare the data for COPY
    console.log("Preparing data for COPY operation");
    const data = results
      .map((row, index) => {
        // Debug problematic rows, especially around line 16
        if (index === 15 || index === 16 || index === 17) {
          // Lines 16, 17, 18 (0-indexed)
          console.log(`Debugging row ${index + 1}:`, JSON.stringify(row));
        }

        const values = [
          row["Company"],
          row["# Employees"],
          row["Industry"],
          row["Website"],
          row["Company Linkedin Url"],
          row["Facebook Url"],
          row["Twitter Url"],
          row["Company Street"],
          row["Company City"],
          row["Company State"],
          row["Company Country"],
          row["Company Postal Code"],
          row["Company Address"],
          row["Keywords"],
          cleanPhoneNumber(row["Company Phone"]), // Clean the phone number
          row["SEO Description"],
          row["Technologies"],
          row["Total Funding"],
          row["Latest Funding"],
          row["Latest Funding Amount"],
          row["Last Raised At"],
          row["Annual Revenue"],
          row["Number of Retail Locations"],
          row["SIC Codes"],
          row["Short Description"],
          row["Founded Year"],
        ]
          .map((val, colIndex) => {
            if (val === null || val === undefined || val === "") {
              return "\\N";
            } else {
              // Properly escape special characters including carriage returns
              const escaped = val
                .toString()
                .replace(/\\/g, "\\\\") // Escape backslashes first
                .replace(/\r/g, "\\r") // Escape carriage returns
                .replace(/\n/g, "\\n") // Escape newlines
                .replace(/\t/g, "\\t"); // Escape tabs

              // Debug specific problematic values
              if (
                (index === 15 || index === 16 || index === 17) &&
                escaped !== val.toString()
              ) {
                console.log(
                  `Row ${index + 1}, Column ${
                    colIndex + 1
                  } needed escaping: ${JSON.stringify(val)} -> ${JSON.stringify(
                    escaped
                  )}`
                );
              }
              return escaped;
            }
          })
          .join("\t");

        return values;
      })
      .join("\n");

    console.log("Data preparation complete");

    // For debugging - check a small section of the prepared data
    console.log("Sample of prepared data:", data.substring(0, 200) + "...");

    // Use COPY to insert data into the temporary table
    console.log("Starting COPY operation");
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
    readable.push(data);
    readable.push(null);
    await new Promise((resolve, reject) => {
      readable
        .pipe(copyStream)
        .on("finish", () => {
          console.log("COPY operation completed successfully");
          resolve();
        })
        .on("error", (err) => {
          console.error("Error during COPY operation:", err);
          reject(err);
        });
    });

    // Insert data from the temporary table into the main table, avoiding duplicates
    console.log("Inserting data from temporary table to main table");
    const insertResult = await client.query(`
            INSERT INTO companies (
                company_name, num_employees, industry, website, company_linkedin_url, facebook_url, twitter_url,
                company_street, company_city, company_state, company_country, company_postal_code, company_address,
                keywords, company_phone, seo_description, technologies, total_funding, latest_funding,
                latest_funding_amount, last_raised_at, annual_revenue, num_retail_locations, sic_codes,
                short_description, founded_year
            )
            SELECT 
                company_name, num_employees::integer, industry, website, company_linkedin_url, facebook_url, twitter_url,
                company_street, company_city, company_state, company_country, company_postal_code, company_address,
                keywords, company_phone, seo_description, technologies, total_funding, latest_funding,
                latest_funding_amount::numeric, last_raised_at::date, annual_revenue::numeric, num_retail_locations::integer,
                sic_codes, short_description, founded_year::integer
            FROM temp_companies
            WHERE NOT EXISTS (
                SELECT 1 FROM companies
                WHERE companies.company_name = temp_companies.company_name
                AND companies.company_address = temp_companies.company_address
            )
        `);

    console.log(
      `Inserted ${insertResult.rowCount} new records to companies table`
    );

    await client.query("COMMIT");
    console.log("Transaction committed successfully");
    return insertResult.rowCount;
  } catch (error) {
    console.error("Error in processAndInsertCompanies:", error);
    await client.query("ROLLBACK");
    console.log("Transaction rolled back due to error");
    throw error;
  } finally {
    client.release();
    console.log("Database client released");
  }
};

const addCompaniesData = (req, res) => {
  console.log("Starting addCompaniesData process");
  if (!req.file) {
    console.log("No file provided in request");
    return res.status(400).json({ error: "CSV file is required" });
  }

  const filePath = req.file.path;
  console.log(`Processing CSV file: ${filePath}`);
  const results = [];

  fs.createReadStream(filePath)
    .pipe(csvParser())
    .on("data", (data) => {
      results.push(data);
      if (results.length % 100 === 0) {
        console.log(`Processed ${results.length} rows from CSV`);
      }
    })
    .on("end", async () => {
      console.log(`CSV parsing complete. Total rows: ${results.length}`);
      try {
        const startTime = Date.now();
        console.log(
          `Starting database insertion at ${new Date(startTime).toISOString()}`
        );
        const insertedCount = await processAndInsertCompanies(results);
        const endTime = Date.now();
        const timeTaken = (endTime - startTime) / 1000;
        console.log(`Database insertion completed in ${timeTaken} seconds`);

        fs.unlinkSync(filePath);
        console.log(`Temporary file ${filePath} deleted`);

        return res.status(200).json({
          message: "Data inserted or updated successfully",
          total_rows: results.length,
          inserted_rows: insertedCount,
          time_taken: `${timeTaken} seconds`,
        });
      } catch (error) {
        console.error("Error during data processing:", error);
        try {
          fs.unlinkSync(filePath);
          console.log(`Temporary file ${filePath} deleted after error`);
        } catch (unlinkError) {
          console.error("Error deleting temporary file:", unlinkError);
        }
        return res.status(500).json({
          error: "Error processing data",
          message: error.message,
          details: error.detail || error.hint || null,
        });
      }
    })
    .on("error", (error) => {
      console.error("Error parsing CSV:", error);
      return res.status(500).json({
        error: "Error parsing CSV file",
        message: error.message,
      });
    });
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
      funding = null, // Add funding filter
      foundingYear = null, // Add foundingYear filter (array of years)
    } = req.body;

    let baseQuery = `SELECT *, COUNT(*) OVER() AS total_count FROM companies WHERE 1=1`;
    let values = [];
    let index = 1;

    // Helper functions for case-insensitive include/exclude array filters
    const addIncludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) = LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" OR ")})`;
        values.push(...valueArray);
        index += valueArray.length;
      }
    };

    const addExcludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) <> LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" AND ")})`;
        values.push(...valueArray);
        index += valueArray.length;
      }
    };

    const addStringFilter = (field, value) => {
      if (value) {
        baseQuery += ` AND ${field} ILIKE $${index}`;
        values.push(`%${value}%`);
        index++;
      }
    };

    // Add search functionality
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

    // Handle employee count filter
    if (employeeCount && employeeCount.length > 0) {
      const employeeRanges = [];

      for (const range of employeeCount) {
        const [min, max] = range.split("-").map(Number);
        if (!isNaN(min) && !isNaN(max)) {
          baseQuery += ` AND (num_employees >= $${index} AND num_employees <= $${
            index + 1
          })`;
          values.push(min, max);
          index += 2;
        } else if (!isNaN(min) && range.includes("+")) {
          baseQuery += ` AND num_employees >= $${index}`;
          values.push(min);
          index++;
        } else {
          // Handle specific values or other formats
          addStringFilter("num_employees::text", range);
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
            `(CAST(annual_revenue AS NUMERIC) >= $${index} AND CAST(annual_revenue AS NUMERIC) <= $${
              index + 1
            })`
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
        baseQuery += ` AND (` + revenueConditions.join(" OR ") + `)`;
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

    // Handle technology filters
    if (includeTechnology && includeTechnology.length > 0) {
      baseQuery += ` AND (`;
      const techConditions = includeTechnology.map(
        (_, i) => `technologies ILIKE $${index + i}`
      );
      baseQuery += techConditions.join(" OR ");
      baseQuery += `)`;

      includeTechnology.forEach((tech) => {
        values.push(`%${tech}%`);
      });
      index += includeTechnology.length;
    }

    // Handle company keyword filter
    addStringFilter("company_name", includeCompanyKeyword);

    // Handle funding filter
    if (funding && funding.length > 0) {
      baseQuery += ` AND (`;
      const fundingConditions = funding.map(
        (_, i) => `latest_funding ILIKE $${index + i}`
      );
      baseQuery += fundingConditions.join(" OR ");
      baseQuery += `)`;

      funding.forEach((fund) => {
        values.push(`%${fund}%`);
      });
      index += funding.length;
    }

    // Handle founding year filter (multiple values)
    if (foundingYear && foundingYear.length > 0) {
      baseQuery += ` AND (`;
      const yearConditions = foundingYear.map(
        (_, i) => `founded_year = $${index + i}`
      );
      baseQuery += yearConditions.join(" OR ");
      baseQuery += `)`;

      foundingYear.forEach((year) => {
        values.push(year);
      });
      index += foundingYear.length;
    }

    // Pagination
    const offset = (page - 1) * limit;
    baseQuery += ` ORDER BY id ASC LIMIT $${index++} OFFSET $${index++}`;
    values.push(limit, offset);

    console.log("baseQuery>", baseQuery);
    console.log("values>", values);

    const { rows } = await pool.query(baseQuery, values);
    const totalCount = rows.length > 0 ? parseInt(rows[0].total_count, 10) : 0;
    const perPageCount = rows.length;

    return res.status(200).json({
      message: "Data fetched successfully",
      total_count: totalCount,
      per_page_count: perPageCount,
      data: rows,
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
      limit = 1000, // Default limit is 1000
      funding = null, // Add funding filter
      foundingYear = null, // Add foundingYear filter (array of years)
    } = req.body;

    const userId = req.currentUser.id;
    const userEmail = req.currentUser.email;
    const userRole = req.currentUser.roleName; // Get user role

    if (!userId || !userEmail)
      return res.status(400).json({ error: "User information not found" });

    await client.query("BEGIN");

    let baseQuery = `SELECT * FROM companies WHERE 1=1`;
    let values = [];
    let index = 1;

    // Helper functions for case-insensitive include/exclude array filters
    const addIncludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) = LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" OR ")})`;
        values.push(...valueArray);
        index += valueArray.length;
      }
    };

    const addExcludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) <> LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" AND ")})`;
        values.push(...valueArray);
        index += valueArray.length;
      }
    };

    const addStringFilter = (field, value) => {
      if (value) {
        baseQuery += ` AND ${field} ILIKE $${index}`;
        values.push(`%${value}%`);
        index++;
      }
    };

    // Add search functionality
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

    // Handle employee count filter
    if (employeeCount && employeeCount.length > 0) {
      for (const range of employeeCount) {
        const [min, max] = range.split("-").map(Number);
        if (!isNaN(min) && !isNaN(max)) {
          baseQuery += ` AND (num_employees >= $${index} AND num_employees <= $${
            index + 1
          })`;
          values.push(min, max);
          index += 2;
        } else if (!isNaN(min) && range.includes("+")) {
          baseQuery += ` AND num_employees >= $${index}`;
          values.push(min);
          index++;
        } else {
          addStringFilter("num_employees::text", range);
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
            `(CAST(annual_revenue AS NUMERIC) >= $${index} AND CAST(annual_revenue AS NUMERIC) <= $${
              index + 1
            })`
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
        baseQuery += ` AND (` + revenueConditions.join(" OR ") + `)`;
      }
    }

    // Apply filters
    addIncludeFilter("company_country", includeCompanyLocation);
    addExcludeFilter("company_country", excludeCompanyLocation);
    addIncludeFilter("industry", includeIndustry);
    addExcludeFilter("industry", excludeIndustry);
    addIncludeFilter("company_name", includeCompany);
    addExcludeFilter("company_name", excludeCompany);
    addStringFilter("company_name", includeCompanyKeyword);

    // Handle technology filters
    if (includeTechnology && includeTechnology.length > 0) {
      baseQuery += ` AND (`;
      const techConditions = includeTechnology.map(
        (_, i) => `technologies ILIKE $${index + i}`
      );
      baseQuery += techConditions.join(" OR ");
      baseQuery += `)`;

      includeTechnology.forEach((tech) => {
        values.push(`%${tech}%`);
      });
      index += includeTechnology.length;
    }

    // Handle funding filter
    if (funding && funding.length > 0) {
      baseQuery += ` AND (`;
      const fundingConditions = funding.map(
        (_, i) => `latest_funding ILIKE $${index + i}`
      );
      baseQuery += fundingConditions.join(" OR ");
      baseQuery += `)`;

      funding.forEach((fund) => {
        values.push(`%${fund}%`);
      });
      index += funding.length;
    }

    // Handle founding year filter (multiple values)
    if (foundingYear && foundingYear.length > 0) {
      baseQuery += ` AND (`;
      const yearConditions = foundingYear.map(
        (_, i) => `founded_year = $${index + i}`
      );
      baseQuery += yearConditions.join(" OR ");
      baseQuery += `)`;

      foundingYear.forEach((year) => {
        values.push(year);
      });
      index += foundingYear.length;
    }

    // Add LIMIT to the query
    baseQuery += ` LIMIT $${index}`;
    values.push(limit);
    index++;

    console.log("baseQuery>", baseQuery);
    console.log("values>", values);

    const { rows } = await client.query(baseQuery, values);

    if (rows.length === 0) {
      await client.query("ROLLBACK");
      return res.status(404).json({ error: "No data found to export" });
    }

    if (userRole !== "admin") {
      // Deduct credits only if the user is NOT an admin
      const creditsToDeduct = rows.length;
      const deductionResult = await deductCredits(userId, creditsToDeduct);

      if (!deductionResult.success) {
        await client.query("ROLLBACK");
        return res
          .status(403)
          .json({ error: "Insufficient credits to export data" });
      }
    }

    // Generate CSV data
    const csvHeader = Object.keys(rows[0]).join(",") + "\n";
    const csvRows = rows
      .map((row) =>
        Object.values(row)
          .map((value) => {
            if (value === null || value === undefined) return "";
            if (typeof value === "string") {
              const escaped = value.replace(/"/g, '""');
              return value.includes(",") ||
                value.includes('"') ||
                value.includes("\n")
                ? `"${escaped}"`
                : escaped;
            }
            return value;
          })
          .join(",")
      )
      .join("\n");

    const csvData = csvHeader + csvRows;

    // Upload CSV to S3
    const fileName = `companies_export_${Date.now()}.csv`;
    const bucketName = process.env.S3_BUCKET_NAME; // Add your bucket name to environment variables
    const { fileUrl } = await uploadFileToS3(
      Buffer.from(csvData, "utf-8"),
      fileName,
      bucketName
    );

    // Prepare filters object
    const filters = {
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
      limit,
      funding,
      foundingYear,
    };

    // Insert into exported_files table
    const insertQuery = `
      INSERT INTO exported_files (user_id, type, export_row_count, file_name, file_url, filters)
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id;
    `;
    const insertValues = [userId, "company", rows.length, fileName, fileUrl, filters];
    await client.query(insertQuery, insertValues);

    if (rows.length > 1000) {
      // If limit is greater than 1000, send the CSV via email
      await sendCSVEmail(userEmail, csvData);

      await client.query("COMMIT");

      return res.status(200).json({
        message: `CSV file has been sent to your email.`,
        remaining_credits:
          userRole !== "admin" ? deductionResult.remainingCredits : "N/A",
      });
    } else {
      // If limit is 1000 or less, send the CSV as a downloadable response
      res.setHeader("Content-Type", "text/csv");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${fileName}"`
      );

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

    const query = `UPDATE companies SET ${setClause}, updated_at = CURRENT_TIMESTAMP WHERE id = $${
      keys.length + 1
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
    const userId = req.currentUser.id; // Get userId from the authenticated user
    const { leads, type } = req.body; // Array of leads to save

    if (!userId) {
      return errorResponse(res, "User ID is required", 400);
    }

    if (!Array.isArray(leads)) {
      return errorResponse(
        res,
        "Invalid input: expected an array of leads",
        400
      );
    }

    await client.query("BEGIN");

    // Save or update each lead
    for (const lead of leads) {
      const { leadId, email, mobile } = lead;

      if (!leadId || !type) {
        await client.query("ROLLBACK");
        return errorResponse(
          res,
          "leadId and type are required for each lead",
          400
        );
      }

      const insertQuery = `
        INSERT INTO saved_leads (user_id, lead_id, type, email, mobile)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (user_id, lead_id, type) 
        DO UPDATE SET 
          email = EXCLUDED.email,
          mobile = EXCLUDED.mobile
      `;
      const insertValues = [
        userId,
        leadId,
        type,
        email || false,
        mobile || false,
      ];
      await client.query(insertQuery, insertValues);
    }

    await client.query("COMMIT");
    return successResponse(res, { message: "Leads saved successfully" });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error saving leads:", error);
    return errorResponse(res, "Error saving leads", 500);
  } finally {
    client.release();
  }
};

const getSavedLeads = async (req, res) => {
  const client = await pool.connect();

  try {
    const { type, page = 1, limit = 10 } = req.query;
    const userId = req.currentUser.id; // Get userId from the authenticated user

    if (!userId) {
      return errorResponse(res, "User ID is required", 400);
    }

    const offset = (page - 1) * limit;

    let query = `
      SELECT sl.id AS saved_lead_id, sl.type, sl.email as "isEmail", sl.mobile as "isMobile", sl.saved_at,
             l.* -- Select all columns from the leads table
      FROM saved_leads sl
      INNER JOIN peopleLeads l ON sl.lead_id = l.id
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
  getSavedLeads,
};

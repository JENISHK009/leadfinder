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

const cleanPhoneNumber = (phone) => {
  if (!phone) return null;
  return phone.replace(/[^\d+]/g, "");
};

const safeDateString = (dateValue) => {
  if (!dateValue || dateValue === "" || dateValue === "\\N") return null;
  return dateValue.toString().trim();
};

const safeInteger = (value) => {
  if (!value || value === "" || value === "\\N") return null;

  if (!/^\d+$/.test(value.toString().trim())) {
    return null;
  }

  const num = parseInt(value.toString().trim(), 10);
  return isNaN(num) ? null : num;
};

const escapeCopyValue = (value) => {
  if (value === null || value === undefined || value === "") {
    return "\\N";
  }

  return value.toString()
    .replace(/\\/g, "\\\\") // Escape backslashes first
    .replace(/\t/g, "\\t")  // Escape tabs
    .replace(/\n/g, "\\n")  // Escape newlines
    .replace(/\r/g, "\\r"); // Escape carriage returns
};

class JsonToCopyTransform extends Transform {
  constructor(options) {
    super({ objectMode: true });
  }

  _transform(row, encoding, callback) {
    const values = [
      escapeCopyValue(row["First Name"] || null),
      escapeCopyValue(row["Last Name"] || null),
      escapeCopyValue(row["Title"] || null),
      escapeCopyValue(row["Company"] || null),
      escapeCopyValue(row["Email"] || null),
      escapeCopyValue(row["Email Status"] || null),
      escapeCopyValue(row["Seniority"] || null),
      escapeCopyValue(row["Departments"] || null),
      escapeCopyValue(cleanPhoneNumber(row["Work Direct Phone"])),
      escapeCopyValue(cleanPhoneNumber(row["Mobile Phone"])),
      escapeCopyValue(cleanPhoneNumber(row["Corporate Phone"])),
      escapeCopyValue(safeInteger(row["# Employees"])),
      escapeCopyValue(row["Industry"] || null),
      escapeCopyValue(row["Keywords"] || null),
      escapeCopyValue(row["Person Linkedin Url"] || null),
      escapeCopyValue(row["Website"] || null),
      escapeCopyValue(row["Company Linkedin Url"] || null),
      escapeCopyValue(row["Facebook Url"] || null),
      escapeCopyValue(row["Twitter Url"] || null),
      escapeCopyValue(row["City"] || null),
      escapeCopyValue(row["State"] || null),
      escapeCopyValue(row["Country"] || null),
      escapeCopyValue(row["Company Address"] || null),
      escapeCopyValue(row["Company City"] || null),
      escapeCopyValue(row["Company State"] || null),
      escapeCopyValue(row["Company Country"] || null),
      escapeCopyValue(row["SEO Description"] || null),
      escapeCopyValue(row["Technologies"] || null),
      escapeCopyValue(row["Annual Revenue"] || null),
      escapeCopyValue(row["Total Funding"] || null),
      escapeCopyValue(row["Latest Funding"] || null),
      escapeCopyValue(row["Latest Funding Amount"] || null),
      escapeCopyValue(safeDateString(row["Last Raised At"])),
      escapeCopyValue(safeInteger(row["Number of Retail Locations"]))
    ].join('\t');

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

    // Create a temporary table to stage the data
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

    await client.query(`
      CREATE INDEX temp_company_linkedin_idx ON temp_leads (company_linkedin_url) 
      WHERE company_linkedin_url IS NOT NULL AND company_linkedin_url != '';
    `);

    await client.query(`
      CREATE INDEX temp_linkedin_idx ON temp_leads (linkedin_url) 
      WHERE linkedin_url IS NOT NULL AND linkedin_url != '';
    `);

    await client.query(`
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
    `);

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
    `);

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
          baseQuery += ` AND (pl.num_employees >= $${index} AND pl.num_employees <= $${index + 1
            })`;
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
            `(pl.annual_revenue >= $${index} AND pl.annual_revenue <= $${index + 1
            })`
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
      leadsId, // New leadsId array filter
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

    let whereClause = "WHERE 1=1";
    let values = [];
    let index = 1;

    if (leadsId && leadsId.length > 0) {
      whereClause = `WHERE id = ANY($${index})`;
      values.push(leadsId);
      index++;
    } else {
      const addIncludeFilter = (field, valueArray) => {
        if (valueArray?.length) {
          whereClause += ` AND ${field} = ANY($${index})`;
          values.push(valueArray);
          index++;
        }
      };

      const addExcludeFilter = (field, valueArray) => {
        if (valueArray?.length) {
          whereClause += ` AND ${field} <> ALL($${index})`;
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

      addIncludeFilter("industry", includeIndustry);
      addExcludeFilter("industry", excludeIndustry);
      addIncludeFilter("seniority", includemanagmentRole);
      addIncludeFilter("company", includeCompany);
      addExcludeFilter("company", excludeCompany);
      addIncludeFilter("departments", includedepartmentKeyword);
      addIncludeFilter("country", includePersonalCountry);
      addExcludeFilter("country", excludePersonalCountry);
      addIncludeFilter("company_country", includecompanyLocation);
      addExcludeFilter("company_country", excludeCompanyLocation);
      addIncludeFilter("technologies", includetechnology);

      if (includejobTitles?.length) {
        whereClause += ` AND (${includejobTitles.map(() => `title ILIKE $${index++}`).join(" OR ")})`;
        values.push(...includejobTitles.map(title => `%${title}%`));
      }

      if (excludeJobTitles?.length) {
        whereClause += ` AND (${excludeJobTitles.map(() => `title NOT ILIKE $${index++}`).join(" AND ")})`;
        values.push(...excludeJobTitles.map(title => `%${title}%`));
      }

      if (funding?.length) {
        whereClause += ` AND (${funding.map(() => `latest_funding ILIKE $${index++}`).join(" OR ")})`;
        values.push(...funding.map(fund => `%${fund}%`));
      }

      if (foundingYear?.length) {
        whereClause += ` AND (${foundingYear.map(() => `EXTRACT(YEAR FROM last_raised_at) = $${index++}`).join(" OR ")})`;
        values.push(...foundingYear);
      }
    }

    let finalQuery = `SELECT * FROM peopleLeads ${whereClause} LIMIT $${index}`;
    values.push(limit);

    console.log("finalQuery>", finalQuery);
    console.log("values>", values);

    const { rows } = await client.query(finalQuery, values);

    if (rows.length === 0) {
      await client.query("ROLLBACK");
      return errorResponse(res, "No data found to export", 404);
    }

    if (userRole !== "admin") {
      const creditsToDeduct = rows.length;
      const deductionResult = await deductCredits(userId, creditsToDeduct);

      if (!deductionResult.success) {
        await client.query("ROLLBACK");
        return errorResponse(res, "Insufficient credits to export leads", 403);
      }
    }

    const cleanRows = rows.map(row => {
      const cleanRow = { ...row };
      delete cleanRow.row_num;
      return cleanRow;
    });

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

    if (rows.length > 1000) {
      await sendCSVEmail(userEmail, csvData);
      await client.query("COMMIT");
      return successResponse(res, { message: "CSV file has been sent to your email.", remaining_credits: userRole !== "admin" ? deductionResult.remainingCredits : "N/A" });
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

function convertDateFormat(dateStr) {
  if (!dateStr) return null;

  // Handle cases where date might already be in different format
  if (dateStr.includes('/')) {
    const parts = dateStr.split('/');
    if (parts.length === 3) {
      return `${parts[2]}-${parts[1]}-${parts[0]}`;
    }
  }

  // Handle DD-MM-YYYY format
  const parts = dateStr.split('-');
  if (parts.length === 3) {
    // Ensure two-digit day and month
    const day = parts[0].padStart(2, '0');
    const month = parts[1].padStart(2, '0');
    return `${parts[2]}-${month}-${day}`;
  }

  // Return as-is if format doesn't match
  return dateStr;
}

const processAndInsertCompanies = async (results) => {
  console.log(`Starting to process ${results.length} companies`);
  const client = await pool.connect();

  try {
    await client.query("BEGIN");
    console.log("Transaction started");

    await client.query(`
      CREATE TEMP TABLE temp_companies (
        id SERIAL PRIMARY KEY,
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
        last_raised_at DATE,
        annual_revenue TEXT,
        num_retail_locations INTEGER,
        sic_codes TEXT,
        short_description TEXT,
        founded_year INTEGER
      ) ON COMMIT DROP
    `);
    console.log("Temporary table created");

    // Process data in batches to reduce memory usage
    const BATCH_SIZE = 50; // Adjust based on your needs
    const batches = [];
    
    for (let i = 0; i < results.length; i += BATCH_SIZE) {
      batches.push(results.slice(i, i + BATCH_SIZE));
    }
    
    console.log(`Split into ${batches.length} batches for processing`);
    
    let totalInserted = 0;
    
    for (let batchIndex = 0; batchIndex < batches.length; batchIndex++) {
      const batch = batches[batchIndex];
      console.log(`Processing batch ${batchIndex + 1}/${batches.length} with ${batch.length} records`);
      
      // Prepare the data for COPY
      const data = batch
        .map(row => {
          const values = [
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
            cleanPhoneNumber(row["Company Phone"]) || "\\N",
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
                // Properly escape special characters
                return val
                  .toString()
                  .replace(/\\/g, "\\\\")
                  .replace(/\r/g, "\\r")
                  .replace(/\n/g, "\\n")
                  .replace(/\t/g, "\\t");
              }
            })
            .join("\t");

          return values;
        })
        .join("\n");

      // Use COPY to insert data into the temporary table (much faster than individual inserts)
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
          .on("finish", resolve)
          .on("error", reject);
      });
      
      console.log(`Batch ${batchIndex + 1} loaded into temp table`);
    }
    
    // Handle duplicates within the temp table by keeping only the latest entry by id
    console.log("Handling duplicates in temp table");
    await client.query(`
      CREATE TEMP TABLE deduplicated_companies AS
      WITH ranked_companies AS (
        SELECT
          *,
          ROW_NUMBER() OVER (
            PARTITION BY company_linkedin_url
            ORDER BY id DESC
          ) as row_num
        FROM temp_companies
        WHERE company_linkedin_url IS NOT NULL AND company_linkedin_url != ''
      )
      SELECT
        company_name, num_employees, industry, website, company_linkedin_url, facebook_url, twitter_url,
        company_street, company_city, company_state, company_country, company_postal_code, company_address,
        keywords, company_phone, seo_description, technologies, total_funding, latest_funding,
        latest_funding_amount, last_raised_at, annual_revenue, num_retail_locations, sic_codes,
        short_description, founded_year
      FROM ranked_companies
      WHERE row_num = 1
      
      UNION ALL
      
      SELECT
        company_name, num_employees, industry, website, company_linkedin_url, facebook_url, twitter_url,
        company_street, company_city, company_state, company_country, company_postal_code, company_address,
        keywords, company_phone, seo_description, technologies, total_funding, latest_funding,
        latest_funding_amount, last_raised_at, annual_revenue, num_retail_locations, sic_codes,
        short_description, founded_year
      FROM temp_companies
      WHERE company_linkedin_url IS NULL OR company_linkedin_url = '';
    `);
    
    // Create index for performance on the deduplicated table
    await client.query("CREATE INDEX ON deduplicated_companies (company_linkedin_url) WHERE company_linkedin_url IS NOT NULL");
    
    console.log("Temp table deduplicated, performing main insertion");

    // First, handle records with company_linkedin_url
    const linkedInResult = await client.query(`
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
      FROM deduplicated_companies
      WHERE company_linkedin_url IS NOT NULL AND company_linkedin_url != ''
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

    console.log(`Upserted ${linkedInResult.rowCount} records with LinkedIn URLs`);

    const noLinkedInResult = await client.query(`
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
      FROM deduplicated_companies dc
      LEFT JOIN companies c ON 
          c.company_name = dc.company_name AND
          (
              (c.company_address IS NOT NULL AND dc.company_address IS NOT NULL AND c.company_address = dc.company_address)
              OR (c.company_address IS NULL AND dc.company_address IS NULL)
          )
      WHERE (dc.company_linkedin_url IS NULL OR dc.company_linkedin_url = '')
      AND c.id IS NULL
    `);

    console.log(`Inserted ${noLinkedInResult.rowCount} new records without LinkedIn URLs`);

    const totalCount = linkedInResult.rowCount + noLinkedInResult.rowCount;
    console.log(`Total records processed: ${totalCount}`);

    await client.query("COMMIT");
    console.log("Transaction committed successfully");
    return totalCount;
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

const addCompaniesData = async (req, res) => {
  console.log("Starting addCompaniesData process");

  if (!req.body || !Array.isArray(req.body) || req.body.length === 0) {
    console.log("No valid data provided in request body");
    return errorResponse(res, "Valid JSON data array is required", 400);
  }

  const results = [...req.body];
  console.log(`Processing ${results.length} company records from JSON data`);

  results.forEach(data => {
    const dateFields = ['Last Raised At', 'Founded Year'];
    dateFields.forEach(field => {
      if (data[field]) {
        data[field] = convertDateFormat(data[field]);
      }
    });
  });

  try {
    const startTime = Date.now();
    console.log(`Starting database insertion at ${new Date(startTime).toISOString()}`);

    const insertedCount = await processAndInsertCompanies(results);

    const endTime = Date.now();
    const timeTaken = (endTime - startTime) / 1000;
    console.log(`Database insertion completed in ${timeTaken} seconds`);

    return successResponse(res, {
      message: "Data inserted or updated successfully",
      total_rows: results.length,
      inserted_rows: insertedCount,
      time_taken: `${timeTaken} seconds`,
    });
  } catch (error) {
    console.error("Error during data processing:", error);
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
          baseQuery += ` AND (num_employees >= $${index} AND num_employees <= $${index + 1
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
            `(CAST(annual_revenue AS NUMERIC) >= $${index} AND CAST(annual_revenue AS NUMERIC) <= $${index + 1
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
      leadsId = null, // New filter for leads IDs
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

    // If leadsId is provided and not empty, override all other filters
    if (leadsId && leadsId.length > 0) {
      baseQuery = `SELECT * FROM companies WHERE id IN (`;

      // Add parameterized placeholders for each lead ID
      baseQuery += leadsId.map((_, i) => `$${index + i}`).join(',');
      baseQuery += `)`;

      // Add the actual lead IDs to the values array
      values = leadsId;

      // Add LIMIT to the query
      baseQuery += ` LIMIT $${index + leadsId.length}`;
      values.push(limit);
    } else {
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
            baseQuery += ` AND (num_employees >= $${index} AND num_employees <= $${index + 1
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
              `(CAST(annual_revenue AS NUMERIC) >= $${index} AND CAST(annual_revenue AS NUMERIC) <= $${index + 1
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
    }

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
      leadsId,
    };

    // Insert into exported_files table
    const insertQuery = `
      INSERT INTO exported_files (user_id, type, export_row_count, file_name, file_url, filters)
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id;
    `;
    const insertValues = [
      userId,
      "company",
      rows.length,
      fileName,
      fileUrl,
      filters,
    ];
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
    const { type, filters, rowSelection, percomponyContact } = req.body; // New parameters: rowSelection and percomponyContact

    if (!type || (type !== 'people' && type !== 'company')) {
      return errorResponse(res, "Invalid type provided. Type must be 'people' or 'company'", 400);
    }

    await client.query("BEGIN");

    let tableName;
    if (type === 'people') {
      tableName = 'peopleLeads';
    } else if (type === 'company') {
      tableName = 'companies';
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

      // Handle employee count filter
      if (includeemployeeCount && includeemployeeCount.length > 0) {
        for (const range of includeemployeeCount) {
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

      // Handle management role filter
      addIncludeFilter("seniority", includemanagmentRole);

      // Handle company filter
      addIncludeFilter("company", includeCompany);
      addExcludeFilter("company", excludeCompany);

      // Handle department keyword filter
      addIncludeFilter("departments", includedepartmentKeyword);

      // Handle job title filters
      if (includejobTitles && includejobTitles.length > 0) {
        whereClause += ` AND (`;
        const titleConditions = includejobTitles.map(
          (_, i) => `title ILIKE $${index + i}`
        );
        whereClause += titleConditions.join(" OR ");
        whereClause += `)`;
        values.push(...includejobTitles.map(title => `%${title}%`));
        index += includejobTitles.length;
      }

      if (excludeJobTitles && excludeJobTitles.length > 0) {
        whereClause += ` AND (`;
        const excludeTitleConditions = excludeJobTitles.map(
          (_, i) => `title NOT ILIKE $${index + i}`
        );
        whereClause += excludeTitleConditions.join(" AND ");
        whereClause += `)`;
        values.push(...excludeJobTitles.map(title => `%${title}%`));
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
        values.push(...includetechnology.map(tech => `%${tech}%`));
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
        values.push(...funding.map(fund => `%${fund}%`));
        index += funding.length;
      }

      // Handle founding year filter
      if (foundingYear && foundingYear.length > 0) {
        whereClause += ` AND (`;
        const yearConditions = foundingYear.map(
          (_, i) => `EXTRACT(YEAR FROM last_raised_at) = $${index + i}`
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

    // Apply percomponyContact only for people leads
    let finalQuery;
    if (type === 'people' && percomponyContact && percomponyContact > 0) {
      finalQuery = `
        WITH filtered_leads AS (
          SELECT pl.*
          FROM peopleLeads pl
          ${whereClause}
        ),
        company_grouped AS (
          SELECT 
            fl.*,
            ROW_NUMBER() OVER (PARTITION BY fl.company_linkedin_url ORDER BY fl.id) as row_num
          FROM 
            filtered_leads fl
        )
        SELECT id FROM company_grouped 
        WHERE row_num <= $${index}
        ORDER BY company_linkedin_url, row_num
      `;
      values.push(percomponyContact); // Add percomponyContact value
    } else {
      // For company leads or when percomponyContact is not applicable
      finalQuery = `
        SELECT id 
        FROM ${tableName}
        ${whereClause}
        LIMIT $${index}
      `;
      values.push(rowSelection); // Add rowSelection value
    }

    const { rows } = await client.query(finalQuery, values);

    if (rows.length === 0) {
      await client.query("ROLLBACK");
      return errorResponse(res, "No leads found with the provided filters", 404);
    }

    await client.query("COMMIT");

    // Extract IDs from the result
    const leadIds = rows.map(row => row.id);

    return successResponse(res, { leadIds });

  } catch (error) {
    await client.query("ROLLBACK");
    console.error("Error fetching selected leads:", error);
    return errorResponse(res, "Error fetching selected leads", 500);
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
  getselectedLeads,
};

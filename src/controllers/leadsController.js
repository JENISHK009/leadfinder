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
      FROM temp_leads
      WHERE company_linkedin_url IS NOT NULL
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

    // Insert or update people leads data
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
      ON CONFLICT (email) DO UPDATE SET
        first_name = EXCLUDED.first_name,
        last_name = EXCLUDED.last_name,
        title = EXCLUDED.title,
        company = EXCLUDED.company,
        email_status = EXCLUDED.email_status,
        seniority = EXCLUDED.seniority,
        departments = EXCLUDED.departments,
        work_direct_phone = EXCLUDED.work_direct_phone,
        mobile_phone = EXCLUDED.mobile_phone,
        corporate_phone = EXCLUDED.corporate_phone,
        num_employees = EXCLUDED.num_employees,
        industry = EXCLUDED.industry,
        keywords = EXCLUDED.keywords,
        linkedin_url = EXCLUDED.linkedin_url,
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
    } = req.body;

    let baseQuery = `SELECT *, COUNT(*) OVER() AS total_count FROM peopleLeads WHERE 1=1`;
    let values = [];
    let index = 1;

    // Updated helper functions for case-insensitive include/exclude array filters
    const addIncludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        // Use ILIKE for case-insensitive matching with ANY
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) = LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" OR ")})`;
        valueArray.forEach((val) => {
          values.push(val);
        });
        index += valueArray.length;
      }
    };

    const addExcludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        // Use ILIKE for case-insensitive matching with ALL
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) <> LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" AND ")})`;
        valueArray.forEach((val) => {
          values.push(val);
        });
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
      // Handle employee count ranges (e.g., "11-50")
      const employeeRanges = [];

      for (const range of includeemployeeCount) {
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
        baseQuery += ` AND (` + revenueConditions.join(" OR ") + `)`;
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
      baseQuery += ` AND (`;
      const titleConditions = includejobTitles.map(
        (_, i) => `title ILIKE $${index + i}`
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
        (_, i) => `title NOT ILIKE $${index + i}`
      );
      baseQuery += excludeTitleConditions.join(" AND ");
      baseQuery += `)`;

      excludeJobTitles.forEach((title) => {
        values.push(`%${title}%`);
      });
      index += excludeJobTitles.length;
    }

    if (includetechnology && includetechnology.length > 0) {
      baseQuery += ` AND (`;
      const techConditions = includetechnology.map(
        (_, i) => `technologies ILIKE $${index + i}`
      );
      baseQuery += techConditions.join(" OR ");
      baseQuery += `)`;

      includetechnology.forEach((tech) => {
        values.push(`%${tech}%`);
      });
      index += includetechnology.length;
    }

    addIncludeFilter("country", includePersonalCountry);
    addExcludeFilter("country", excludePersonalCountry);

    addIncludeFilter("company_country", includecompanyLocation);
    addExcludeFilter("company_country", excludeCompanyLocation);

    // Pagination
    const offset = (page - 1) * limit;
    baseQuery += ` ORDER BY id ASC LIMIT $${index++} OFFSET $${index++}`;
    values.push(limit, offset);

    console.log("baseQuery>", baseQuery);
    console.log("values>", values);

    const { rows } = await pool.query(baseQuery, values);
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
    } = req.body;

    const userId = req.currentUser.id;
    const userEmail = req.currentUser.email;
    const userRole = req.currentUser.roleName;
    if (!userId || !userEmail)
      return errorResponse(res, "User information not found", 400);

    await client.query("BEGIN");

    let baseQuery = `SELECT * FROM peopleLeads WHERE 1=1`;
    let values = [];
    let index = 1;

    // Add filters dynamically (same logic as before)
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

    // Apply filters
    if (search) {
      baseQuery += ` AND (
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
    addIncludeFilter("company", includeCompany);
    addExcludeFilter("company", excludeCompany);
    addIncludeFilter("seniority", includemanagmentRole);
    addIncludeFilter("departments", includedepartmentKeyword);
    addIncludeFilter("country", includePersonalCountry);
    addExcludeFilter("country", excludePersonalCountry);
    addIncludeFilter("company_country", includecompanyLocation);
    addExcludeFilter("company_country", excludeCompanyLocation);

    // Handle job title filters
    if (includejobTitles && includejobTitles.length > 0) {
      baseQuery += ` AND (${includejobTitles
        .map((_, i) => `title ILIKE $${index + i}`)
        .join(" OR ")})`;
      values.push(...includejobTitles.map((title) => `%${title}%`));
      index += includejobTitles.length;
    }

    if (excludeJobTitles && excludeJobTitles.length > 0) {
      baseQuery += ` AND (${excludeJobTitles
        .map((_, i) => `title NOT ILIKE $${index + i}`)
        .join(" AND ")})`;
      values.push(...excludeJobTitles.map((title) => `%${title}%`));
      index += excludeJobTitles.length;
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

    if (rows.length > 1000) {
      // If limit is greater than 1000, send the CSV via email
      await sendCSVEmail(userEmail, csvData);
      await client.query("COMMIT");

      return successResponse(res, {
        message: `CSV file has been sent to your email.`,
        remaining_credits:
          userRole !== "admin" ? deductionResult.remainingCredits : "N/A",
      });
    } else {
      // If limit is 1000 or less, send the CSV as a downloadable response
      res.setHeader("Content-Type", "text/csv");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="people_leads_export.csv"`
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
        valueArray.forEach((val) => {
          values.push(val);
        });
        index += valueArray.length;
      }
    };

    const addExcludeFilter = (field, valueArray) => {
      if (valueArray && valueArray.length > 0) {
        const conditions = valueArray.map(
          (_, i) => `LOWER(${field}) <> LOWER($${index + i})`
        );
        baseQuery += ` AND (${conditions.join(" AND ")})`;
        valueArray.forEach((val) => {
          values.push(val);
        });
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
        `attachment; filename="companies_export.csv"`
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
};

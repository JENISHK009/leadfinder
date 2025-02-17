import csvParser from 'csv-parser';
import fs from 'fs';
import { Readable } from 'stream';
import pool from '../config/db.js';
import { successResponse, errorResponse, sendCSVEmail } from '../utils/index.js';
import pkg from 'pg-copy-streams';
const { from } = pkg;
import { deductCredits } from '../servies/userService.js';

const cleanPhoneNumber = (phone) => {
    if (!phone) return null;
    return phone.replace(/[^\d+]/g, '');
};

const processAndInsertLeads = async (results) => {
    const client = await pool.connect();

    try {
        await client.query('BEGIN');
        await client.query('CREATE TEMP TABLE temp_leads (LIKE leads INCLUDING ALL) ON COMMIT DROP');

        const data = results.map(row => {
            const values = [
                row['First Name'], row['Last Name'], row['Title'], row['Company'], row['Email'],
                row['Email Status'], row['Seniority'], row['Departments'], cleanPhoneNumber(row['Work Direct Phone']),
                cleanPhoneNumber(row['Mobile Phone']), cleanPhoneNumber(row['Corporate Phone']), row['# Employees'],
                row['Industry'], row['Keywords'], row['Person Linkedin Url'], row['Website'], row['Company Linkedin Url'],
                row['Facebook Url'], row['Twitter Url'], row['City'], row['State'], row['Country'], row['Company Address'],
                row['Company City'], row['Company State'], row['Company Country'], row['SEO Description'],
                row['Technologies'], row['Annual Revenue'], row['Total Funding'], row['Latest Funding'],
                row['Latest Funding Amount'], row['Last Raised At'], row['Number of Retail Locations']
            ].map(val => (val === null || val === undefined || val === '' ? '\\N' : val.toString().replace(/[\t\n\\]/g, ' '))).join('\t');

            return values;
        }).join('\n');

        const copyStream = client.query(from(`COPY temp_leads (
            first_name, last_name, title, company, email, email_status, seniority, departments, work_direct_phone,
            mobile_phone, corporate_phone, num_employees, industry, keywords, linkedin_url, website, company_linkedin_url,
            facebook_url, twitter_url, city, state, country, company_address, company_city, company_state, company_country,
            seo_description, technologies, annual_revenue, total_funding, latest_funding, latest_funding_amount,
            last_raised_at, num_retail_locations
        ) FROM STDIN WITH (FORMAT text, NULL '\\N')`));

        const readable = new Readable();
        readable.push(data);
        readable.push(null);
        await new Promise((resolve, reject) => {
            readable.pipe(copyStream).on('finish', resolve).on('error', reject);
        });

        await client.query(`
            INSERT INTO leads (
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
                first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, title = EXCLUDED.title,
                company = EXCLUDED.company, email_status = EXCLUDED.email_status, seniority = EXCLUDED.seniority,
                departments = EXCLUDED.departments, work_direct_phone = EXCLUDED.work_direct_phone,
                mobile_phone = EXCLUDED.mobile_phone, corporate_phone = EXCLUDED.corporate_phone,
                num_employees = EXCLUDED.num_employees, industry = EXCLUDED.industry, keywords = EXCLUDED.keywords,
                linkedin_url = EXCLUDED.linkedin_url, website = EXCLUDED.website, company_linkedin_url = EXCLUDED.company_linkedin_url,
                facebook_url = EXCLUDED.facebook_url, twitter_url = EXCLUDED.twitter_url, city = EXCLUDED.city,
                state = EXCLUDED.state, country = EXCLUDED.country, company_address = EXCLUDED.company_address,
                company_city = EXCLUDED.company_city, company_state = EXCLUDED.company_state, company_country = EXCLUDED.company_country,
                seo_description = EXCLUDED.seo_description, technologies = EXCLUDED.technologies, annual_revenue = EXCLUDED.annual_revenue,
                total_funding = EXCLUDED.total_funding, latest_funding = EXCLUDED.latest_funding,
                latest_funding_amount = EXCLUDED.latest_funding_amount, last_raised_at = EXCLUDED.last_raised_at,
                num_retail_locations = EXCLUDED.num_retail_locations, updated_at = CURRENT_TIMESTAMP
        `);

        await client.query('COMMIT');
    } catch (error) {
        await client.query('ROLLBACK');
        throw error;
    } finally {
        client.release();
    }
};

const addPeopleLeadsData = (req, res) => {
    if (!req.file) return errorResponse(res, 'CSV file is required');

    const filePath = req.file.path;
    const results = [];

    fs.createReadStream(filePath)
        .pipe(csvParser())
        .on('data', (data) => results.push(data))
        .on('end', async () => {
            try {
                const startTime = Date.now();
                await processAndInsertLeads(results);
                const endTime = Date.now();
                const timeTaken = (endTime - startTime) / 1000;
                fs.unlinkSync(filePath);
                return successResponse(res, {
                    message: 'Data inserted or updated successfully',
                    count: results.length,
                    time_taken: `${timeTaken} seconds`
                });
            } catch (error) {
                fs.unlinkSync(filePath);
                console.error('Error processing data:', error);
                return errorResponse(res, 'Error processing data', 500);
            }
        });
};

const getLeads = async (req, res) => {
    try {
        const {
            first_name, last_name, email, company, industry, country, state, city, seniority, departments,
            min_employees, max_employees, page = 1, limit = 10
        } = req.body;

        let baseQuery = `SELECT *, COUNT(*) OVER() AS total_count FROM leads WHERE 1=1`;
        let values = [];
        let index = 1;

        const addFilter = (field, value, isArray = false) => {
            if (value) {
                if (isArray && value.length > 0) {
                    baseQuery += ` AND ${field} = ANY($${index})`;
                    values.push(value);
                    index++;
                } else if (!isArray) {
                    baseQuery += ` AND ${field} ILIKE $${index}`;
                    values.push(`%${value}%`);
                    index++;
                }
            }
        };

        addFilter('first_name', first_name);
        addFilter('last_name', last_name);
        addFilter('email', email);
        addFilter('company', company, true);
        addFilter('industry', industry, true);
        addFilter('country', country, true);
        addFilter('state', state, true);
        addFilter('city', city, true);
        addFilter('seniority', seniority, true);
        addFilter('departments', departments, true);

        if (min_employees) {
            baseQuery += ` AND num_employees >= $${index}`;
            values.push(min_employees);
            index++;
        }
        if (max_employees) {
            baseQuery += ` AND num_employees <= $${index}`;
            values.push(max_employees);
            index++;
        }

        const offset = (page - 1) * limit;
        baseQuery += ` ORDER BY id ASC LIMIT $${index++} OFFSET $${index++}`;
        values.push(limit, offset);

        const { rows } = await pool.query(baseQuery, values);
        const totalCount = rows.length > 0 ? parseInt(rows[0].total_count, 10) : 0;
        const perPageCount = rows.length;

        return successResponse(res, {
            message: "Data fetched successfully",
            total_count: totalCount,
            per_page_count: perPageCount,
            data: rows
        });
    } catch (error) {
        console.error("Error fetching leads:", error);
        return errorResponse(res, "Error fetching data", 500);
    }
};

const exportLeadsToCSV = async (req, res) => {
    const client = await pool.connect();

    try {
        const {
            first_name, last_name, email, company, industry, country, state, city, seniority, departments,
            min_employees, max_employees
        } = req.body;

        const userId = req.currentUser.id;
        const userEmail = req.currentUser.email;

        if (!userId || !userEmail) return errorResponse(res, "User information not found", 400);

        await client.query('BEGIN');

        let baseQuery = `SELECT * FROM leads WHERE 1=1`;
        let values = [];
        let index = 1;

        const addFilter = (field, value, isArray = false) => {
            if (value) {
                if (isArray && value.length > 0) {
                    baseQuery += ` AND ${field} = ANY($${index})`;
                    values.push(value);
                    index++;
                } else if (!isArray) {
                    baseQuery += ` AND ${field} ILIKE $${index}`;
                    values.push(`%${value}%`);
                    index++;
                }
            }
        };

        addFilter('first_name', first_name);
        addFilter('last_name', last_name);
        addFilter('email', email);
        addFilter('company', company, true);
        addFilter('industry', industry, true);
        addFilter('country', country, true);
        addFilter('state', state, true);
        addFilter('city', city, true);
        addFilter('seniority', seniority, true);
        addFilter('departments', departments, true);

        if (min_employees) {
            baseQuery += ` AND num_employees >= $${index}`;
            values.push(min_employees);
            index++;
        }
        if (max_employees) {
            baseQuery += ` AND num_employees <= $${index}`;
            values.push(max_employees);
            index++;
        }

        const { rows } = await client.query(baseQuery, values);

        if (rows.length === 0) {
            await client.query('ROLLBACK');
            return errorResponse(res, "No data found to export", 404);
        }

        const creditsToDeduct = rows.length;
        const deductionResult = await deductCredits(userId, creditsToDeduct);

        if (!deductionResult.success) {
            await client.query('ROLLBACK');
            return errorResponse(res, "Insufficient credits to export leads", 403);
        }

        const csvHeader = Object.keys(rows[0]).join(',') + '\n';
        const csvRows = rows.map(row =>
            Object.values(row).map(value =>
                typeof value === 'string' && value.includes(',') ? `"${value}"` : value
            ).join(',')
        ).join('\n');

        const csvData = csvHeader + csvRows;
        await sendCSVEmail(userEmail, csvData);

        await client.query('COMMIT');

        return successResponse(res, {
            message: `CSV file has been sent to your email. ${creditsToDeduct} credit(s) deducted.`,
            remaining_credits: deductionResult.remainingCredits,
        });
    } catch (error) {
        await client.query('ROLLBACK');
        console.error("Error exporting leads:", error);
        return errorResponse(res, "Error exporting data", 500);
    } finally {
        client.release();
    }
};

const deductCreditsFromUser = async (req, res) => {
    try {
        const { count } = req.body; 
        const userId = req.currentUser.id; 

        if (!userId) {
            return errorResponse(res, "User information not found", 400);
        }

        if (!count || typeof count !== 'number' || count <= 0) {
            return errorResponse(res, "Invalid credit count provided", 400);
        }

        const deductionResult = await deductCredits(userId, count);

        if (!deductionResult.success) {
            return errorResponse(res, deductionResult.message || "Failed to deduct credits", 403);
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


export { addPeopleLeadsData, getLeads, exportLeadsToCSV, deductCreditsFromUser };
import csvParser from 'csv-parser';
import fs from 'fs';
import pool from '../config/db.js';
import { successResponse, errorResponse } from '../utils/responseUtil.js';

// ========================= CSV Upload and Insert =========================
export function addPeopleLeadsData(req, res) {
    if (!req.file) {
        return errorResponse(res, 'CSV file is required');
    }
    const filePath = req.file.path;
    const results = [];

    try {
        fs.createReadStream(filePath)
            .pipe(csvParser())
            .on('data', (data) => results.push(data))
            .on('end', async () => {
                try {
                    const client = await pool.connect();
                    console.log("Results:", results);

                    for (const row of results) {
                        const {
                            'First Name': first_name, 'Last Name': last_name, Title: title, Company: company,
                            Email: email, 'Email Status': email_status, Seniority: seniority, Departments: departments,
                            'Work Direct Phone': work_direct_phone, 'Mobile Phone': mobile_phone,
                            'Corporate Phone': corporate_phone, Phone: phone, Category: category,
                            Organization: organization, Position: position, Country: country, City: city,
                            State: state, 'Company Address': company_address, 'Company City': company_city,
                            'Company State': company_state, 'Company Country': company_country, Industry: industry,
                            '# Employees': num_employees, 'Person Linkedin Url': linkedin_url, Website: website,
                            'Company Linkedin Url': company_linkedin_url, 'Facebook Url': facebook_url,
                            'Twitter Url': twitter_url, Keywords: keywords, Technologies: technologies,
                            'SEO Description': seo_description, 'Annual Revenue': annual_revenue,
                            'Total Funding': total_funding, 'Latest Funding': latest_funding,
                            'Latest Funding Amount': latest_funding_amount, 'Last Raised At': last_raised_at,
                            'Number of Retail Locations': num_retail_locations
                        } = row;

                        await client.query(
                            `INSERT INTO leads (
                                first_name, last_name, title, company, email, email_status, seniority, departments, 
                                work_direct_phone, mobile_phone, corporate_phone, phone, category, organization, 
                                position, country, city, state, company_address, company_city, company_state, 
                                company_country, industry, num_employees, linkedin_url, website, company_linkedin_url, 
                                facebook_url, twitter_url, keywords, technologies, seo_description, annual_revenue, 
                                total_funding, latest_funding, latest_funding_amount, last_raised_at, num_retail_locations
                            ) VALUES (
                                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 
                                $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, 
                                $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38
                            )`,
                            [
                                first_name || null, last_name || null, title || null, company || null, 
                                email || null, email_status || null, seniority || null, departments || null,
                                work_direct_phone || null, mobile_phone || null, corporate_phone || null,
                                phone || null, category || null, organization || null, position || null,
                                country || null, city || null, state || null, company_address || null,
                                company_city || null, company_state || null, company_country || null,
                                industry || null, num_employees ? parseInt(num_employees) : null,
                                linkedin_url || null, website || null, company_linkedin_url || null,
                                facebook_url || null, twitter_url || null, keywords || null, 
                                technologies || null, seo_description || null, annual_revenue || null,
                                total_funding || null, latest_funding || null, latest_funding_amount || null,
                                last_raised_at ? new Date(last_raised_at) : null, 
                                num_retail_locations ? parseInt(num_retail_locations) : null
                            ]
                        );
                    }
                    client.release();
                    return successResponse(res, { message: 'Data inserted successfully', count: results.length });
                } catch (error) {
                    console.error(error);
                    return errorResponse(res, 'Error inserting data', 500);
                } finally {
                    fs.unlinkSync(filePath);
                }
            });
    } catch (error) {
        console.error(error);
        return errorResponse(res, 'Error processing file', 500);
    }
}

// ========================= Fetch Leads with Filters =========================
export const getLeads = async (req, res) => {
    try {
        const { 
            first_name, last_name, email, company, industry, country, state, city, seniority, departments,
            min_employees, max_employees, page = 1, limit = 10 
        } = req.body;

        let query = `SELECT * FROM leads WHERE 1=1`;
        let values = [];
        let index = 1;

        if (first_name) {
            query += ` AND first_name ILIKE $${index++}`;
            values.push(`%${first_name}%`);
        }
        if (last_name) {
            query += ` AND last_name ILIKE $${index++}`;
            values.push(`%${last_name}%`);
        }
        if (email) {
            query += ` AND email ILIKE $${index++}`;
            values.push(`%${email}%`);
        }
        if (company) {
            query += ` AND company ILIKE $${index++}`;
            values.push(`%${company}%`);
        }
        if (industry) {
            query += ` AND industry ILIKE $${index++}`;
            values.push(`%${industry}%`);
        }
        if (country) {
            query += ` AND country ILIKE $${index++}`;
            values.push(`%${country}%`);
        }
        if (state) {
            query += ` AND state ILIKE $${index++}`;
            values.push(`%${state}%`);
        }
        if (city) {
            query += ` AND city ILIKE $${index++}`;
            values.push(`%${city}%`);
        }
        if (seniority) {
            query += ` AND seniority ILIKE $${index++}`;
            values.push(`%${seniority}%`);
        }
        if (departments) {
            query += ` AND departments ILIKE $${index++}`;
            values.push(`%${departments}%`);
        }
        if (min_employees) {
            query += ` AND num_employees >= $${index++}`;
            values.push(min_employees);
        }
        if (max_employees) {
            query += ` AND num_employees <= $${index++}`;
            values.push(max_employees);
        }

        // Pagination
        const offset = (page - 1) * limit;
        query += ` ORDER BY id ASC LIMIT $${index++} OFFSET $${index++}`;
        values.push(limit, offset);

        const { rows } = await pool.query(query, values);

        return successResponse(res, { message: "Data fetched successfully", count: rows.length, data: rows });

    } catch (error) {
        console.error("Error fetching leads:", error);
        return errorResponse(res, "Error fetching data", 500);
    }
};

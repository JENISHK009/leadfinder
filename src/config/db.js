import pg from 'pg';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

dotenv.config();

const { Pool } = pg;

const pool = new Pool({
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  host: process.env.DB_HOST,
  port: process.env.DB_PORT,
  database: process.env.DB_NAME,
  ssl: {
    rejectUnauthorized : false
  }
});

pool.on("connect", () => {
  console.log("✅ Connected to PostgreSQL Database");
});

pool.on("error", (err) => {
  console.error("❌ Unexpected database error", err);
  process.exit(-1);
});

// Test the connection on startup
(async () => {
  try {
    const client = await pool.connect();
    const res = await client.query('SELECT VERSION()');
    console.log("PostgreSQL version:", res.rows[0].version);
    client.release();
  } catch (err) {
    console.error("❌ Failed to verify database connection", err);
    process.exit(-1);
  }
})();

export default pool;
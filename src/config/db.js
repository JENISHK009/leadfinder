import pkg from "pg";
import dotenv from "dotenv";

dotenv.config();

const { Pool } = pkg;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
});

pool.on("connect", () => {
    console.log("✅ Connected to Supabase PostgreSQL Database");
});

pool.on("error", (err) => {
    console.error("❌ Unexpected database error", err);
    process.exit(-1);
});

export default pool;

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";
import { Pool } from "pg";

const pool = new Pool({
    connectionString: "postgresql://postgres.fqnircnejqcuhvbbzrqk:123456jkJK@aws-0-ap-southeast-1.pooler.supabase.com:6543/postgres?pgbouncer=true",
    ssl: { rejectUnauthorized: false },
});

const __dirname = dirname(fileURLToPath(import.meta.url));
const modelsDir = path.join(__dirname, "../models");

async function checkTableExists(tableName) {
    const result = await pool.query(`SELECT to_regclass('${tableName}')`);
    return result.rows[0]?.to_regclass !== null;
}

async function migrateModel(modelName) {
    const modelPath = path.join(modelsDir, `${modelName}.sql`);
    if (!fs.existsSync(modelPath)) {
        console.error(`Model file for ${modelName} not found!`);
        return;
    }

    const createTableQuery = fs.readFileSync(modelPath, "utf-8");
    const tableExists = await checkTableExists(modelName);

    if (tableExists) {
        console.log(`Table for model ${modelName} already exists`);
    } else {
        console.log(`Creating table for model ${modelName}`);
        await pool.query(createTableQuery);
    }
}

async function ensureDbConnection() {
    try {
        await pool.query("SELECT 1");
        console.log("✅ Database connection established.");
    } catch (err) {
        console.error("❌ Database connection failed:", err);
        process.exit(1);
    }
}

async function runMigrations() {
    await ensureDbConnection();

    const modelNames = fs.readdirSync(modelsDir).map(file => file.replace(".sql", ""));
    for (const modelName of modelNames) {
        await migrateModel(modelName);
    }

    console.log("✅ Migration complete!");
}

runMigrations().catch((err) => {
    console.error("❌ Error running migrations:", err);
});

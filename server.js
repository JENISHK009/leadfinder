import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import bodyParser from "body-parser";
import authRoutes from "./src/routes/authRoutes.js";
import pool from "./src/config/db.js"; // Import DB connection

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());
app.use(bodyParser.json());

// Routes
app.use("/api/auth", authRoutes);
app.get("/api/test", (req, res) => {
    res.status(200).json({ message: "Server is running!" });
});

// Check database connection before starting the server
pool.connect()
    .then(() => {
        app.listen(PORT, () => {
            console.log(`Server is running on port ${PORT}`);
        });
    })
    .catch((err) => {
        console.error("Database connection failed", err);
        process.exit(1);
    });

import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import bodyParser from "body-parser";
import pool from "./src/config/db.js";
import { encryptionMiddleware, authenticateUser } from "./src/middleware/index.js";
import { authRoutes, leadsRoutes } from "./src/routes/index.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(bodyParser.json());
app.use(encryptionMiddleware);

app.use("/api/auth", authRoutes);

app.get("/api/test", (req, res) => {
    res.json({ message: "Server is running!" });
});

app.use(authenticateUser);
app.use("/api/leads", leadsRoutes);

pool.connect()
    .then(() => {
        console.log("Database connected successfully!");
        app.listen(PORT, () => {
            console.log(`Server is running on port ${PORT}`);
        });
    })
    .catch((err) => {
        console.error("Database connection failed", err);
        process.exit(1);
    });

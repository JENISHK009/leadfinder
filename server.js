import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import bodyParser from "body-parser";
import pool from "./src/config/db.js";
import {
  encryptionMiddleware,
  authenticateUser,
} from "./src/middleware/index.js";
import {
  authRoutes,
  leadsRoutes,
  plansRoutes,
  webhookRoutes,
  userRoutes,
} from "./src/routes/index.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 5000;

// Middleware
app.use(cors());

// Apply express.raw() middleware ONLY for the webhook route
app.use(
  "/api/webhook",
  express.raw({ type: "application/json" }),
  webhookRoutes
);

// Middleware for parsing JSON (for all other routes)
app.use(bodyParser.json());

// Encryption middleware
app.use(encryptionMiddleware);

// Routes
app.use("/api/auth", authRoutes);

// Test route
app.get("/api/test", (req, res) => {
  res.json({ message: "Server is running!" });
});

// Authenticate user middleware
app.use(authenticateUser);

// Protected routes
app.use("/api/leads", leadsRoutes);
app.use("/api/plans", plansRoutes);
app.use("/api/users", userRoutes);

// Connect to the database and start the server
pool
  .connect()
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

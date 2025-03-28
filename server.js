import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import bodyParser from "body-parser";
import session from "express-session";
import passport from "passport";

import pool from "./src/config/db.js";
dotenv.config();

// Middleware
import {
  encryptionMiddleware,
  authenticateUser,
} from "./src/middleware/index.js";

// Routes
import {
  authRoutes,
  googleAuthRoutes,
  leadsRoutes,
  plansRoutes,
  webhookRoutes,
  userRoutes,
} from "./src/routes/index.js";

// Passport Google Config
import { configurePassportGoogle } from "./src/utils/passportGoogleConfig.js";

const app = express();
const PORT = process.env.PORT || 5000;

// ======================
// Middleware Setup
// ======================
app.use(
  cors({
    origin: process.env.FRONTEND_URL,
    credentials: true,
  })
);

app.use(
  session({
    secret: process.env.SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      secure: false, // Set to true in production with HTTPS
      maxAge: 24 * 60 * 60 * 1000, // 1 day
    },
  })
);

app.use(passport.initialize());
app.use(passport.session());

// Configure Passport Google Strategy
configurePassportGoogle();

// ======================
// Routes Setup
// ======================
app.use(
  "/api/webhook",
  express.raw({ type: "application/json" }),
  webhookRoutes
);

// Middleware for parsing JSON (for all other routes)
app.use(bodyParser.json());

// Encryption middleware
app.use(encryptionMiddleware);

// Public Routes
app.use("/api/auth", authRoutes);
app.use("/api/auth", googleAuthRoutes); // Google auth routes

app.use(authenticateUser);

// Protected routes
app.use("/api/leads", leadsRoutes);
app.use("/api/plans", plansRoutes);
app.use("/api/users", userRoutes);

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
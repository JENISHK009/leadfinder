import express from "express";
import { userController } from "../controllers/index.js";

const router = express.Router();

router.get("/getUsersWithActivePlans", userController.getUsersWithActivePlans);
router.get(
  "/getUserSubscriptionHistory",
  userController.getUserSubscriptionHistory
);

export default router;

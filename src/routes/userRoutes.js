import express from "express";
import { userController } from "../controllers/index.js";

const router = express.Router();

router.get("/getUsersWithActivePlans", userController.getUsersWithActivePlans);
router.get(
  "/getUserSubscriptionHistory",
  userController.getUserSubscriptionHistory
);
router.get("/getUserPoints", userController.getUserPoints);
router.post("/updatePassword", userController.updatePassword);


export default router;

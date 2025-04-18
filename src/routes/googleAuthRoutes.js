import express from "express";
import passport from "passport";
const router = express.Router();

router.get("/google", passport.authenticate("google"));

router.get(
  "/google/callback",
  passport.authenticate("google", { session: false }),
  (req, res) => {
    console.log("req.user>>", req.user)
    res.redirect(`${process.env.FRONTEND_URL}/auth/success?token=${req.user.token}&email=${req.user.email}`);
  }
);

export default router;
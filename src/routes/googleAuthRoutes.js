import express from "express";
import passport from "passport";
const router = express.Router();

router.get("/google", passport.authenticate("google"));

router.get(
  "/google/callback",
  passport.authenticate("google", { session: false }),
  (req, res) => {
    res.redirect(`${process.env.CLIENT_URL}/auth/success?token=${req.user.token}`);
  }
);

export default router;
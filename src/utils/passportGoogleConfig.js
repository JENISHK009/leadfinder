import passport from "passport";
import { Strategy as GoogleStrategy } from "passport-google-oauth20";
import { googleAuthCallback } from "../controllers/googleAuthController.js";

export const configurePassportGoogle = () => {
  passport.use(
    new GoogleStrategy(
      {
        clientID: process.env.GOOGLE_CLIENT_ID,
        clientSecret: process.env.GOOGLE_CLIENT_SECRET,
        callbackURL: "/api/auth/google/callback",
        scope: ["profile", "email"],
        passReqToCallback: true
      },
      googleAuthCallback
    )
  );

  passport.serializeUser((user, done) => {
    done(null, user);
  });

  passport.deserializeUser((user, done) => {
    done(null, user);
  });
};
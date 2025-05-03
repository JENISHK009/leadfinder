import { OTP_CONFIG, USER_ROLES } from "../enum/index.js";

export function generateOTP() {
  return Math.floor(100000 + Math.random() * 900000).toString();
}

export function isOtpExpired(otpTimestamp) {
  if (!otpTimestamp) return true;

  const currentTime = new Date();
  const otpTime = new Date(otpTimestamp);
  const diffInMinutes = (currentTime - otpTime) / (1000 * 60);

  return diffInMinutes > OTP_CONFIG.EXPIRY_MINUTES;
}
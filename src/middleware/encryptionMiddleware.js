import CryptoJS from "crypto-js";
import dotenv from "dotenv";

dotenv.config();

const SECRET_KEY = process.env.ENCRYPTION_KEY || "default_secret_key";

export const encryptData = (data) =>
  CryptoJS.AES.encrypt(JSON.stringify(data), SECRET_KEY).toString();

export const decryptData = (encryptedData) => {
  try {
    const decryptedText = CryptoJS.AES.decrypt(
      encryptedData,
      SECRET_KEY
    ).toString(CryptoJS.enc.Utf8);
    return decryptedText ? JSON.parse(decryptedText) : null;
  } catch {
    return null;
  }
};

export const encryptionMiddleware = (req, res, next) => {
  try {
    console.log("req.body", req.body);
    console.log("req.body?.data", req.body?.data);
    console.log("req.query?.data", req.query?.data);
    if (req.body?.data) {
      const decryptedBody = decryptData(req.body.data);
      console.log("decryptedBody", decryptedBody);
      if (!decryptedBody)
        return res
          .status(400)
          .json({ success: false, message: "Invalid encrypted body data" });
      req.body = decryptedBody;
      console.log("req.body", req.body);
    }

    if (req.query?.data) {
      const decryptedQuery = decryptData(req.query.data);
      if (!decryptedQuery)
        return res
          .status(400)
          .json({ success: false, message: "Invalid encrypted query data" });
      req.query = decryptedQuery;
      console.log("req.query", req.query);
    }

    const originalJson = res.json;
    res.json = (data) => {
      console.log("data", data);
      const success = res.statusCode >= 200 && res.statusCode < 300;
      return originalJson.call(res, {
        success,
        data: encryptData(data),
      });
    };

    next();
  } catch {
    return res
      .status(500)
      .json({ success: false, message: "Error processing encryption" });
  }
};

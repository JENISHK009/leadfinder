import express from "express";
import multer from "multer";
import { leadsController } from "../controllers/index.js";
const upload = multer({ dest: "uploads/" });

const router = express.Router();

router.post(
  "/addPeopleLeadsData",
  leadsController.addPeopleLeadsData
);
router.post("/getPeopleLeads", leadsController.getPeopleLeads);
router.post("/exportPeopleLeadsToCSV", leadsController.exportPeopleLeadsToCSV);
router.post("/deductCreditsFromUser", leadsController.deductCreditsFromUser);
router.post(
  "/addCompaniesData",
  leadsController.addCompaniesData
);
router.post("/getCompanies", leadsController.getCompanies);
router.post("/exportCompaniesToCSV", leadsController.exportCompaniesToCSV);
router.put("/editPeopleLeadsData", leadsController.editPeopleLeadsData);
router.put("/editCompanyLeadData", leadsController.editCompanyLeadData);
router.get("/getCompanyChartData", leadsController.getCompanyChartData);
router.get("/getPeopleLeadsDepartmentChartData", leadsController.getPeopleLeadsDepartmentChartData);
router.get("/getExportedFiles", leadsController.getExportedFiles);
router.post("/saveLeads", leadsController.saveLeads);
router.get("/getSavedLeads", leadsController.getSavedLeads);
router.post("/getselectedLeads", leadsController.getselectedLeads);


export default router;

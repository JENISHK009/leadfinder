
import express from 'express';
import multer from 'multer';
import { leadsController } from '../controllers/index.js'
const upload = multer({ dest: 'uploads/' });

const router = express.Router();

router.post('/insertLeads', upload.single('file'), leadsController.addPeopleLeadsData);
router.post('/getLeads', leadsController.getLeads);
router.post('/exportLeadsToCSV', leadsController.exportLeadsToCSV);
router.post('/deductCreditsFromUser', leadsController.deductCreditsFromUser);




export default router;

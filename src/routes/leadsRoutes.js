
import express from 'express';
import multer from 'multer';
import { addPeopleLeadsData, getLeads } from '../controllers/leadsController.js'
const upload = multer({ dest: 'uploads/' });

const router = express.Router();

router.post('/insertLeads', upload.single('file'), addPeopleLeadsData);
router.post('/getLeads', getLeads);


export default router;

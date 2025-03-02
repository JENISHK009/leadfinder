import express from 'express';
import { planController } from '../controllers/index.js';

const router = express.Router();

router.post('/createPlan', planController.createPlan);
router.delete('/deletePlan', planController.deletePlan);
router.get('/getAllPlans', planController.getAllPlans);
router.get('/getPlanById', planController.getPlanById);
router.put('/updatePlan', planController.updatePlan);
router.put('/buySubscriptionPlan', planController.buySubscriptionPlan);




export default router;

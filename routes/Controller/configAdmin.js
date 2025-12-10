// routes/configAdmin.routes.js
const express = require("express");
const router = express.Router();
const controller = require("../Models/configAdmin.model");
const {configAdmin} = require("../../models/index");

const { authRequired, requireRole } = require('../Auth/authMiddleware');

router.post("/config",authRequired, controller.createConfig);
router.put("/config",authRequired, controller.updateConfig);
router.delete("/config",authRequired, controller.deleteConfig);
router.get("/config",authRequired, async (req, res) => {
  const config = await configAdmin.findOne();
  res.json({ success: true, data: config });
});

module.exports = router;
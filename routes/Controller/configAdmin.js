// routes/configAdmin.routes.js
const express = require("express");
const router = express.Router();
const controller = require("../Models/configAdmin.model");
const {configAdmin} = require("../../models/index");
const { getForexFactoryNews } = require('../Module/Hooks/ForexFactory');

const { authRequired, requireRole } = require('../Auth/authMiddleware');

router.post("/config",authRequired, controller.createConfig);
router.put("/config",authRequired, controller.updateConfig);
router.delete("/config",authRequired, controller.deleteConfig);
router.get("/config",authRequired, async (req, res) => {
  const config = await configAdmin.findOne();
  res.json({ success: true, data: config });
});
// router.get("/calculate",authRequired, getForexFactoryNews);
router.get('/forex-news/impact/:level', async (req, res) => {
    try {
        const { level } = req.params; // high, medium, low, all
        const news = await getForexFactoryNews();
        
        // Nếu level = 'all', trả về tất cả
        const filtered = level.toLowerCase() === 'all' 
            ? news 
            : news.filter(item => 
                item.impactName.toLowerCase() === level.toLowerCase()
              );
        
        res.json({
            success: true,
            impact: level,
            count: filtered.length,
            data: filtered
        });
        
    } catch (error) {
        res.status(500).json({
            success: false,
            message: error.message
        });
    }
});

module.exports = router;
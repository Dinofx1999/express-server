var express = require('express');
var router = express.Router();
var {symbolConfigs , spreadPlus , symbolSetting} = require('../models/index');
const Redis = require('../routes/Module/Redis/clientRedis');
const moment = require('moment-timezone');

/* GET home page. */
router.post('/config', async function(req, res, next) {
    try {
        const newSymbol = new symbolConfigs(req.body);
        await newSymbol.save();
        const Data = await symbolConfigs.find({});
        return res.status(200).json(req.body); // Trả về body, không phải toàn bộ req
    } catch (error) {
        if(error.code === 11000) return res.status(400).json({
            'mess' : "Symbol Đã Tồn Tại",
            'code' : 0
        });
    }
});

router.get('/all', async function(req, res, next) {
    try {
        const Data = await symbolConfigs.find({});
        const Spread_Plus = await spreadPlus.findOne({name_Setting : "Spread_Plus"});
        const Payload = {
            Data: Data,
            Spread_Plus: Spread_Plus
        }
        return res.status(200).json(Payload); // Trả về body, không phải toàn bộ req
    } catch (error) {
        return res.status(400).json(error); 
    }
});

router.post('/getsymbol', async function(req, res, next) {
    try {
        const re = req.body;
        symbolConfigs.findById(re._id).then((result) => {
            return res.status(200).send(result);
          
        });
    } catch (error) {
        console.log("Lỗi");
    }
});

router.post('/update', async function(req, res, next) {
    try {
        
        const re = req.body;
        const Data = {
            Symbol: re.Symbol,
            Spread_STD: re.Spread_STD,
            Spread_ECN: re.Spread_ECN,
            Sydney: re.Sydney,
            Tokyo: re.Tokyo,
            London: re.London,
            NewYork: re.NewYork
        }
        console.log(Data);
        symbolConfigs.findByIdAndUpdate(re._id , Data , {new : true}).then((result) => {
            console.log("Ahi" ,result);
            return res.status(200).send(result);
        });
        
    } catch (error) {
        console.log(error);
    }
});

router.post('/delete', async function(req, res, next) {
    try {
        const re = req.body;
        symbolConfigs.findByIdAndDelete(re._id).then((result) => {
            return res.status(200).send(re);
        });
    } catch (error) {
        console.log("Lỗi");
    }
});

router.get('/:symbol', async function(req, res, next) {
    const symbol = req.params.symbol;
    try {
        symbolConfigs.find({Symbol : symbol}).then((result) => {
            return res.status(200).send(result);
        });
    } catch (error) {
        console.log("Lỗi");
    }
});

router.post('/spread', async function(req, res, next) {
    const Spread = req.body;
    const check_ = await spreadPlus.findOne({name_Setting : "Spread_Plus"});
    try {
        if(check_ === null) {
            const newSpread = new spreadPlus(Spread);
            await newSpread.save();
            res.status(200).send({mess: "Thêm Mới Thành Công"}); // Trả về body, không phải toàn bộ req
        }
        else {
            await spreadPlus.updateOne({name_Setting : "Spread_Plus"} , Spread);
            res.status(200).send({mess: "Update Thành Công"}); 
        }
    } catch (error) {
        res.status(400).send({mess: "Lỗi"});
    }
    
});


//SymbolConfig
router.post('/infosymbol', async function(req, res, next) {
    // const symbol = req.params.symbol;
    console.log(req.body);
    try {
        // symbolconfig.find({Symbol : symbol}).then((result) => {
        //     return res.status(200).send(result);
        // });
        const check = await symbolSetting.findOne({broker : req.body.Broker , symbol : req.body.Symbol});

        if(check === null) {
            const Payload = {
                broker: req.body.Broker,
                symbol: req.body.Symbol,
                time_active: null,
                time_open: null,
                time_close: null,
                time_run: null
            }
            const newSymbol = new symbolSetting(Payload);
            await newSymbol.save();
            return res.status(200).send({mess: "Thêm Mới Thành Công"}); // Trả về body, không phải toàn bộ req
        }else {
            return res.status(200).send({mess: check}); // Trả về body, không phải toàn bộ req
        }
    } catch (error) {
        console.log("Lỗi" , error);
    }
});

router.post('/infosymbol-config', async function(req, res, next) {
    try {
        const payload = req.body;
        console.log("Payload", payload);
        
        // Tìm và cập nhật document
        const check = await symbolSetting.findOneAndUpdate(
            // Query - điều kiện để tìm document
            { broker: payload.broker, symbol: payload.symbol},
            
            // Update - thông tin cần cập nhật
            { 
                time_open: payload.time_open,
                time_close: payload.time_close,
                time_active: payload.time_active,
                time_run: addActiveTime(payload.time_run, payload.time_active),
            },
            // Options
            { new: true, upsert: true }
        );
        
        return res.status(200).json({
            message: check._id ? "Cập nhật thành công" : "Tạo mới thành công",
            data: check
        });
    } catch (error) {
        console.log("Lỗi", error);
        return res.status(500).json({ message: "Lỗi server", error: error.message });
    }
});

function addActiveTime(time_run, time_active) {
    const result = moment(time_run, 'YYYY.MM.DD HH:mm:ss').add(Number(time_active), 'seconds');
    return result.format('YYYY.MM.DD HH:mm:ss');
}

module.exports = router;

var express = require('express');
var router = express.Router();
var {  ErrorAnalysis} = require('../../models/index');

/* GET home page. */


router.get('/all', async function(req, res, next) {
    try {
        const Data = await ErrorAnalysis.find({$expr: { $lt: ["$time_start", "$time_current"] }})
                               .sort({ time_start: -1 });
        return res.status(200).json(Data); // Trả về body, không phải toàn bộ req
    } catch (error) {
        return res.status(400).json(error); 
    }
});

router.post('/getErro_Symbol', async function(req, res, next) {
    try {
        const re = req.body;
        ErrorAnalysis.findOne(re._id).then((result) => {
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
        ErrorAnalysis.findByIdAndUpdate(re._id , Data , {new : true}).then((result) => {
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
        ErrorAnalysis.findByIdAndDelete(re._id).then((result) => {
            return res.status(200).send(re);
        });
    } catch (error) {
        console.log("Lỗi");
    }
});

router.get('/:symbol', async function(req, res, next) {
    const symbol = req.params.symbol;
    try {
        ErrorAnalysis.find({symbol : symbol}).then((result) => {
            return res.status(200).send(result);
        });
    } catch (error) {
        console.log("Lỗi");
    }
});

router.post('/search', async function(req, res, next) {
    const symbol = req.body.symbol;
    const broker = req.body.broker;
    try {
        if(broker !== '' && (symbol === '' || symbol === undefined)){
            console.log("Broker" , broker);
            ErrorAnalysis.find({broker : broker}).then((result) => {
                return res.status(200).send(result);
            });
        }else if((broker === '' || broker === undefined) && symbol !== ''){
            console.log("Symbol" , symbol);
            ErrorAnalysis.find({symbol : symbol}).then((result) => {
                return res.status(200).send(result);
            });
        }else{
            console.log("All" , symbol , broker);
            ErrorAnalysis.find({symbol : symbol , broker : broker}).then((result) => {
                return res.status(200).send(result);
            });
        }
    } catch (error) {
        console.log("Lỗi");
    }
});

module.exports = router;

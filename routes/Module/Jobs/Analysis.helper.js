/* eslint-disable */
function checkBuySignal(data,checkBroker, currentBroker, point) {
  const priceBuyCurrent = Number(currentBroker.ask_mdf) + point;
  const priceBuyCheck = Number(checkBroker.bid);
  
  return {
    succes: priceBuyCurrent < priceBuyCheck,
    signal: 'BUY',
    priceCurrent: priceBuyCurrent,
    priceCheck: priceBuyCheck,
    difference: priceBuyCheck - priceBuyCurrent
  };
}

const symbolsSetType = new Set([
            "XAUUSD", "XAUEUR", "XAGUSD", "XAGEUR", "UKOIL", "WTI",
            "BTCUSD", "ETHUSD", "USDJPY", "USDCAD", "AUDUSD", "EURGBP",
            "EURAUD", "EURCHF", "EURJPY", "GBPCHF", "CADJPY", "GBPJPY",
            "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY", "CHFJPY", "EURNZD",
            "EURCAD", "CADCHF", "NZDJPY", "NZDUSD", "GBPAUD", "GBPCAD",
            "GBPNZD", "NZDCAD", "NZDCHF", "USDCHF", "GBPUSD", "EURUSD","BTCEUR","ETCUSD",
            "LNKUSD","BNBUSD","EOSUSD","BCHUSD","XLMUSD","LTCUSD","AVAUSD","XRPUSD",
            "SOLUSD","DOGEUSD","MATICUSD","TRXUSD","FILUSD","DOTUSD","ADAUSD",
            "XMRUSD","ZECUSD","XEMUSD","XDCUSD","XVGUSD",
          ]);

module.exports = {
  checkBuySignal,
  symbolsSetType
};
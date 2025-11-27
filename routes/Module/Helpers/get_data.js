 
 const Redis = require('../Redis/clientRedis');
 class getData {

// Hoặc nếu sử dụng trong RedisManager
async getDetailedSymbolInfo(symbolName) {
  // Lấy tất cả brokers
  const allBrokers = await Redis.getAllBrokers();
  
  // Mảng kết quả
  const result = [];
  
  // Lặp qua từng broker
  for (const broker of allBrokers) {
    // Tìm symbol trong danh sách Infosymbol
    const foundSymbol = broker.Infosymbol.find(symbol => 
      symbol && symbol.Symbol === symbolName
    );
    
    // Nếu tìm thấy, thêm vào kết quả
    if (foundSymbol) {
      result.push({
        Broker: broker.Broker,
        TypeAccount: broker.TypeAccount,
        Index: broker.Index,
        ...foundSymbol  // Sao chép tất cả thuộc tính của symbol
      });
    }
  }
  
  return result;
}

async getDetailedSymbolInfo_type1( data , symbolName) {
    // Lấy tất cả brokers
    // Mảng kết quả
    const result = [];
    
    // Lặp qua từng broker
    for (const broker of data) {
      // Tìm symbol trong danh sách Infosymbol
      const foundSymbol = broker.Infosymbol.find(symbol => 
        symbol && symbol.Symbol === symbolName
      );
      
      // Nếu tìm thấy, thêm vào kết quả
      if (foundSymbol) {
        result.push({
          Broker: broker.Broker,
          TypeAccount: broker.TypeAccount,
          Index: broker.Index,
          ...foundSymbol  // Sao chép tất cả thuộc tính của symbol
        });
      }
    }
    return result;
  }

}
module.exports = new getData();
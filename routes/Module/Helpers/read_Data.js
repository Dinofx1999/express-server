class Read_Data {

    filterSymbolData(data, symbolToFilter) {
        const result = [];
        
        // Duyệt qua mỗi broker trong dữ liệu
        for (const broker of data) {
          // Tìm symbol trong danh sách Infosymbol
          const foundSymbol = broker.Infosymbol.find(item => item.Symbol === symbolToFilter);
          
          // Nếu tìm thấy, thêm vào kết quả theo định dạng yêu cầu
          if (foundSymbol) {
            result.push({
              Broker: broker.Broker,
              Symbol: foundSymbol.Symbol,
              PriceBid_modify: foundSymbol.PriceBid_modify,
              PriceBid: foundSymbol.PriceBid
            });
          }
        }
        
        return result;
      }
}

module.exports = new Read_Data();
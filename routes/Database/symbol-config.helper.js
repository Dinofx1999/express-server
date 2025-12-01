/* eslint-disable */
const { getCollection } = require('./mongodb');
const { log, colors } = require('../Module/Helpers/Log');

/**
 * Insert một symbol config
 */
async function insertSymbolConfig(symbolData) {
  try {
    const collection = getCollection(String(process.env.SYMBOL_CONFIGS || 'symbol_configs'));
    
    const document = {
      Symbol: symbolData.Symbol,
      Spread_STD: symbolData.Spread_STD,
      Spread_ECN: symbolData.Spread_ECN,
      Sydney: symbolData.Sydney,
      Tokyo: symbolData.Tokyo,
      London: symbolData.London,
      NewYork: symbolData.NewYork,
      createdAt: new Date(),
      updatedAt: new Date()
    };
    
    const result = await collection.insertOne(document);
    log(colors.green, `✅ Inserted: ${symbolData.Symbol}`);
    return result;
  } catch (error) {
    if (error.code === 11000) {
      log(colors.yellow, `⚠️ Symbol exists: ${symbolData.Symbol}`);
    } else {
      log(colors.red, `❌ Insert error:`, error.message);
    }
    throw error;
  }
}

/**
 * Insert nhiều symbol configs
 */
async function insertManySymbolConfigs(symbolsArray) {
  try {
    const collection = getCollection(String(process.env.SYMBOL_CONFIGS || 'symbol_configs'));
    
    const documents = symbolsArray.map(s => ({
      Symbol: s.Symbol,
      Spread_STD: s.Spread_STD,
      Spread_ECN: s.Spread_ECN,
      Sydney: s.Sydney,
      Tokyo: s.Tokyo,
      London: s.London,
      NewYork: s.NewYork,
      createdAt: new Date(),
      updatedAt: new Date()
    }));
    
    const result = await collection.insertMany(documents, { ordered: false });
    log(colors.green, `✅ Inserted ${result.insertedCount} symbols`);
    return result;
  } catch (error) {
    log(colors.red, `❌ InsertMany error:`, error.message);
    throw error;
  }
}

/**
 * Lấy config của một symbol
 */
async function getSymbolConfig(symbol) {
  try {
    const collection = getCollection(String(process.env.SYMBOL_CONFIGS || 'symbol_configs'));
    const result = await collection.findOne({ Symbol: symbol });
    return result;
  } catch (error) {
    log(colors.red, `❌ Get error:`, error.message);
    throw error;
  }
}

/**
 * Lấy tất cả symbol configs
 */
async function getAllSymbolConfigs() {
  try {
    const collection = getCollection('symbol_configs');
    const results = await collection.find({}).toArray();
    return results;
  } catch (error) {
    log(colors.red, `❌ GetAll error:`, error.message);
    throw error;
  }
}

/**
 * Update symbol config
 */
async function updateSymbolConfig(symbol, updates) {
  try {
    const collection = getCollection(String(process.env.SYMBOL_CONFIGS || 'symbol_configs'));
    
    const result = await collection.updateOne(
      { Symbol: symbol },
      { 
        $set: {
          ...updates,
          updatedAt: new Date()
        }
      }
    );
    
    log(colors.green, `✅ Updated: ${symbol}`);
    return result;
  } catch (error) {
    log(colors.red, `❌ Update error:`, error.message);
    throw error;
  }
}

/**
 * Upsert (insert hoặc update)
 */
async function upsertSymbolConfig(symbolData) {
  try {
    const collection = getCollection(String(process.env.SYMBOL_CONFIGS || 'symbol_configs'));
    
    const document = {
      Symbol: symbolData.Symbol,
      Spread_STD: symbolData.Spread_STD,
      Spread_ECN: symbolData.Spread_ECN,
      Sydney: symbolData.Sydney,
      Tokyo: symbolData.Tokyo,
      London: symbolData.London,
      NewYork: symbolData.NewYork,
      updatedAt: new Date()
    };
    
    const result = await collection.updateOne(
      { Symbol: symbolData.Symbol },
      { 
        $set: document,
        $setOnInsert: { createdAt: new Date() }
      },
      { upsert: true }
    );
    
    if (result.upsertedCount > 0) {
      log(colors.green, `✅ Inserted: ${symbolData.Symbol}`);
    } else {
      log(colors.blue, `✅ Updated: ${symbolData.Symbol}`);
    }
    
    return result;
  } catch (error) {
    log(colors.red, `❌ Upsert error:`, error.message);
    throw error;
  }
}

/**
 * Delete symbol config
 */
async function deleteSymbolConfig(symbol) {
  try {
    const collection = getCollection(String(process.env.SYMBOL_CONFIGS || 'symbol_configs'));
    const result = await collection.deleteOne({ Symbol: symbol });
    log(colors.green, `✅ Deleted: ${symbol}`);
    return result;
  } catch (error) {
    log(colors.red, `❌ Delete error:`, error.message);
    throw error;
  }
}

module.exports = {
  insertSymbolConfig,
  insertManySymbolConfigs,
  getSymbolConfig,
  getAllSymbolConfigs,
  updateSymbolConfig,
  upsertSymbolConfig,
  deleteSymbolConfig,
};
/* eslint-disable */
const { MongoClient } = require('mongodb');
const { log, colors } = require('../Module/Helpers/Log');

let client = null;
let db = null;
let isConnected = false;

/**
 * Kết nối MongoDB
 */
async function connectMongoDB() {
  if (isConnected && client) {
    return { client, db };
  }

  try {
    const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017';
    const dbName ='forex_db';

    log(
      colors.blue,
      `${process.env.ICON_ACCESS_LOG} [MongoDB] Connecting...`,
      colors.cyan,
      `DB: ${dbName} | PID: ${process.pid}`
    );

    client = new MongoClient(uri, {
      maxPoolSize: 10,
      minPoolSize: 2,
      maxIdleTimeMS: 30000,
      serverSelectionTimeoutMS: 5000,
      socketTimeoutMS: 45000,
    });

    await client.connect();
    await client.db('admin').command({ ping: 1 });

    db = client.db(dbName);
    isConnected = true;

    log(
      colors.green,
      `${process.env.ICON_ACCESS_LOG} [MongoDB] Connected!`,
      colors.cyan,
      `PID: ${process.pid}`
    );

    return { client, db };

  } catch (error) {
    log(colors.red, `${process.env.ICON_ERROR_LOG} [MongoDB] Connection failed:`, colors.reset, error.message);
    throw error;
  }
}

/**
 * Ngắt kết nối MongoDB
 */
async function disconnectMongoDB() {
  if (client) {
    try {
      await client.close();
      isConnected = false;
      log(colors.yellow, `${process.env.ICON_WARNING_LOG} [MongoDB] Disconnected`, colors.cyan, `PID: ${process.pid}`);
    } catch (error) {
      log(colors.red, `${process.env.ICON_ERROR_LOG} [MongoDB] Disconnect error:`, colors.reset, error.message);
    }
  }
}

/**
 * Lấy database instance
 */
function getDB() {
  if (!isConnected || !db) {
    throw new Error('MongoDB not connected. Call connectMongoDB() first.');
  }

  return db;
}

/**
 * Lấy collection
 */

function getCollection(collectionName) {
  return getDB().collection(collectionName);
}

/**
 * Check connection status
 */
function isMongoConnected() {
  return isConnected;
}

module.exports = {
  connectMongoDB,
  disconnectMongoDB,
  getDB,
  getCollection,
  isMongoConnected,
};
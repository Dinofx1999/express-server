
// Thành CommonJS:
const VERSION = 'v1/api';
const API_BASE_URL = 'https://localhost:3001/';
const API_TIMEOUT = 5000;
const MAX_RETRIES = 3;

const API_ALL_INFO_BROKERS =     VERSION+'/';
const API_PORT_BROKER_ENDPOINT = VERSION+'/:broker/port';
const API_DESTROY_BROKER = VERSION+'/:broker/destroy';
const API_RESET = VERSION+'/:broker/:symbol/reset';
const API_RESET_ALL_ONLY_SYMBOL = VERSION+'/:symbol/reset';
const API_RESET_ALL_BROKERS = VERSION+'/reset-all-brokers';
const API_CONFIG_SYMBOL = VERSION+'/symbol/config';
const API_GET_CONFIG_SYMBOL = VERSION+'/symbol/config/:symbol';
const API_ANALYSIS_CONFIG = VERSION+'/analysis-config';

const API_PRICE_SYMBOL = VERSION+'/price/:symbol';

const API_REGISTER =    '/register';
const API_LOGIN =       '/login';
const API_REFRESH_TOKEN = VERSION+'/auth/refresh-token';

module.exports = {
    VERSION,
    API_BASE_URL,
    API_TIMEOUT,
    MAX_RETRIES,
    API_ALL_INFO_BROKERS,
    API_PORT_BROKER_ENDPOINT,
    API_DESTROY_BROKER,
    API_RESET,
    API_RESET_ALL_ONLY_SYMBOL,
    API_RESET_ALL_BROKERS,
    API_CONFIG_SYMBOL,
    API_GET_CONFIG_SYMBOL,
    API_ANALYSIS_CONFIG,
    API_PRICE_SYMBOL,
    API_REGISTER,
    API_LOGIN,
    API_REFRESH_TOKEN
};

export const VERSION = 'v1/api';
export const API_BASE_URL = 'https://localhost:3001/';
export const API_TIMEOUT = 5000; // in milliseconds
export const MAX_RETRIES = 3;

//LINK API
export const API_ALL_INFO_BROKERS =     VERSION+'/';
export const API_PORT_BROKER_ENDPOINT = VERSION+'/:broker/port';
export const API_DESTROY_BROKER = VERSION+'/:broker/destroy';
export const API_RESET = VERSION+'/:broker/:symbol/reset';
export const API_RESET_ALL_ONLY_SYMBOL = VERSION+'/:symbol/reset';
export const API_RESET_ALL_BROKERS = VERSION+'/reset-all-brokers';
export const API_CONFIG_SYMBOL = VERSION+'/symbol/config';
export const API_GET_CONFIG_SYMBOL = VERSION+'/symbol/config/:symbol';
export const API_ANALYSIS_CONFIG = VERSION+'/analysis-config';

export const API_PRICE_SYMBOL = VERSION+'/price/:symbol';


//AUTH

export const API_REGISTER =     VERSION+'/auth/register';
export const API_LOGIN =        VERSION+'/auth/login';
export const API_REFRESH_TOKEN =VERSION+'/auth/refresh-token';


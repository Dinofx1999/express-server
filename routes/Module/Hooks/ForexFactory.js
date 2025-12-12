const axios = require('axios');
const vm = require('vm');

async function getForexFactoryNews() {
    try {
        
        const url = "https://www.forexfactory.com/calendar?day=today";
        const headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.forexfactory.com/',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Sec-Ch-Ua': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
            'Sec-Ch-Ua-Mobile': '?0',
            'Sec-Ch-Ua-Platform': '"Windows"',
            'Cache-Control': 'max-age=0',
            'DNT': '1'
        };

        const response = await axios.get(url, { headers });
        console.log('ForexFactory response status:', response.status);
        // console.log('ForexFactory response status:', response);
        const html = response.data;

        const regex = /window\.calendarComponentStates\[1\]\s*=\s*({[\s\S]*?});/;
        const match = html.match(regex);

        if (!match) {
            throw new Error('Không tìm thấy dữ liệu');
        }

        const sandbox = { result: null, Object: Object };
        const script = `result = ${match[1]};`;
        vm.createContext(sandbox);
        vm.runInContext(script, sandbox);
        
        const calendarData = sandbox.result;
        const events = calendarData.days[0].events || [];

        // Lấy 4 trường theo yêu cầu
        const result = events.map(event => ({
            impactName: event.impactName,
            timeLabel: event.timeLabel.padEnd(10),
            currency: event.currency,
            name: event.name
        }));

        return result;

    } catch (error) {
        throw error;
    }
}

module.exports = { getForexFactoryNews };
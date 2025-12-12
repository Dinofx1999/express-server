const axios = require('axios');
const vm = require('vm');

async function getForexFactoryNews() {
    try {
        const url = "https://www.forexfactory.com/calendar?day=today";
        const headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        };

        const response = await axios.get(url, { headers });
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
        console.log('Forex Factory News fetched:', result.length, 'items');

        return result;

    } catch (error) {
        throw error;
    }
}

module.exports = { getForexFactoryNews };
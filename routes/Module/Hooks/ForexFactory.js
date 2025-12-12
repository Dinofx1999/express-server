const cloudscraper = require('cloudscraper');
const vm = require('vm');

async function getForexFactoryNews() {
    try {
        console.log('🔄 Using cloudscraper on Windows...');
        
        const html = await cloudscraper.get({
            uri: 'https://www.forexfactory.com/calendar?day=today',
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
        });

        const regex = /window\.calendarComponentStates\[1\]\s*=\s*({[\s\S]*?});/;
        const match = html.match(regex);

        if (!match) throw new Error('No data');

        const sandbox = { result: null, Object: Object };
        vm.createContext(sandbox);
        vm.runInContext(`result = ${match[1]};`, sandbox);
        
        const events = sandbox.result.days[0].events || [];

        return events.map(event => ({
            impactName: event.impactName,
            timeLabel: event.timeLabel.padEnd(10),
            currency: event.currency,
            name: event.name
        }));

    } catch (error) {
        throw error;
    }
}

module.exports = { getForexFactoryNews };
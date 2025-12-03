const LOCK_TTL = 1000;      // Lock timeout 1 giây
const RESULT_TTL = 2000;    // Result cache 2 giây
const WAIT_INTERVAL = 30;   // Interval check (ms)
const MAX_WAIT_TIME = 800;  // Max wait time (ms)

class RequestDeduplicator {
    constructor(redisClient) {
        this.redis = redisClient;
    }

    /**
     * Execute với deduplication - chỉ 1 request được xử lý
     * @param {string} key - Unique key (VD: "RESET:EURUSD")
     * @param {Function} processor - Async function xử lý
     */
    async execute(key, processor) {
        const lockKey = `dedup:lock:${key}`;
        const resultKey = `dedup:result:${key}`;

        // Thử acquire lock
        const acquired = await this.redis.set(lockKey, '1', 'NX', 'PX', LOCK_TTL);

        if (acquired === 'OK') {
            // ═══ LEADER: Xử lý ═══
            // console.log(`\x1b[32m[DEDUP-LEADER]\x1b[0m ${key} - Processing...`);
            
            try {
                await processor();
                await this.redis.set(resultKey, 'done', 'PX', RESULT_TTL);
                // console.log(`\x1b[32m[DEDUP-LEADER]\x1b[0m ${key} - Done!`);
            } catch (error) {
                await this.redis.set(resultKey, 'error', 'PX', RESULT_TTL);
                throw error;
            }
        } else {
            // ═══ WAITER: Đợi leader xong ═══
            // console.log(`\x1b[33m[DEDUP-WAITER]\x1b[0m ${key} - Skipped (already processing)`);
            await this._waitForResult(resultKey);
        }
    }

    async _waitForResult(resultKey) {
        const startTime = Date.now();
        while (Date.now() - startTime < MAX_WAIT_TIME) {
            const cached = await this.redis.get(resultKey);
            if (cached) return;
            await new Promise(r => setTimeout(r, WAIT_INTERVAL));
        }
    }
}

module.exports = RequestDeduplicator;
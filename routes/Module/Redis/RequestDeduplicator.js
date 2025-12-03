const LOCK_TTL = 5000;      // Lock timeout 5 giây (tăng lên)
const RESULT_TTL = 10000;   // Result cache 10 giây
const WAIT_INTERVAL = 50;   // Interval check (ms)
const MAX_WAIT_TIME = 4000; // Max wait time (ms)

class RequestDeduplicator {
    constructor(redisClient) {
        this.redis = redisClient;
    }

    async execute(key, processor) {
        const lockKey = `dedup:lock:${key}`;
        const resultKey = `dedup:result:${key}`;

        // ══════════════════════════════════════════════
        // FIX 1: Check result TRƯỚC - nếu có rồi thì skip luôn
        // ══════════════════════════════════════════════
        const existingResult = await this.redis.get(resultKey);
        if (existingResult === 'done') {
            console.log(`[DEDUP-CACHED] ${key} - Already processed, skipping`);
            return { status: 'cached' };
        }

        // Thử acquire lock
        const acquired = await this.redis.set(lockKey, '1', 'NX', 'PX', LOCK_TTL);

        if (acquired === 'OK') {
            // ═══ LEADER: Xử lý ═══
            console.log(`[DEDUP-LEADER] ${key} - Processing...`);
            
            try {
                const result = await processor();
                
                // Set result TRƯỚC khi release lock (implicit qua TTL)
                await this.redis.set(resultKey, 'done', 'PX', RESULT_TTL);
                
                console.log(`[DEDUP-LEADER] ${key} - Done!`);
                return { status: 'processed', result };
                
            } catch (error) {
                await this.redis.set(resultKey, 'error', 'PX', RESULT_TTL);
                // Xóa lock để request khác có thể retry
                await this.redis.del(lockKey);
                throw error;
            }
        } else {
            // ═══ WAITER: Đợi leader xong ═══
            console.log(`[DEDUP-WAITER] ${key} - Waiting for leader...`);
            
            const waitResult = await this._waitForResult(resultKey);
            
            if (waitResult === 'done') {
                console.log(`[DEDUP-WAITER] ${key} - Leader finished, skipping`);
                return { status: 'skipped' };
            } else if (waitResult === 'error') {
                throw new Error(`Leader failed for ${key}`);
            } else {
                // FIX 3: Timeout - có thể retry hoặc throw
                console.log(`[DEDUP-WAITER] ${key} - Timeout waiting`);
                throw new Error(`Timeout waiting for ${key}`);
            }
        }
    }

    async _waitForResult(resultKey) {
        const startTime = Date.now();
        
        while (Date.now() - startTime < MAX_WAIT_TIME) {
            const cached = await this.redis.get(resultKey);
            if (cached) return cached;  // Trả về 'done' hoặc 'error'
            await new Promise(r => setTimeout(r, WAIT_INTERVAL));
        }
        
        return null;  // Timeout
    }
}

module.exports = RequestDeduplicator;
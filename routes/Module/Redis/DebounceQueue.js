/**
 * UniqueDebounceQueue
 * - Track unique payloads (dedupe by content)
 * - Chỉ reset timer khi có payload MỚI
 * - Xử lý tất cả unique payloads sau debounce
 * - Serial queue execution
 */

const DEBOUNCE_TIME = 3000;   // 3 giây không có message mới
const MAX_WAIT_TIME = 15000;  // Tối đa 15 giây từ message đầu tiên
const MAX_PAYLOADS = 500;     // Tối đa 500 unique payloads

class UniqueDebounceQueue {
    constructor(options = {}) {
        this.debounceTime = options.debounceTime || DEBOUNCE_TIME;
        this.maxWaitTime = options.maxWaitTime || MAX_WAIT_TIME;
        this.maxPayloads = options.maxPayloads || MAX_PAYLOADS;
        
        // Tracking per key
        this.timers = new Map();           // key -> debounce timeoutId
        this.maxTimers = new Map();        // key -> max wait timeoutId
        this.firstTime = new Map();        // key -> timestamp message đầu tiên
        this.uniquePayloads = new Map();   // key -> Map<hash, payload>
        this.processors = new Map();       // key -> processor function
        
        // Queue
        this.queue = [];
        this.isProcessing = false;
        this.currentTask = null;
    }

    /**
     * Tạo hash từ payload để check duplicate
     */
    _createHash(payload) {
        // Sắp xếp keys để đảm bảo consistent hash
        const sorted = Object.keys(payload)
            .sort()
            .reduce((acc, key) => {
                acc[key] = payload[key];
                return acc;
            }, {});
        return JSON.stringify(sorted);
    }

    /**
     * Nhận message
     * @param {string} key - Group key (VD: "RESET:EURUSD" hoặc chỉ "RESET")
     * @param {Object} payload - Payload data (VD: {symbol: 'EURUSD', broker: 'ABC'})
     * @param {Function} processor - Function xử lý, nhận array of payloads
     */
    receive(key, payload, processor) {
        const now = Date.now();
        const hash = this._createHash(payload);
        
        // Lấy hoặc tạo Map cho key này
        if (!this.uniquePayloads.has(key)) {
            this.uniquePayloads.set(key, new Map());
        }
        const payloadsMap = this.uniquePayloads.get(key);
        
        // ══════════════════════════════════════════════
        // CHECK: Duplicate payload → Ignore
        // ══════════════════════════════════════════════
        if (payloadsMap.has(hash)) {
            console.log(`[DEBOUNCE] ${key} - Duplicate payload, ignoring:`, payload);
            return { 
                status: 'duplicate', 
                uniqueCount: payloadsMap.size,
                payload 
            };
        }

        // ══════════════════════════════════════════════
        // NEW unique payload
        // ══════════════════════════════════════════════
        payloadsMap.set(hash, payload);
        this.processors.set(key, processor);
        
        const isFirstPayload = !this.firstTime.has(key);
        const uniqueCount = payloadsMap.size;

        console.log(`[DEBOUNCE] ${key} - New unique #${uniqueCount}:`, payload);

        // ══════════════════════════════════════════════
        // CHECK: Max payloads → Force execute
        // ══════════════════════════════════════════════
        if (uniqueCount >= this.maxPayloads) {
            console.log(`[DEBOUNCE] ${key} - Max payloads reached (${uniqueCount}), force execute!`);
            this._clearTimers(key);
            this._addToQueue(key);
            return { 
                status: 'max_payloads_reached', 
                uniqueCount,
                action: 'force_execute'
            };
        }

        // ══════════════════════════════════════════════
        // First payload → Set max wait timer
        // ══════════════════════════════════════════════
        if (isFirstPayload) {
            this.firstTime.set(key, now);
            
            const maxTimeoutId = setTimeout(() => {
                console.log(`[DEBOUNCE] ${key} - Max wait time reached, force execute!`);
                this._clearTimers(key);
                this._addToQueue(key);
            }, this.maxWaitTime);
            
            this.maxTimers.set(key, maxTimeoutId);
        }

        // ══════════════════════════════════════════════
        // Reset debounce timer (vì có payload MỚI)
        // ══════════════════════════════════════════════
        if (this.timers.has(key)) {
            clearTimeout(this.timers.get(key));
        }

        const timeoutId = setTimeout(() => {
            this._clearTimers(key);
            this._addToQueue(key);
        }, this.debounceTime);

        this.timers.set(key, timeoutId);

        // Tính thời gian
        const elapsed = now - (this.firstTime.get(key) || now);
        const remainingMaxWait = Math.max(0, this.maxWaitTime - elapsed);

        return { 
            status: 'accepted', 
            uniqueCount,
            remainingMaxWait,
            willExecuteIn: Math.min(this.debounceTime, remainingMaxWait)
        };
    }

    /**
     * Clear timers
     */
    _clearTimers(key) {
        if (this.timers.has(key)) {
            clearTimeout(this.timers.get(key));
            this.timers.delete(key);
        }
        if (this.maxTimers.has(key)) {
            clearTimeout(this.maxTimers.get(key));
            this.maxTimers.delete(key);
        }
    }

    /**
     * Thêm vào queue
     */
    _addToQueue(key) {
        const payloadsMap = this.uniquePayloads.get(key);
        const processor = this.processors.get(key);
        const firstTime = this.firstTime.get(key);
        const totalWaitTime = firstTime ? Date.now() - firstTime : 0;

        // Lấy tất cả unique payloads
        const payloads = payloadsMap ? Array.from(payloadsMap.values()) : [];
        const uniqueCount = payloads.length;

        // Clear tracking
        this.timers.delete(key);
        this.maxTimers.delete(key);
        this.firstTime.delete(key);
        this.uniquePayloads.delete(key);
        this.processors.delete(key);

        if (!processor || uniqueCount === 0) {
            console.warn(`[QUEUE] ${key} - No processor or empty payloads, skipping`);
            return;
        }

        // Check duplicate in queue
        if (this.queue.some(item => item.key === key)) {
            console.log(`[QUEUE] ${key} - Already in queue, skipping`);
            return;
        }

        // Add to queue
        this.queue.push({ key, payloads, uniqueCount, processor, totalWaitTime });
        console.log(`[QUEUE] ${key} - Added (${uniqueCount} unique, waited ${totalWaitTime}ms). Queue: ${this.queue.length}`);

        this._processNext();
    }

    /**
     * Process next
     */
    async _processNext() {
        if (this.isProcessing || this.queue.length === 0) {
            return;
        }

        this.isProcessing = true;
        this.currentTask = this.queue.shift();

        const { key, payloads, uniqueCount, processor, totalWaitTime } = this.currentTask;

        console.log(`[PROCESS] 🚀 ${key} - Start (${uniqueCount} unique, waited ${totalWaitTime}ms)`);

        try {
            // Gọi processor với ARRAY of payloads
            await processor(payloads, { 
                key, 
                uniqueCount, 
                totalWaitTime 
            });
            console.log(`[PROCESS] ✅ ${key} - Done!`);
        } catch (error) {
            console.error(`[PROCESS] ❌ ${key} - Error:`, error.message);
        }

        this.isProcessing = false;
        this.currentTask = null;

        this._processNext();
    }

    /**
     * Check if key is pending
     */
    isPending(key) {
        return this.uniquePayloads.has(key) || 
               this.queue.some(item => item.key === key) ||
               this.currentTask?.key === key;
    }

    /**
     * Get unique count for a key
     */
    getUniqueCount(key) {
        const payloadsMap = this.uniquePayloads.get(key);
        return payloadsMap ? payloadsMap.size : 0;
    }

    /**
     * Get status
     */
    getStatus() {
        const pending = {};
        for (const [key, payloadsMap] of this.uniquePayloads) {
            const firstTime = this.firstTime.get(key);
            pending[key] = {
                uniqueCount: payloadsMap.size,
                payloads: Array.from(payloadsMap.values()),
                waitedMs: firstTime ? Date.now() - firstTime : 0,
                remainingMaxWait: firstTime 
                    ? Math.max(0, this.maxWaitTime - (Date.now() - firstTime)) 
                    : this.maxWaitTime
            };
        }

        return {
            isProcessing: this.isProcessing,
            currentTask: this.currentTask ? {
                key: this.currentTask.key,
                uniqueCount: this.currentTask.uniqueCount
            } : null,
            queueLength: this.queue.length,
            queueItems: this.queue.map(item => ({ 
                key: item.key, 
                uniqueCount: item.uniqueCount 
            })),
            pendingDebounce: pending
        };
    }

    /**
     * Force execute
     */
    forceExecute(key) {
        if (this.uniquePayloads.has(key)) {
            this._clearTimers(key);
            this._addToQueue(key);
            return true;
        }
        return false;
    }

    /**
     * Cancel
     */
    cancel(key) {
        this._clearTimers(key);
        this.firstTime.delete(key);
        this.uniquePayloads.delete(key);
        this.processors.delete(key);
        return true;
    }

    /**
     * Destroy
     */
    destroy() {
        for (const timeoutId of this.timers.values()) {
            clearTimeout(timeoutId);
        }
        for (const timeoutId of this.maxTimers.values()) {
            clearTimeout(timeoutId);
        }
        this.timers.clear();
        this.maxTimers.clear();
        this.firstTime.clear();
        this.uniquePayloads.clear();
        this.processors.clear();
        this.queue = [];
    }
}

module.exports = UniqueDebounceQueue;
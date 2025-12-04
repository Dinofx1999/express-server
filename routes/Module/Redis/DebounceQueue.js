/**
 * SymbolDebounceQueue - Final Version
 * 
 * Logic:
 * - Track duplicate bằng FULL PAYLOAD (Symbol + Broker)
 * - Duplicate payload → ignore, không reset timer
 * - New payload → accept, reset timer
 * - Process → Mỗi Symbol chỉ xử lý 1 LẦN (dedupe by symbol)
 */

const DEBOUNCE_TIME = 3000;       // 3s không có payload mới
const MAX_WAIT_TIME = 15000;      // Tối đa 15s
const MAX_PAYLOADS = 500;         // Tối đa 500 unique payloads
const DELAY_BETWEEN_TASKS = 60;   // 60ms delay giữa các task

class SymbolDebounceQueue {
    constructor(options = {}) {
        this.debounceTime = options.debounceTime || DEBOUNCE_TIME;
        this.maxWaitTime = options.maxWaitTime || MAX_WAIT_TIME;
        this.maxPayloads = options.maxPayloads || MAX_PAYLOADS;
        this.delayBetweenTasks = options.delayBetweenTasks || DELAY_BETWEEN_TASKS;
        
        // Tracking per group key
        this.timers = new Map();           // groupKey -> debounce timeoutId
        this.maxTimers = new Map();        // groupKey -> max wait timeoutId
        this.firstTime = new Map();        // groupKey -> timestamp payload đầu tiên
        this.uniquePayloads = new Map();   // groupKey -> Map<hash, payload>
        this.processors = new Map();       // groupKey -> processor function
        this.receivedCounts = new Map();   // groupKey -> total messages received
        
        // Queue
        this.queue = [];
        this.isProcessing = false;
        this.currentTask = null;
    }

    /**
     * Tạo hash từ payload để check duplicate
     * Hash = Symbol + Broker (hoặc các field quan trọng)
     */
    _createHash(payload) {
        // Chỉ hash các field quan trọng để xác định duplicate
        const keyFields = {
            symbol: payload.symbol || payload.Symbol || '',
            broker: payload.broker || payload.Broker || ''
        };
        return `${keyFields.symbol}:${keyFields.broker}`;
    }

    /**
     * Extract symbol từ payload
     */
    _extractSymbol(payload) {
        return payload.symbol || payload.Symbol || '';
    }

    /**
     * Nhận message
     * @param {string} groupKey - Group key (VD: "RESET")
     * @param {Object} payload - Payload data (VD: {symbol: 'EURUSD', broker: 'ABC'})
     * @param {Function} processor - Function xử lý, nhận (symbol, meta)
     */
    receive(groupKey, payload, processor) {
        const now = Date.now();
        const hash = this._createHash(payload);
        const symbol = this._extractSymbol(payload);
        
        // Init maps nếu chưa có
        if (!this.uniquePayloads.has(groupKey)) {
            this.uniquePayloads.set(groupKey, new Map());
        }
        if (!this.receivedCounts.has(groupKey)) {
            this.receivedCounts.set(groupKey, 0);
        }
        
        // Tăng received count
        const receivedCount = this.receivedCounts.get(groupKey) + 1;
        this.receivedCounts.set(groupKey, receivedCount);
        
        const payloadsMap = this.uniquePayloads.get(groupKey);
        
        // ══════════════════════════════════════════════
        // CHECK: Duplicate payload (same Symbol + Broker) → IGNORE
        // ══════════════════════════════════════════════
        if (payloadsMap.has(hash)) {
            // console.log(`[DEBOUNCE] ${groupKey} - Duplicate payload (#${receivedCount}):`, hash);
            return { 
                status: 'duplicate', 
                hash,
                symbol,
                uniqueCount: payloadsMap.size,
                receivedCount
            };
        }

        // ══════════════════════════════════════════════
        // NEW unique payload → Accept và reset timer
        // ══════════════════════════════════════════════
        payloadsMap.set(hash, {
            ...payload,
            symbol,
            _hash: hash,
            _receivedAt: now
        });
        this.processors.set(groupKey, processor);
        
        const isFirstPayload = !this.firstTime.has(groupKey);
        const uniqueCount = payloadsMap.size;

        console.log(`[DEBOUNCE] ${groupKey} - NEW unique #${uniqueCount}: ${hash} (total: ${receivedCount})`);

        // ══════════════════════════════════════════════
        // CHECK: Max payloads → Force execute
        // ══════════════════════════════════════════════
        if (uniqueCount >= this.maxPayloads) {
            console.log(`[DEBOUNCE] ${groupKey} - Max payloads reached (${uniqueCount}), force execute!`);
            this._clearTimers(groupKey);
            this._addToQueue(groupKey);
            return { 
                status: 'max_payloads_reached', 
                hash,
                symbol,
                uniqueCount,
                action: 'force_execute'
            };
        }

        // ══════════════════════════════════════════════
        // First payload → Set max wait timer
        // ══════════════════════════════════════════════
        if (isFirstPayload) {
            this.firstTime.set(groupKey, now);
            
            const maxTimeoutId = setTimeout(() => {
                console.log(`[DEBOUNCE] ${groupKey} - Max wait time reached (${this.maxWaitTime}ms), force execute!`);
                this._clearTimers(groupKey);
                this._addToQueue(groupKey);
            }, this.maxWaitTime);
            
            this.maxTimers.set(groupKey, maxTimeoutId);
        }

        // ══════════════════════════════════════════════
        // Reset debounce timer (vì có payload MỚI)
        // ══════════════════════════════════════════════
        if (this.timers.has(groupKey)) {
            clearTimeout(this.timers.get(groupKey));
        }

        const timeoutId = setTimeout(() => {
            this._clearTimers(groupKey);
            this._addToQueue(groupKey);
        }, this.debounceTime);

        this.timers.set(groupKey, timeoutId);

        // Tính thời gian
        const elapsed = now - (this.firstTime.get(groupKey) || now);
        const remainingMaxWait = Math.max(0, this.maxWaitTime - elapsed);

        return { 
            status: 'accepted', 
            hash,
            symbol,
            uniqueCount,
            receivedCount,
            remainingMaxWait,
            willExecuteIn: Math.min(this.debounceTime, remainingMaxWait)
        };
    }

    /**
     * Clear timers for a group
     */
    _clearTimers(groupKey) {
        if (this.timers.has(groupKey)) {
            clearTimeout(this.timers.get(groupKey));
            this.timers.delete(groupKey);
        }
        if (this.maxTimers.has(groupKey)) {
            clearTimeout(this.maxTimers.get(groupKey));
            this.maxTimers.delete(groupKey);
        }
    }

    /**
     * Thêm vào queue - DEDUPE theo Symbol (mỗi symbol 1 task)
     */
    _addToQueue(groupKey) {
        const payloadsMap = this.uniquePayloads.get(groupKey);
        const processor = this.processors.get(groupKey);
        const firstTime = this.firstTime.get(groupKey);
        const receivedCount = this.receivedCounts.get(groupKey) || 0;
        const totalWaitTime = firstTime ? Date.now() - firstTime : 0;

        // Lấy tất cả payloads
        const allPayloads = payloadsMap ? Array.from(payloadsMap.values()) : [];
        const uniquePayloadCount = allPayloads.length;

        // ══════════════════════════════════════════════
        // DEDUPE theo Symbol - Mỗi symbol chỉ xử lý 1 lần
        // ══════════════════════════════════════════════
        const symbolMap = new Map(); // symbol -> { symbol, brokers: [...], payloads: [...] }
        
        for (const payload of allPayloads) {
            const symbol = payload.symbol;
            if (!symbolMap.has(symbol)) {
                symbolMap.set(symbol, {
                    symbol,
                    brokers: [],
                    payloads: [],
                    firstPayload: payload
                });
            }
            const entry = symbolMap.get(symbol);
            entry.brokers.push(payload.broker || payload.Broker || 'unknown');
            entry.payloads.push(payload);
        }

        const uniqueSymbolCount = symbolMap.size;

        // Clear tracking cho group này
        this.timers.delete(groupKey);
        this.maxTimers.delete(groupKey);
        this.firstTime.delete(groupKey);
        this.uniquePayloads.delete(groupKey);
        this.processors.delete(groupKey);
        this.receivedCounts.delete(groupKey);

        if (!processor || uniqueSymbolCount === 0) {
            console.warn(`[QUEUE] ${groupKey} - No processor or empty, skipping`);
            return;
        }

        // Thêm từng SYMBOL vào queue (không phải từng payload)
        let addedCount = 0;
        for (const [symbol, data] of symbolMap) {
            // Check duplicate trong queue
            const isDuplicate = this.queue.some(
                item => item.groupKey === groupKey && item.symbol === symbol
            );
            
            if (!isDuplicate) {
                this.queue.push({ 
                    groupKey,
                    symbol,
                    brokers: data.brokers,           // Danh sách brokers đã gửi
                    payloads: data.payloads,         // Tất cả payloads của symbol này
                    firstPayload: data.firstPayload, // Payload đầu tiên
                    processor,
                    totalWaitTime,
                    receivedCount,
                    uniquePayloadCount,
                    uniqueSymbolCount
                });
                addedCount++;
            }
        }

        console.log(`[QUEUE] ${groupKey} - Added ${addedCount} symbols (from ${uniquePayloadCount} payloads, ${receivedCount} msgs). Queue: ${this.queue.length}`);

        this._processNext();
    }

    /**
     * Process next task với delay
     */
    async _processNext() {
        if (this.isProcessing || this.queue.length === 0) {
            return;
        }

        this.isProcessing = true;
        this.currentTask = this.queue.shift();

        const { 
            groupKey, 
            symbol, 
            brokers, 
            payloads, 
            firstPayload,
            processor, 
            totalWaitTime,
            uniquePayloadCount,
            uniqueSymbolCount
        } = this.currentTask;

        console.log(`[PROCESS] 🚀 ${groupKey}:${symbol} - Start (${brokers.length} brokers). Remaining: ${this.queue.length}`);

        try {
            await processor(symbol, { 
                groupKey,
                symbol,
                brokers,                  // ['ABC', '123', 'XYZ'] - tất cả brokers đã gửi symbol này
                payloads,                 // Tất cả payloads của symbol này
                firstPayload,             // Payload đầu tiên
                brokerCount: brokers.length,
                totalWaitTime,
                queueRemaining: this.queue.length
            });
            console.log(`[PROCESS] ✅ ${groupKey}:${symbol} - Done!`);
        } catch (error) {
            console.error(`[PROCESS] ❌ ${groupKey}:${symbol} - Error:`, error.message);
        }

        this.isProcessing = false;
        this.currentTask = null;

        // ══════════════════════════════════════════════
        // DELAY trước khi xử lý task tiếp theo
        // ══════════════════════════════════════════════
        if (this.queue.length > 0 && this.delayBetweenTasks > 0) {
            await this._delay(this.delayBetweenTasks);
        }

        this._processNext();
    }

    /**
     * Delay helper
     */
    _delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * Check if payload is pending (by hash)
     */
    isPendingPayload(groupKey, payload) {
        const hash = this._createHash(payload);
        const payloadsMap = this.uniquePayloads.get(groupKey);
        if (payloadsMap && payloadsMap.has(hash)) return true;
        
        // Check in queue
        const symbol = this._extractSymbol(payload);
        return this.queue.some(item => 
            item.groupKey === groupKey && 
            item.payloads.some(p => this._createHash(p) === hash)
        );
    }

    /**
     * Check if symbol is pending
     */
    isPendingSymbol(groupKey, symbol) {
        // Check trong debounce pending
        const payloadsMap = this.uniquePayloads.get(groupKey);
        if (payloadsMap) {
            for (const payload of payloadsMap.values()) {
                if (payload.symbol === symbol) return true;
            }
        }
        
        // Check trong queue
        if (this.queue.some(item => item.groupKey === groupKey && item.symbol === symbol)) {
            return true;
        }
        
        // Check đang processing
        if (this.currentTask?.groupKey === groupKey && this.currentTask?.symbol === symbol) {
            return true;
        }
        
        return false;
    }

    /**
     * Get status
     */
    getStatus() {
        const pending = {};
        for (const [groupKey, payloadsMap] of this.uniquePayloads) {
            const firstTime = this.firstTime.get(groupKey);
            
            // Group payloads by symbol
            const symbolGroups = {};
            for (const payload of payloadsMap.values()) {
                const sym = payload.symbol;
                if (!symbolGroups[sym]) {
                    symbolGroups[sym] = { brokers: [], count: 0 };
                }
                symbolGroups[sym].brokers.push(payload.broker || payload.Broker);
                symbolGroups[sym].count++;
            }
            
            pending[groupKey] = {
                uniquePayloads: payloadsMap.size,
                uniqueSymbols: Object.keys(symbolGroups).length,
                symbols: symbolGroups,
                receivedCount: this.receivedCounts.get(groupKey) || 0,
                waitedMs: firstTime ? Date.now() - firstTime : 0,
                remainingMaxWait: firstTime 
                    ? Math.max(0, this.maxWaitTime - (Date.now() - firstTime)) 
                    : this.maxWaitTime
            };
        }

        return {
            config: {
                debounceTime: this.debounceTime,
                maxWaitTime: this.maxWaitTime,
                maxPayloads: this.maxPayloads,
                delayBetweenTasks: this.delayBetweenTasks
            },
            isProcessing: this.isProcessing,
            currentTask: this.currentTask ? {
                groupKey: this.currentTask.groupKey,
                symbol: this.currentTask.symbol,
                brokers: this.currentTask.brokers
            } : null,
            queueLength: this.queue.length,
            queueItems: this.queue.map(item => ({
                groupKey: item.groupKey,
                symbol: item.symbol,
                brokerCount: item.brokers.length
            })),
            pendingDebounce: pending
        };
    }

    /**
     * Force execute một group ngay lập tức
     */
    forceExecute(groupKey) {
        if (this.uniquePayloads.has(groupKey)) {
            console.log(`[FORCE] ${groupKey} - Force executing...`);
            this._clearTimers(groupKey);
            this._addToQueue(groupKey);
            return true;
        }
        return false;
    }

    /**
     * Force execute tất cả groups
     */
    forceExecuteAll() {
        const groups = Array.from(this.uniquePayloads.keys());
        for (const groupKey of groups) {
            this.forceExecute(groupKey);
        }
        return groups.length;
    }

    /**
     * Cancel một group
     */
    cancel(groupKey) {
        this._clearTimers(groupKey);
        this.firstTime.delete(groupKey);
        this.uniquePayloads.delete(groupKey);
        this.processors.delete(groupKey);
        this.receivedCounts.delete(groupKey);
        console.log(`[CANCEL] ${groupKey} - Cancelled`);
        return true;
    }

    /**
     * Clear queue
     */
    clearQueue() {
        const cleared = this.queue.length;
        this.queue = [];
        console.log(`[CLEAR] Cleared ${cleared} items from queue`);
        return cleared;
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
        this.receivedCounts.clear();
        this.queue = [];
        this.isProcessing = false;
        this.currentTask = null;
        
        console.log(`[DESTROY] SymbolDebounceQueue destroyed`);
    }
}

module.exports = SymbolDebounceQueue;
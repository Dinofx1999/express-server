/**
 * DebounceQueue - Debounce + Serial Queue Execution
 * - Debounce: Đợi N giây không có message mới
 * - Queue: Chỉ xử lý 1 task tại 1 thời điểm
 */

const DEBOUNCE_TIME = 3000;  // 3 giây

class DebounceQueue {
    constructor(options = {}) {
        this.debounceTime = options.debounceTime || DEBOUNCE_TIME;
        
        // Debounce tracking
        this.timers = new Map();      // key -> timeoutId
        this.counts = new Map();      // key -> message count
        this.lastData = new Map();    // key -> last message data
        this.processors = new Map();  // key -> processor function
        
        // Queue
        this.queue = [];              // Array of { key, data, count, processor }
        this.isProcessing = false;    // Flag: đang xử lý hay không
        this.currentTask = null;      // Task đang xử lý
    }

    /**
     * Nhận message và debounce
     */
    receive(key, data, processor) {
        // Tăng count
        const currentCount = (this.counts.get(key) || 0) + 1;
        this.counts.set(key, currentCount);
        
        // Lưu data và processor mới nhất
        this.lastData.set(key, data);
        this.processors.set(key, processor);

        // Clear timer cũ (nếu có)
        if (this.timers.has(key)) {
            clearTimeout(this.timers.get(key));
        }

        // Set timer mới
        const timeoutId = setTimeout(() => {
            this._addToQueue(key);
        }, this.debounceTime);

        this.timers.set(key, timeoutId);

        console.log(`[DEBOUNCE] ${key} - Received #${currentCount}, waiting ${this.debounceTime}ms...`);
        
        return { 
            status: 'queued', 
            count: currentCount,
            queueLength: this.queue.length,
            isProcessing: this.isProcessing
        };
    }

    /**
     * Thêm vào queue sau khi debounce xong
     */
    _addToQueue(key) {
        const count = this.counts.get(key) || 0;
        const data = this.lastData.get(key);
        const processor = this.processors.get(key);

        // Clear debounce tracking
        this.timers.delete(key);
        this.counts.delete(key);
        this.lastData.delete(key);
        this.processors.delete(key);

        if (!processor) {
            console.warn(`[QUEUE] ${key} - No processor found, skipping`);
            return;
        }

        // Kiểm tra đã có trong queue chưa (tránh duplicate)
        const existsInQueue = this.queue.some(item => item.key === key);
        if (existsInQueue) {
            console.log(`[QUEUE] ${key} - Already in queue, skipping`);
            return;
        }

        // Thêm vào queue
        this.queue.push({ key, data, count, processor });
        console.log(`[QUEUE] ${key} - Added to queue (${count} messages). Queue size: ${this.queue.length}`);

        // Bắt đầu xử lý nếu chưa chạy
        this._processNext();
    }

    /**
     * Xử lý task tiếp theo trong queue
     */
    async _processNext() {
        // Nếu đang xử lý hoặc queue rỗng -> return
        if (this.isProcessing || this.queue.length === 0) {
            return;
        }

        this.isProcessing = true;
        this.currentTask = this.queue.shift();

        const { key, data, count, processor } = this.currentTask;

        console.log(`[PROCESS] 🚀 ${key} - Starting (${count} messages). Remaining: ${this.queue.length}`);

        try {
            await processor(data, { count, key });
            console.log(`[PROCESS] ✅ ${key} - Completed!`);
        } catch (error) {
            console.error(`[PROCESS] ❌ ${key} - Error:`, error.message);
        }

        this.isProcessing = false;
        this.currentTask = null;

        // Xử lý task tiếp theo (nếu có)
        this._processNext();
    }

    /**
     * Lấy trạng thái hiện tại
     */
    getStatus() {
        return {
            isProcessing: this.isProcessing,
            currentTask: this.currentTask?.key || null,
            queueLength: this.queue.length,
            queueItems: this.queue.map(item => item.key),
            pendingDebounce: Array.from(this.timers.keys())
        };
    }

    /**
     * Force execute một key ngay (bỏ qua debounce)
     */
    forceExecute(key) {
        if (this.timers.has(key)) {
            clearTimeout(this.timers.get(key));
            this._addToQueue(key);
            return true;
        }
        return false;
    }

    /**
     * Cancel debounce cho một key
     */
    cancelDebounce(key) {
        if (this.timers.has(key)) {
            clearTimeout(this.timers.get(key));
            this.timers.delete(key);
            this.counts.delete(key);
            this.lastData.delete(key);
            this.processors.delete(key);
            console.log(`[CANCEL] ${key} - Debounce cancelled`);
            return true;
        }
        return false;
    }

    /**
     * Clear toàn bộ queue (không clear task đang chạy)
     */
    clearQueue() {
        const cleared = this.queue.length;
        this.queue = [];
        console.log(`[CLEAR] Cleared ${cleared} items from queue`);
        return cleared;
    }

    /**
     * Cleanup khi shutdown
     */
    destroy() {
        // Clear all debounce timers
        for (const timeoutId of this.timers.values()) {
            clearTimeout(timeoutId);
        }
        this.timers.clear();
        this.counts.clear();
        this.lastData.clear();
        this.processors.clear();
        this.queue = [];
        console.log('[DESTROY] DebounceQueue destroyed');
    }
}

module.exports = DebounceQueue;
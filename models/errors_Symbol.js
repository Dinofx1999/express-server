const mongoose = require('mongoose');

const ErrorAnalysisSchema = new mongoose.Schema({
    Broker: {
        type: String,
        required: true,
    },
    TimeStart: {
        type: String,
        required: true,
    },
    TimeCurrent: {
        type: String,
        required: true,
    },
    Symbol: {
        type: String,
        required: true,
    },
    Count: {
        type: Number,
        default: 0,
    },
    Messenger: {
        type: String,
        enum: ['BUY', 'SELL'],
        required: true,
    },
    Broker_Main: {
        type: String,
        required: true,
    },
    KhoangCach: {
        type: Number,
        required: true,
    },
    Symbol_Raw: {
        type: String,
        required: false,
    },
    Spread_main: {
        type: Number,
        required: true,
    },
    Spread_Sync: {
        type: Number,
        required: true,
    },
    IsStable: {
        type: Boolean,
        default: false,
    },
    Type: {
        type: String,
        default: 'Delay Price',
    },
    Delay: {
        type: Number,
        required: true,
    }
}, {
    timestamps: true,
});

// Index để query nhanh hơn
ErrorAnalysisSchema.index({ Symbol: 1, Broker: 1 });
ErrorAnalysisSchema.index({ Messenger: 1 });
ErrorAnalysisSchema.index({ TimeStart: -1 });

module.exports = mongoose.model(String(process.env.ANALYSIS_DB) || 'ANALYSIS_DB', ErrorAnalysisSchema);
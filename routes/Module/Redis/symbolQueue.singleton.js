const SymbolDebounceQueue = require('./DebounceQueue');

const queue = new SymbolDebounceQueue({
  debounceTime: 50,
  maxWaitTime: 80,
  maxPayloads: 400,
  delayBetweenTasks: 60,
  cooldownTime: 2000,
});

module.exports = queue;

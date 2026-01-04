function isForexSymbol(symbol) {
  if (!symbol || symbol.length !== 6) return false;

  const FX = ['USD','EUR','JPY','GBP','AUD','NZD','CHF','CAD', 'XAU','XAG'];
  const base = symbol.slice(0, 3);
  const quote = symbol.slice(3, 6);

  return FX.includes(base) && FX.includes(quote);
}

module.exports = { isForexSymbol }; 
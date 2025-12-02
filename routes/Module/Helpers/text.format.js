function formatString(str) {
  return str.trim().replace(/\s+/g, '-').toLowerCase();
}

function normSym(s) {
  return String(s ?? '')
    .normalize('NFKC')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

function calculatePercentage(str) {
  if (!str) return null;
  
  const match = str.match(/(\d+)\s*\/\s*(\d+)/);
  if (!match) return null;
  
  const percentage = (parseInt(match[1]) / parseInt(match[2])) * 100;
  return parseFloat(percentage.toFixed(0));
}


module.exports = {formatString, normSym ,calculatePercentage };

function formatString(str) {
  return normSym(str.trim().replace(/\s+/g, '-'));
}

function normSym(s) {
  return String(s ?? '')
    .normalize('NFKC')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}


module.exports = {formatString, normSym };

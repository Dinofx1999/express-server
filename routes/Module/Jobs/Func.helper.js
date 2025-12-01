/* eslint-disable */
function getSymbolInfo(data, symbol) {
  return data.find(item => item.Symbol === symbol);
}

function getForexSession(timeStr) {
  // Convert time string "YYYY.MM.DD HH:mm:ss" to Date
  const [datePart, timePart] = timeStr.split(" ");
  const [year, month, day] = datePart.split(".").map(Number);
  const [hour, minute, second] = timePart.split(":").map(Number);

  const date = new Date(year, month - 1, day, hour, minute, second);

  // Get hour in VN time (already VN time given)
  const h = date.getHours();

  if (h >= 5 && h < 14) return "Sydney";
  if (h >= 6 && h < 15) return "Tokyo";
  if (h >= 14 && h < 23) return "London";
  if (h >= 20 || h < 5) return "NewYork"; // crosses midnight

  return "Unknown";
}

function Digit(number) {
  if(number === 1) return 0.1;
  if(number === 2) return 0.01;
  if(number === 3) return 0.001;
  if(number === 4) return 0.0001;
  if(number === 5) return 0.00001;
  if(number === 6) return 0.000001;
  if(number === 7) return 0.0000001;
  if(number === 8) return 0.00000001;
  if(number === 9) return 0.000000001;
  if(number === 10) return 0.0000000001;
  return 0;
}

function Digit_Rec(number) {
  if(number === 1) return 10;
  if(number === 2) return 100;
  if(number === 3) return 1000;
  if(number === 4) return 10000;
  if(number === 5) return 100000;
  if(number === 6) return 1000000;
  if(number === 7) return 10000000;
  if(number === 8) return 100000000;
  if(number === 9) return 1000000000;
  if(number === 10) return 10000000000;
  return 0;
}

function removeSpaces(text, c) {
  let result = text;
  
  // Thay thế tất cả khoảng trắng bằng ký tự c
  if (c) result = result.replace(/ /g, c);  // g = global (tất cả)
  else result = result.replace(/ /g, '-'); // Nếu không có c, loại bỏ khoảng trắng
  return result;
}

// CommonJS exports
module.exports = {
  getSymbolInfo,
  getForexSession,
  Digit,
  Digit_Rec,
  removeSpaces
};
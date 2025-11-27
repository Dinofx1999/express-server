function getTimeGMT7(format = 'datetime') {
  const now = new Date();
  
  // Offset GMT+7 = +7 hours = 7 * 60 * 60 * 1000 milliseconds
  const gmt7Time = new Date(now.getTime() + (7 * 60 * 60 * 1000));
  
  const year = gmt7Time.getUTCFullYear();
  const month = String(gmt7Time.getUTCMonth() + 1).padStart(2, '0');
  const day = String(gmt7Time.getUTCDate()).padStart(2, '0');
  const hour = String(gmt7Time.getUTCHours()).padStart(2, '0');
  const minute = String(gmt7Time.getUTCMinutes()).padStart(2, '0');
  const second = String(gmt7Time.getUTCSeconds()).padStart(2, '0');
  
  switch(format) {
    case 'date':
      return `${year}-${month}-${day}`;
    case 'time':
      return `${hour}:${minute}:${second}`;
    case 'datetime':
    default:
      return `${year}.${month}.${day} ${hour}:${minute}:${second}`;
  }
}
module.exports = { getTimeGMT7 };
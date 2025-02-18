// src/utils/logger.js
const moment = require('moment-timezone');

const logger = {
  info: (...args) => console.log(
    moment().tz('Asia/Jakarta').format('YYYY-MM-DD HH:mm:ss.SSS'),
    'INFO:',
    ...args
  ),
  error: (...args) => console.error(
    moment().tz('Asia/Jakarta').format('YYYY-MM-DD HH:mm:ss.SSS'),
    'ERROR:',
    ...args
  ),
  debug: (...args) => console.debug(
    moment().tz('Asia/Jakarta').format('YYYY-MM-DD HH:mm:ss.SSS'),
    'DEBUG:',
    ...args
  ),
  warn: (...args) => console.warn(
    moment().tz('Asia/Jakarta').format('YYYY-MM-DD HH:mm:ss.SSS'),
    'WARN:',
    ...args
  )
};

module.exports = logger;
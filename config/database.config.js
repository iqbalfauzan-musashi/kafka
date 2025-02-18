// config/database.config.js
const dbConfig = {
  user: "SA",
  password: "Musashi123",
  server: "localhost",
  database: "IOT_HUB",
  options: {
    encrypt: true,
    trustServerCertificate: true,
    enableArithAbort: true
  },
  pool: {
    max: 20,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

module.exports = { dbConfig };


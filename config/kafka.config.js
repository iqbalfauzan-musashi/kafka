// config/kafka.config.js
const kafkaConfig = {
  clientId: "iot-client",
  brokers: ["localhost:9092"],
  retry: {
    initialRetryTime: 100,
    retries: 8
  },
  connectionTimeout: 3000,
  authenticationTimeout: 1000,
  requestTimeout: 30000,
  enforceRequestTimeout: true
};

module.exports = { kafkaConfig };

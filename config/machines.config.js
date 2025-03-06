// config/machines.config.js
const machinesConfig = [
  {
    machineCode: '45051',
    host: '10.42.46.1',
    port: 502,
    registers: {
      startAddress: 45,
      length: 3,
      counterAddress: 47
    }
  },
  {
    machineCode: '45050',
    host: '10.42.46.2',
    port: 502,
    registers: {
      startAddress: 45,
      length: 3,
      counterAddress: 47
    }
  },
  {
    machineCode: '45045',
    host: '10.42.46.3',
    port: 502,
    registers: {
      startAddress: 45,
      length: 3,
      counterAddress: 47
    }
  },
  {
    machineCode: '45044',
    host: '10.42.46.4',
    port: 502,
    registers: {
      startAddress: 45,
      length: 3,
      counterAddress: 47
    }
  },
  {
    machineCode: '45047',
    host: '10.42.46.5',
    port: 502,
    registers: {
      startAddress: 45,
      length: 3,
      counterAddress: 47
    }
  }
];

const globalConfig = {
  readInterval: 2000,
  reconnectDelay: 5000,
  connectionTimeout: 5000,
  maxRetries: 5,
  resetSchedule: {
    hour: 16,    // Jam reset (format 24 jam)
    minute: 24,   // Menit reset
    timezone: 'Asia/Jakarta'
  }
};

module.exports = { machinesConfig, globalConfig };
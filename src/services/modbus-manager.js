// src/services/modbus-manager.js
const ModbusService = require('./modbus-service');
const MachineProducer = require('../producer/machine-producer');
const logger = require('../utils/logger');
const { machinesConfig, globalConfig } = require('../../config/machines.config');

class ModbusManager {
  constructor() {
    this.machines = new Map();
    this.producer = new MachineProducer();
    this.isRunning = false;
    this.pollingInterval = null;
  }

  async init() {
    try {
      await this.producer.connect();
      
      // Initialize all machines
      for (const config of machinesConfig) {
        const machine = {
          service: new ModbusService(config),
          config: config,
          consecutiveErrors: 0
        };
        this.machines.set(config.machineCode, machine);
      }

      // Connect all machines sequentially to avoid connection conflicts
      for (const machine of this.machines.values()) {
        try {
          await machine.service.connect();
          await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second between connections
        } catch (error) {
          logger.error(`Failed to connect to machine ${machine.config.machineCode}:`, error);
        }
      }

      this.isRunning = true;
      this.startPolling();
      logger.info('ModbusManager initialized successfully');
    } catch (error) {
      logger.error('ModbusManager initialization error:', error);
      await this.cleanup();
      throw error;
    }
  }

  startPolling() {
    if (this.pollingInterval) {
      clearInterval(this.pollingInterval);
    }

    this.pollingInterval = setInterval(async () => {
      if (!this.isRunning) return;

      for (const [machineCode, machine] of this.machines) {
        if (!machine.service.isConnected) {
          continue;
        }

        try {
          const data = await machine.service.readData();
          if (data) {
            const success = await this.producer.sendData(machineCode, data);
            if (success) {
              machine.consecutiveErrors = 0;
              logger.info(`Data sent successfully for machine ${machineCode}`);
            } else {
              throw new Error('Failed to send data to Kafka');
            }
          }
        } catch (error) {
          machine.consecutiveErrors++;
          logger.error(`Error in machine ${machineCode} (${machine.consecutiveErrors}/${globalConfig.maxRetries}):`, error);

          if (machine.consecutiveErrors >= globalConfig.maxRetries) {
            await this.restartMachine(machineCode);
          }
        }
      }
    }, globalConfig.readInterval);
  }

  async restartMachine(machineCode) {
    const machine = this.machines.get(machineCode);
    if (!machine || machine.service.isConnecting) return;

    logger.info(`Restarting machine ${machineCode}...`);
    await machine.service.disconnect();
    
    try {
      await new Promise(resolve => setTimeout(resolve, globalConfig.reconnectDelay));
      await machine.service.connect();
      machine.consecutiveErrors = 0;
      logger.info(`Machine ${machineCode} successfully restarted`);
    } catch (error) {
      logger.error(`Failed to restart machine ${machineCode}:`, error);
    }
  }

  async cleanup() {
    this.isRunning = false;
    
    if (this.pollingInterval) {
      clearInterval(this.pollingInterval);
      this.pollingInterval = null;
    }
    
    // Disconnect all machines sequentially
    for (const machine of this.machines.values()) {
      try {
        await machine.service.disconnect();
        await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second between disconnections
      } catch (error) {
        logger.error(`Error disconnecting machine ${machine.config.machineCode}:`, error);
      }
    }
    
    try {
      await this.producer.disconnect();
      logger.info('ModbusManager cleanup completed');
    } catch (error) {
      logger.error('Error disconnecting producer:', error);
    }
  }
}

module.exports = ModbusManager;
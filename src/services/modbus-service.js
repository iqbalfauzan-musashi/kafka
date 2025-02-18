// src/services/modbus-service.js
const Modbus = require('jsmodbus');
const net = require('net');
const logger = require('../utils/logger');
const { globalConfig } = require('../../config/machines.config');

class ModbusService {
  constructor(config) {
    this.config = config;
    this.socket = new net.Socket();
    this.client = new Modbus.client.TCP(this.socket);
    this.isConnected = false;
    this.isConnecting = false;
    this.connectionTimeout = null;
    this.lastData = null;
    this.retryCount = 0;
    this.setupSocketEvents();
  }

  setupSocketEvents() {
    this.socket.on('error', (err) => {
      logger.error(`PLC ${this.config.machineCode} connection error:`, err);
    });

    this.socket.on('close', () => {
      logger.info(`PLC ${this.config.machineCode} connection closed`);
      this.handleDisconnection();
    });
  }

  async connect() {
    if (this.isConnecting) {
      throw new Error(`Connection attempt already in progress for PLC ${this.config.machineCode}`);
    }
    
    if (this.isConnected) {
      logger.info(`PLC ${this.config.machineCode} is already connected`);
      return;
    }
    
    this.isConnecting = true;
    
    try {
      return new Promise((resolve, reject) => {
        if (this.connectionTimeout) {
          clearTimeout(this.connectionTimeout);
        }

        const connectTimeout = setTimeout(() => {
          this.socket.destroy();
          this.isConnecting = false;
          reject(new Error(`Connection timeout for PLC ${this.config.machineCode}`));
        }, globalConfig.connectionTimeout);

        this.socket.once('connect', () => {
          clearTimeout(connectTimeout);
          logger.info(`Connected to PLC ${this.config.machineCode} at ${this.config.host}`);
          this.isConnected = true;
          this.isConnecting = false;
          this.retryCount = 0;
          resolve();
        });

        this.socket.once('error', (err) => {
          clearTimeout(connectTimeout);
          this.isConnecting = false;
          reject(err);
        });

        this.socket.connect({
          host: this.config.host,
          port: this.config.port
        });
      });
    } catch (error) {
      this.isConnecting = false;
      throw error;
    }
  }

  handleDisconnection() {
    if (!this.isConnected && !this.isConnecting) return;
    
    this.isConnected = false;
    this.isConnecting = false;
    
    if (this.socket) {
      this.socket.destroy();
    }

    if (this.retryCount < globalConfig.maxRetries) {
      this.retryCount++;
      this.connectionTimeout = setTimeout(async () => {
        try {
          logger.info(`Attempting to reconnect to PLC ${this.config.machineCode} (Attempt ${this.retryCount}/${globalConfig.maxRetries})...`);
          await this.connect();
        } catch (err) {
          logger.error(`Reconnection attempt failed for ${this.config.machineCode}:`, err);
        }
      }, globalConfig.reconnectDelay);
    } else {
      logger.error(`Maximum retry attempts reached for PLC ${this.config.machineCode}`);
    }
  }

  async readData() {
    if (!this.isConnected) {
      throw new Error(`Not connected to PLC ${this.config.machineCode}`);
    }

    try {
      const { startAddress, length } = this.config.registers;
      const response = await this.client.readHoldingRegisters(startAddress, length);

      if (!response || !response.response || !response.response._body) {
        throw new Error(`Invalid response from PLC ${this.config.machineCode}`);
      }

      const values = response.response._body._valuesAsArray;

      if (!Array.isArray(values) || values.length < 3) {
        throw new Error(`Invalid data format from PLC ${this.config.machineCode}`);
      }

      const Status = values[0];
      const Counter = values[2];

      let isUpdate = 1;
      if (this.lastData !== null) {
        if (this.lastData.Status === Status && this.lastData.Counter === Counter) {
          isUpdate = 0;
        }
      }

      this.lastData = { Status, Counter };

      return {
        machine_code: this.config.machineCode,
        data: values,
        is_update: isUpdate,
        timestamp: new Date().toISOString()
      };
    } catch (error) {
      logger.error(`Error reading Modbus data from ${this.config.machineCode}:`, error);
      if (error.message.includes('ECONNRESET') || error.message.includes('Port Not Open')) {
        this.handleDisconnection();
      }
      throw error;
    }
  }

  async writeRegister(address, value) {
    if (!this.isConnected) {
      throw new Error(`Not connected to PLC ${this.config.machineCode}`);
    }

    try {
      const response = await this.client.writeSingleRegister(address, value);
      
      if (!response || !response.response) {
        throw new Error(`Invalid write response from PLC ${this.config.machineCode}`);
      }

      logger.info(`Successfully wrote value ${value} to register ${address} for PLC ${this.config.machineCode}`);
      return true;
    } catch (error) {
      logger.error(`Error writing to register ${address} for PLC ${this.config.machineCode}:`, error);
      if (error.message.includes('ECONNRESET') || error.message.includes('Port Not Open')) {
        this.handleDisconnection();
      }
      throw error;
    }
  }

  async disconnect() {
    if (this.connectionTimeout) {
      clearTimeout(this.connectionTimeout);
    }
    
    this.isConnected = false;
    this.isConnecting = false;
    
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.destroy();
    }
    
    logger.info(`Disconnected from PLC ${this.config.machineCode}`);
  }
}

module.exports = ModbusService;
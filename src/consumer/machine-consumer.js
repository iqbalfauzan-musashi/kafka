// src/consumer/machine-consumer.js
const { Kafka } = require("kafkajs");
const sql = require("mssql");
const moment = require('moment-timezone');
const { kafkaConfig } = require("../../config/kafka.config");
const { dbConfig } = require("../../config/database.config");
const logger = require('../utils/logger');
const { OperationTranslator } = require('../utils/operation-codes');

class MachineConsumer {
  constructor() {
    this.kafka = new Kafka(kafkaConfig);
    this.consumer = this.kafka.consumer({ groupId: "iot-group" });
    this.pool = null;
    this.isConnected = false;
  }

  async connect() {
    try {
      await this.consumer.connect();
      logger.info("Consumer connected to Kafka");

      // Configure MSSQL to use the correct timezone
      const config = {
        ...dbConfig,
        options: {
          ...dbConfig.options,
          useUTC: false  // Prevent SQL Server from converting local time to UTC
        }
      };

      this.pool = await sql.connect(config);
      this.isConnected = true;
      logger.info("Connected to database");
    } catch (error) {
      logger.error("Connection error:", error);
      throw error;
    }
  }

  getCurrentTimestamp() {
    return moment().tz('Asia/Jakarta').format('YYYY-MM-DD HH:mm:ss.SSS');
  }

  parseTimestamp(timestamp) {
    // Ensure the timestamp is treated as Asia/Jakarta timezone
    const jakartaTime = moment.tz(timestamp, 'Asia/Jakarta');
    return jakartaTime.format('YYYY-MM-DD HH:mm:ss.SSS');
  }

  async getMachineName(machineCode) {
    try {
      const request = this.pool.request();
      const result = await request
        .input("machine_code", sql.NVarChar, machineCode)
        .query(`
          SELECT MACHINE_NAME 
          FROM CODE_MACHINE_PRODUCTION 
          WHERE MACHINE_CODE = @machine_code AND IS_SHOW = 1
        `);

      if (result.recordset.length > 0) {
        return result.recordset[0].MACHINE_NAME;
      }
      return 'Unknown Machine';
    } catch (error) {
      logger.error("Error getting machine name:", error);
      return 'Unknown Machine';
    }
  }

  async updateRealtimeStatus(machineCode, data, timestamp) {
    try {
      const request = this.pool.request();
      const [statusCode, sendPlc, machineCounter] = data;
      const operationName = OperationTranslator.getOperationName(statusCode);
      const machineName = await this.getMachineName(machineCode);

      // Convert the timestamp to Jakarta timezone before inserting
      const jakartaTimestamp = moment.tz(timestamp, 'Asia/Jakarta');
      const formattedTimestamp = jakartaTimestamp.format('YYYY-MM-DD HH:mm:ss.SSS');

      await request
        .input("machine_code", sql.NVarChar, machineCode)
        .input("machine_name", sql.NVarChar, machineName)
        .input("operation_name", sql.NVarChar, operationName)
        .input("machine_counter", sql.Int, machineCounter)
        .input("send_plc", sql.Int, sendPlc)
        .input("updated_at", sql.DateTime2, formattedTimestamp)
        .query(`
          SET LANGUAGE us_english;  -- Ensure consistent date formatting
          MERGE INTO MACHINE_STATUS_PRODUCTION AS target
          USING (VALUES (@machine_code)) AS source(machine_code)
          ON target.MachineCode = source.machine_code
          WHEN MATCHED THEN
            UPDATE SET 
              MachineName = @machine_name,
              OPERATION_NAME = @operation_name,
              MACHINE_COUNTER = @machine_counter,
              SEND_PLC = @send_plc,
              UpdatedAt = @updated_at
          WHEN NOT MATCHED THEN
            INSERT (
              MachineCode, MachineName, OPERATION_NAME,
              SEND_PLC, MACHINE_COUNTER, CreatedAt, UpdatedAt
            )
            VALUES (
              @machine_code, @machine_name, @operation_name,
              @send_plc, @machine_counter, @updated_at, @updated_at
            );
        `);

      logger.info(`Realtime status updated for machine: ${machineName} (${machineCode}) at ${formattedTimestamp}`);
      return true;
    } catch (error) {
      logger.error("Error updating realtime status:", error);
      throw error;
    }
  }

  async saveToHistory(machineCode, data, timestamp) {
    try {
      const request = this.pool.request();
      const [statusCode, sendPlc, machineCounter] = data;
      const operationName = OperationTranslator.getOperationName(statusCode);
      const machineName = await this.getMachineName(machineCode);

      // Convert the timestamp to Jakarta timezone before inserting
      const jakartaTimestamp = moment.tz(timestamp, 'Asia/Jakarta');
      const formattedTimestamp = jakartaTimestamp.format('YYYY-MM-DD HH:mm:ss.SSS');

      const result = await request
        .input("machine_code", sql.NVarChar, machineCode)
        .input("machine_name", sql.NVarChar, machineName)
        .input("operation_name", sql.NVarChar, operationName)
        .input("machine_counter", sql.Int, machineCounter)
        .input("send_plc", sql.Int, sendPlc)
        .input("created_at", sql.DateTime2, formattedTimestamp)
        .query(`
          SET LANGUAGE us_english;  -- Ensure consistent date formatting
          INSERT INTO HISTORY_MACHINE_PRODUCTION (
            MachineCode,
            MachineName, 
            OPERATION_NAME,
            MACHINE_COUNTER, 
            SEND_PLC, 
            CreatedAt
          )
          VALUES (
            @machine_code,
            @machine_name, 
            @operation_name,
            @machine_counter, 
            @send_plc, 
            @created_at
          );
          SELECT SCOPE_IDENTITY() AS id;
        `);

      logger.info(`Historical data saved with ID: ${result.recordset[0].id} at ${formattedTimestamp}`);
      return result.recordset[0].id;
    } catch (error) {
      logger.error("Error saving to history:", error);
      throw error;
    }
  }

  async start() {
    if (!this.isConnected) {
      throw new Error('Consumer not connected');
    }

    try {
      await this.consumer.subscribe({
        topic: "machine-data",
        fromBeginning: true
      });

      await this.consumer.run({
        eachMessage: async ({ message }) => {
          try {
            const data = JSON.parse(message.value.toString());
            logger.info("Received data:", data);

            if (data.is_update === 1) {
              await this.updateRealtimeStatus(
                data.machine_code,
                data.data,
                data.timestamp
              );

              await this.saveToHistory(
                data.machine_code,
                data.data,
                data.timestamp
              );

              logger.info(`Data processed for machine ${data.machine_code} at ${data.timestamp}`);
            } else {
              logger.debug(`Skipping duplicate data for machine ${data.machine_code}`);
            }
          } catch (error) {
            logger.error("Error processing message:", error);
          }
        }
      });
    } catch (error) {
      logger.error("Consumer run error:", error);
      throw error;
    }
  }

  async disconnect() {
    try {
      await this.consumer.disconnect();
      await sql.close();
      this.isConnected = false;
      logger.info("Consumer disconnected and database connection closed");
    } catch (error) {
      logger.error("Disconnect error:", error);
    }
  }
}

module.exports = MachineConsumer;
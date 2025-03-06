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
    this.iotHubPool = null;
    this.machineLogPool = null;
    this.isConnected = false;
    this.machineTableMap = {};
    this.tableStructureVerified = false;
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

      // Connect to IOT_HUB database
      this.iotHubPool = await sql.connect(config);
      
      // Create a new connection to MACHINE_LOG database
      const machineLogConfig = {
        ...config,
        database: "MACHINE_LOG" // Use MACHINE_LOG database
      };
      
      // Create a new SQL connection pool for MACHINE_LOG
      this.machineLogPool = await new sql.ConnectionPool(machineLogConfig).connect();
      
      this.isConnected = true;
      logger.info("Connected to databases");
      
      // Populate machine table mapping
      await this.populateMachineTableMap();
    } catch (error) {
      logger.error("Connection error:", error);
      throw error;
    }
  }

  async populateMachineTableMap() {
    try {
      const request = this.iotHubPool.request();
      const result = await request.query(`
        SELECT MACHINE_CODE 
        FROM CODE_MACHINE_PRODUCTION 
        WHERE IS_SHOW = 1
      `);

      for (const row of result.recordset) {
        const machineCode = row.MACHINE_CODE;
        // Create the machine table name based on machine code
        this.machineTableMap[machineCode] = `Machine_${machineCode}`;
      }

      logger.info("Machine table mapping populated:", this.machineTableMap);
    } catch (error) {
      logger.error("Error populating machine table map:", error);
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
      const request = this.iotHubPool.request();
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
      const request = this.iotHubPool.request();
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
        .input("created_at", sql.DateTime2, formattedTimestamp)
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
              CreatedAt = @created_at
          WHEN NOT MATCHED THEN
            INSERT (
              MachineCode, MachineName, OPERATION_NAME,
              SEND_PLC, MACHINE_COUNTER, CreatedAt
            )
            VALUES (
              @machine_code, @machine_name, @operation_name,
              @send_plc, @machine_counter, @created_at
            );
        `);

      logger.info(`Realtime status updated for machine: ${machineName} (${machineCode}) at ${formattedTimestamp}`);
      return true;
    } catch (error) {
      logger.error("Error updating realtime status:", error);
      throw error;
    }
  }

  async getTableColumns(tableName) {
    try {
      const request = this.machineLogPool.request();
      const result = await request.query(`
        SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE, 
               COLUMNPROPERTY(OBJECT_ID('[MACHINE_LOG].[dbo].[${tableName}]'), COLUMN_NAME, 'IsIdentity') AS IS_IDENTITY
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '${tableName}'
      `);
      
      return result.recordset;
    } catch (error) {
      logger.error(`Error getting table columns for ${tableName}:`, error);
      return null;
    }
  }

  async saveToHistory(machineCode, data, timestamp) {
    try {
      // Get the machine-specific table name from the map
      const tableName = this.machineTableMap[machineCode];
      
      if (!tableName) {
        logger.error(`No table mapping found for machine code: ${machineCode}`);
        return null;
      }
      
      // Check if table exists first
      const tableCheck = await this.machineLogPool.request().query(`
        SELECT OBJECT_ID('[MACHINE_LOG].[dbo].[${tableName}]', 'U') as TableID
      `);
      
      if (!tableCheck.recordset[0].TableID) {
        logger.error(`Table ${tableName} does not exist. Creating it now.`);
        await this.ensureTablesExist();
      }
      
      // Get all column names first to ensure they exist
      const columnsQuery = await this.machineLogPool.request().query(`
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '${tableName}'
      `);
      
      const columnNames = columnsQuery.recordset.map(col => col.COLUMN_NAME);
      const hasIdColumn = columnNames.includes('ID');
      
      const request = this.machineLogPool.request();
      const [statusCode, sendPlc, machineCounter] = data;
      const operationName = OperationTranslator.getOperationName(statusCode);
      const machineName = await this.getMachineName(machineCode);
  
      // Convert the timestamp to Jakarta timezone before inserting
      const jakartaTimestamp = moment.tz(timestamp, 'Asia/Jakarta');
      const formattedTimestamp = jakartaTimestamp.format('YYYY-MM-DD HH:mm:ss.SSS');
  
      // Adjust query based on whether ID column exists
      let query;
      if (hasIdColumn) {
        // Check if ID is an identity column
        const idIdentityCheck = await this.machineLogPool.request().query(`
          SELECT COLUMNPROPERTY(OBJECT_ID('[MACHINE_LOG].[dbo].[${tableName}]'), 'ID', 'IsIdentity') AS IS_IDENTITY
        `);
        
        const isIdentity = idIdentityCheck.recordset[0].IS_IDENTITY === 1;
        
        if (isIdentity) {
          // If ID is IDENTITY, don't include it in the insert
          query = `
            SET LANGUAGE us_english;
            INSERT INTO [MACHINE_LOG].[dbo].[${tableName}] (
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
          `;
        } else {
          // If ID needs to be manually inserted, get the next ID value
          const idResult = await request.query(`
            SELECT ISNULL(MAX(ID), 0) + 1 AS NextID 
            FROM [MACHINE_LOG].[dbo].[${tableName}]
          `);
          
          const nextId = idResult.recordset[0].NextID;
          
          request.input("id", sql.Int, nextId);
          
          query = `
            SET LANGUAGE us_english;
            INSERT INTO [MACHINE_LOG].[dbo].[${tableName}] (
              ID,
              MachineCode,
              MachineName, 
              OPERATION_NAME,
              MACHINE_COUNTER, 
              SEND_PLC, 
              CreatedAt
            )
            VALUES (
              @id,
              @machine_code,
              @machine_name, 
              @operation_name,
              @machine_counter, 
              @send_plc, 
              @created_at
            );
            SELECT @id AS id;
          `;
        }
      } else {
        // If ID column doesn't exist at all, don't try to use it
        query = `
          SET LANGUAGE us_english;
          INSERT INTO [MACHINE_LOG].[dbo].[${tableName}] (
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
          SELECT 0 AS id;
        `;
      }
  
      // Set other parameters
      request
        .input("machine_code", sql.NVarChar, machineCode)
        .input("machine_name", sql.NVarChar, machineName)
        .input("operation_name", sql.NVarChar, operationName)
        .input("machine_counter", sql.Int, machineCounter)
        .input("send_plc", sql.Int, sendPlc)
        .input("created_at", sql.DateTime2, formattedTimestamp);
  
      // Execute the query
      const result = await request.query(query);
      const insertedId = result.recordset[0].id;
  
      logger.info(`Historical data saved to ${tableName} with ID: ${insertedId} at ${formattedTimestamp}`);
      return insertedId;
    } catch (error) {
      logger.error(`Error saving to history table for machine ${machineCode}:`, error);
      throw error;
    }
  }

  async validateTableStructure(tableName) {
    try {
      logger.info(`Validating structure for table ${tableName}`);
      
      const columns = await this.getTableColumns(tableName);
      
      // If table doesn't exist, return false
      if (!columns || columns.length === 0) {
        return false;
      }
      
      // Check for required columns
      const requiredColumns = ['MachineCode', 'MachineName', 'OPERATION_NAME', 
                              'MACHINE_COUNTER', 'SEND_PLC', 'CreatedAt'];
      
      const missingColumns = requiredColumns.filter(
        reqColumn => !columns.some(col => col.COLUMN_NAME === reqColumn)
      );
      
      if (missingColumns.length > 0) {
        logger.warn(`Table ${tableName} is missing columns: ${missingColumns.join(', ')}`);
        return false;
      }
      
      return true;
    } catch (error) {
      logger.error(`Error validating table structure for ${tableName}:`, error);
      return false;
    }
  }

  async ensureTablesExist() {
    try {
      // For each machine code in the map, ensure the corresponding table exists
      for (const [machineCode, tableName] of Object.entries(this.machineTableMap)) {
        const request = this.machineLogPool.request();
        
        // First check if the table exists and has the right structure
        const isValid = await this.validateTableStructure(tableName);
        
        if (!isValid) {
          logger.info(`Creating or recreating table ${tableName} for machine ${machineCode}`);
          
          // Drop the table if it exists but has incorrect structure
          await request.query(`
            IF OBJECT_ID('[MACHINE_LOG].[dbo].[${tableName}]', 'U') IS NOT NULL
              DROP TABLE [MACHINE_LOG].[dbo].[${tableName}]
          `);
          
          // Create the table with proper structure
          await request.query(`
            CREATE TABLE [MACHINE_LOG].[dbo].[${tableName}] (
              ID INT IDENTITY(1,1) PRIMARY KEY,
              MachineCode NVARCHAR(50) NOT NULL,
              MachineName NVARCHAR(100) NOT NULL,
              OPERATION_NAME NVARCHAR(100) NOT NULL,
              MACHINE_COUNTER INT NOT NULL,
              SEND_PLC INT NOT NULL,
              CreatedAt DATETIME2 NOT NULL
            );
            
            -- Create index for faster querying
            CREATE INDEX IX_${tableName}_CreatedAt ON [MACHINE_LOG].[dbo].[${tableName}] (CreatedAt);
          `);
          logger.info(`Table ${tableName} created successfully`);
        } else {
          logger.info(`Table ${tableName} already exists with correct structure`);
        }
      }
      
      logger.info("All tables verified/created successfully");
      this.tableStructureVerified = true;
    } catch (error) {
      logger.error("Error ensuring tables exist:", error);
      throw error;
    }
  }

  async start() {
    if (!this.isConnected) {
      throw new Error('Consumer not connected');
    }

    try {
      // Ensure all required tables exist
      await this.ensureTablesExist();
      
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
      
      if (this.iotHubPool) {
        await this.iotHubPool.close();
      }
      
      if (this.machineLogPool) {
        await this.machineLogPool.close();
      }
      
      this.isConnected = false;
      logger.info("Consumer disconnected and database connections closed");
    } catch (error) {
      logger.error("Disconnect error:", error);
    }
  }
}

module.exports = MachineConsumer;
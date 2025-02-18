// src/services/plc-reset-service.js
const cron = require('node-cron');
const ModbusService = require('./modbus-service');
const logger = require('../utils/logger');
const { machinesConfig, globalConfig } = require('../../config/machines.config');
const moment = require('moment-timezone');

class PLCResetService {
    constructor() {
        this.machines = new Map();
        this.lastResetDate = null;
        this.resetHour = globalConfig.resetSchedule.hour;
        this.resetMinute = globalConfig.resetSchedule.minute;
        this.timezone = globalConfig.resetSchedule.timezone;
        this.scheduler = null;
        this.preCheckScheduler = null;
        this.resetInProgress = false;
        this.initializeMachines();
    }

    initializeMachines() {
        for (const config of machinesConfig) {
            const machine = {
                service: new ModbusService(config),
                config: config,
                lastResetAttempt: null,
                resetRetryCount: 0,
                resetStatus: false
            };
            this.machines.set(config.machineCode, machine);
        }
    }

    async connect() {
        const connectionPromises = [];
        
        for (const [machineCode, machine] of this.machines.entries()) {
            const connectionPromise = (async () => {
                try {
                    if (!machine.service.isConnected) {
                        await machine.service.connect();
                        await new Promise(resolve => setTimeout(resolve, 1000));
                        logger.info(`Connected to PLC ${machineCode} for reset service`);
                    }
                } catch (error) {
                    logger.error(`Failed to connect to PLC ${machineCode} for reset service:`, error);
                }
            })();
            connectionPromises.push(connectionPromise);
        }

        await Promise.all(connectionPromises);
    }

    async resetCounter(machineCode) {
        const machine = this.machines.get(machineCode);
        if (!machine) {
            throw new Error(`Machine ${machineCode} not found`);
        }

        const now = moment().tz(this.timezone);
        
        try {
            if (!machine.service.isConnected) {
                logger.info(`Reconnecting to PLC ${machineCode} before reset...`);
                await machine.service.connect();
                await new Promise(resolve => setTimeout(resolve, 1000));
            }

            // Read current counter value
            const data = await machine.service.readData();
            if (!data || !data.data || data.data.length < 3) {
                throw new Error(`Invalid data read from machine ${machineCode}`);
            }

            const currentCounter = data.data[2];
            logger.info(`Current counter value for machine ${machineCode}: ${currentCounter}`);

            // Reset counter to 0
            await machine.service.writeRegister(machine.config.registers.counterAddress, 0);
            
            // Wait a bit before verification
            await new Promise(resolve => setTimeout(resolve, 1000));

            // Verify reset
            const verificationData = await machine.service.readData();
            if (!verificationData || !verificationData.data || verificationData.data[2] !== 0) {
                throw new Error(`Counter reset verification failed for machine ${machineCode}`);
            }

            machine.lastResetAttempt = now;
            machine.resetRetryCount = 0;
            machine.resetStatus = true;
            logger.info(`Successfully reset counter for machine ${machineCode} from ${currentCounter} to 0`);
            
            return true;
        } catch (error) {
            machine.resetRetryCount++;
            logger.error(`Error resetting counter for machine ${machineCode} (Attempt ${machine.resetRetryCount}):`, error);
            
            if (machine.resetRetryCount < globalConfig.maxRetries) {
                logger.info(`Scheduling retry for machine ${machineCode} in ${globalConfig.reconnectDelay}ms`);
                return new Promise(resolve => {
                    setTimeout(async () => {
                        try {
                            const result = await this.resetCounter(machineCode);
                            resolve(result);
                        } catch (retryError) {
                            logger.error(`Retry failed for machine ${machineCode}:`, retryError);
                            resolve(false);
                        }
                    }, globalConfig.reconnectDelay);
                });
            } else {
                logger.error(`Max retry attempts reached for resetting machine ${machineCode}`);
                machine.resetRetryCount = 0;
                machine.resetStatus = false;
                return false;
            }
        }
    }

    isWithinResetWindow() {
        const now = moment().tz(this.timezone);
        const currentHour = now.hour();
        const currentMinute = now.minute();
        
        // Check if we're within the reset window (15 minutes)
        return (currentHour === this.resetHour && 
                currentMinute >= this.resetMinute && 
                currentMinute < (this.resetMinute + 15));
    }

    shouldPerformReset() {
        if (this.resetInProgress) {
            logger.debug('Reset operation already in progress');
            return false;
        }

        if (!this.isWithinResetWindow()) {
            return false;
        }

        const now = moment().tz(this.timezone);
        if (this.lastResetDate && this.lastResetDate.isSame(now, 'day')) {
            logger.debug('Already performed reset operations today');
            return false;
        }

        return true;
    }

    async checkAndResetIfNeeded() {
        if (!this.shouldPerformReset()) {
            return;
        }

        try {
            this.resetInProgress = true;
            const now = moment().tz(this.timezone);
            logger.info(`Initiating daily PLC counter reset at ${now.format('YYYY-MM-DD HH:mm:ss')}`);
            
            const resetResults = await Promise.all(
                Array.from(this.machines.keys()).map(machineCode => 
                    this.resetCounter(machineCode)
                )
            );

            const allSuccessful = resetResults.every(result => result === true);
            if (allSuccessful) {
                this.lastResetDate = now;
                logger.info('Daily reset operation completed successfully for all machines');
            } else {
                logger.warn('Daily reset operation completed with some failures');
            }
        } catch (error) {
            logger.error('Error during daily reset operation:', error);
        } finally {
            this.resetInProgress = false;
        }
    }

    startScheduler() {
        if (this.scheduler || this.preCheckScheduler) {
            this.stopScheduler();
        }

        // Check every minute during the reset window
        const cronSchedule = `*/1 ${this.resetMinute}-${this.resetMinute + 14} ${this.resetHour} * * *`;
        
        logger.info(`Reset service scheduled for ${this.resetHour}:${this.resetMinute} ${this.timezone}`);
        logger.info(`Cron schedule: ${cronSchedule}`);

        this.scheduler = cron.schedule(cronSchedule, async () => {
            await this.checkAndResetIfNeeded();
        }, {
            timezone: this.timezone
        });

        // Additional safety check 5 minutes before and after the scheduled time
        const preCheckMinute = this.resetMinute === 0 ? 55 : this.resetMinute - 5;
        const preCheckHour = this.resetMinute === 0 ? (this.resetHour === 0 ? 23 : this.resetHour - 1) : this.resetHour;
        
        const preCheckSchedule = `*/1 ${preCheckMinute}-59,0-${this.resetMinute + 19} ${preCheckHour},${this.resetHour} * * *`;
        
        this.preCheckScheduler = cron.schedule(preCheckSchedule, async () => {
            await this.checkAndResetIfNeeded();
        }, {
            timezone: this.timezone
        });

        logger.info('PLC reset scheduler started with enhanced monitoring');
    }


    stopScheduler() {
        if (this.scheduler) {
            this.scheduler.stop();
            this.scheduler = null;
        }
        if (this.preCheckScheduler) {
            this.preCheckScheduler.stop();
            this.preCheckScheduler = null;
        }
    }

    async stop() {
        this.stopScheduler();
        
        for (const [machineCode, machine] of this.machines.entries()) {
            try {
                if (machine.service.isConnected) {
                    await machine.service.disconnect();
                    logger.info(`Disconnected from PLC ${machineCode}`);
                }
            } catch (error) {
                logger.error(`Error disconnecting from PLC ${machineCode}:`, error);
            }
        }
    }
}

module.exports = PLCResetService;
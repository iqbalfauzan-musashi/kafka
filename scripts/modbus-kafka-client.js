//scripts/modbus-kafka-client.js
const ModbusManager = require('../src/services/modbus-manager');
const logger = require('../src/utils/logger');

class ModbusKafkaClient {
    constructor() {
        this.manager = new ModbusManager();
    }

    async init() {
        try {
            await this.manager.init();
            this.setupGracefulShutdown();
            logger.info('ModbusKafkaClient initialized successfully');
        } catch (error) {
            logger.error('Initialization error:', error);
            await this.cleanup();
            process.exit(1);
        }
    }

    async cleanup() {
        await this.manager.cleanup();
        logger.info('ModbusKafkaClient cleanup completed');
    }

    setupGracefulShutdown() {
        process.on('SIGINT', async () => {
            logger.info('Received SIGINT signal. Shutting down gracefully...');
            await this.cleanup();
            process.exit(0);
        });

        process.on('SIGTERM', async () => {
            logger.info('Received SIGTERM signal. Shutting down gracefully...');
            await this.cleanup();
            process.exit(0);
        });
    }
}

// Create and start the client
const client = new ModbusKafkaClient();
client.init().catch(error => {
    logger.error('Failed to start client:', error);
    process.exit(1);
});
// scripts/producer-app.js
const ModbusManager = require('../src/services/modbus-manager');
const logger = require('../src/utils/logger');

class ProducerApp {
    constructor() {
        this.manager = new ModbusManager();
    }

    async init() {
        try {
            await this.manager.init();
            this.setupGracefulShutdown();
            logger.info('Producer initialized successfully');
        } catch (error) {
            logger.error('Producer initialization error:', error);
            await this.cleanup();
            process.exit(1);
        }
    }

    async cleanup() {
        await this.manager.cleanup();
        logger.info('Producer cleanup completed');
    }

    setupGracefulShutdown() {
        const shutdownHandler = async (signal) => {
            logger.info(`Received ${signal} signal. Shutting down producer gracefully...`);
            await this.cleanup();
            process.exit(0);
        };

        process.on('SIGINT', () => shutdownHandler('SIGINT'));
        process.on('SIGTERM', () => shutdownHandler('SIGTERM'));
        
        process.on('uncaughtException', async (error) => {
            logger.error('Uncaught exception:', error);
            await this.cleanup();
            process.exit(1);
        });

        process.on('unhandledRejection', async (reason, promise) => {
            logger.error('Unhandled Rejection at:', promise, 'reason:', reason);
            await this.cleanup();
            process.exit(1);
        });
    }
}

// Create and start the producer
const producerApp = new ProducerApp();
producerApp.init().catch(error => {
    logger.error('Failed to start producer:', error);
    process.exit(1);
});
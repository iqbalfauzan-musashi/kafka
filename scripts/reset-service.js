// scripts/reset-service.js
const PLCResetService = require('../src/services/plc-reset-service');
const logger = require('../src/utils/logger');

class ResetServiceManager {
    constructor() {
        this.resetService = new PLCResetService();
    }

    async start() {
        try {
            logger.info('Initializing Reset Service...');
            await this.resetService.connect();
            
            logger.info('Starting reset scheduler...');
            this.resetService.startScheduler();

            this.setupGracefulShutdown();
            logger.info('Reset Service initialized successfully');
        } catch (error) {
            logger.error('Reset Service initialization error:', error);
            await this.cleanup();
            process.exit(1);
        }
    }

    setupGracefulShutdown() {
        const shutdownHandler = async (signal) => {
            logger.info(`Received ${signal} signal. Shutting down Reset Service gracefully...`);
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

    async cleanup() {
        try {
            await this.resetService.stop();
            logger.info('Reset Service cleanup completed');
        } catch (error) {
            logger.error('Error during Reset Service cleanup:', error);
        }
    }
}

// Create and start the Reset Service
const manager = new ResetServiceManager();
manager.start().catch(error => {
    logger.error('Failed to start Reset Service:', error);
    process.exit(1);
});
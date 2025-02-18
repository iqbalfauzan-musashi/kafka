// scripts/consumer-app.js
const MachineConsumer = require('../src/consumer/machine-consumer');
const logger = require('../src/utils/logger');

async function runConsumer() {
    const consumer = new MachineConsumer();

    try {
        await consumer.connect();
        await consumer.start();

        process.on('SIGINT', async () => {
            logger.info('Shutting down consumer...');
            await consumer.disconnect();
            process.exit(0);
        });
    } catch (error) {
        logger.error('Consumer error:', error);
        await consumer.disconnect();
        process.exit(1);
    }
}

runConsumer().catch(error => {
    logger.error('Failed to start consumer:', error);
    process.exit(1);
});
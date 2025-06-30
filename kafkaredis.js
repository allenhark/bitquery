const { Kafka, CompressionTypes, CompressionCodecs } = require('kafkajs');
const LZ4 = require('kafkajs-lz4');
const Redis = require('ioredis');
const dotenv = require('dotenv');
const { v4: uuidv4 } = require('uuid');
dotenv.config();

CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;

const username = process.env.username;
const password = process.env.password;
const topic = 'solana.transactions.proto';
const id = uuidv4();

const redis = new Redis();

const kafka = new Kafka({
    clientId: username,
    brokers: ['rpk0.bitquery.io:9092', 'rpk1.bitquery.io:9092', 'rpk2.bitquery.io:9092'],
    sasl: {
        mechanism: 'scram-sha-512',
        username,
        password,
    },
});

const consumer = kafka.consumer({
    groupId: `${username}-${id}`,
    sessionTimeout: 10000,
    heartbeatInterval: 3000,
    maxWaitTimeInMs: 25,
    allowAutoTopicCreation: false,
});

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
        autoCommit: true,
        partitionsConsumedConcurrently: 6,
        eachMessage: async ({ message }) => {
            // Only keep the latest message in Redis (overwrite any backlog)
            await redis.set('latest_kafka_msg', message.value.toString());
        },
    });
};

run().catch(console.error);

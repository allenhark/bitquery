import chalk from "chalk";
import { extractPumpFunTokenDetails } from "./ExtractToken2";
import { Worker } from 'worker_threads';
import path from 'path';

// Import required modules
const { Kafka } = require('kafkajs');
const bs58 = require('bs58');
const { CompressionTypes, CompressionCodecs } = require('kafkajs');
const LZ4 = require('kafkajs-lz4');
const { v4: uuidv4 } = require('uuid');
//const config = require('./config.json');  //credentials imported from JSON file
// Enable LZ4 compression
CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;
import dotenv from 'dotenv';

dotenv.config();

// Configuration
const username = process.env.username; //''; //credentials imported from JSON file
const password = process.env.password; //'';
const topic = 'solana.transactions.proto';

const id = uuidv4();

// Map to keep track of workers per partition
const partitionWorkers = new Map<number, Worker>();

// Helper to get or create a worker for a partition
function getWorkerForPartition(partition: number, topic: string) {
  if (!partitionWorkers.has(partition)) {
    const worker = new Worker(path.resolve(__dirname, './processTokenWorker.js'));
    worker.postMessage({ type: 'init', topic });
    worker.on('message', (msg) => {
      if (msg.type === 'result') {
        console.log(chalk.cyan('🚀 Pump.fun token creation detected (from worker)'));
        console.log(msg.meta);
      } else if (msg.type === 'error') {
        console.error('Worker error:', msg.error);
      }
    });
    worker.on('error', (err) => {
      console.error('Worker thread error:', err);
    });
    partitionWorkers.set(partition, worker);
  }
  return partitionWorkers.get(partition)!;
}

// Initialize Kafka Client (Non-SSL)
const kafka = new Kafka({
  clientId: username,
  brokers: ['rpk0.bitquery.io:9092', 'rpk1.bitquery.io:9092', 'rpk2.bitquery.io:9092'],
  sasl: {
    mechanism: 'scram-sha-512',
    username: username,
    password: password,
  },
});

// Function to convert bytes to base58
const convertBytes = (buffer, encoding = 'base58') => {
  if (encoding === 'base58') {
    return bs58.default.encode(buffer);
  }
  return buffer.toString('hex');
}

// Recursive function to convert Protobuf messages to JSON
const protobufToJson = (msg, encoding = 'base58') => {
  const result = {};
  for (const [key, value] of Object.entries(msg)) {
    if (Array.isArray(value)) {
      result[key] = value.map((item, idx) => {
        if (typeof item === 'object' && item !== null) {
          return protobufToJson(item, encoding);
        } else {
          return item;
        }
      });
    } else if (value && typeof value === 'object' && Buffer.isBuffer(value)) {
      result[key] = convertBytes(value, encoding);
    } else if (value && typeof value === 'object') {
      result[key] = protobufToJson(value, encoding);
    } else {
      result[key] = value;
    }
  }
  return result;
}

// Initialize consumer with more aggressive settings
const consumer = kafka.consumer({
  groupId: `${username}-${id}`,
  sessionTimeout: 6000,
  heartbeatInterval: 500,
  allowAutoTopicCreation: false,
  maxWaitTimeInMs: 50,
  maxBytes: 2097152,
  fetchMaxBytes: 1048576,
  fetchMinBytes: 1,
});

// Run the consumer
const run = async () => {
  try {
    await consumer.connect();
    await consumer.subscribe({
      topic,
      fromBeginning: false
    });

    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 500,
      autoCommitThreshold: 5,
      partitionsConsumedConcurrently: 4,
      eachBatch: async ({ batch, partition }) => {
        const worker = getWorkerForPartition(partition, topic);
        worker.postMessage({ type: 'batch', messages: batch.messages, topic });
      },
    });

  } catch (error) {
    console.error('Error running consumer:', error);
  }
}

// Start the consumer
run().catch(console.error);
import chalk from "chalk";
import { Worker } from "worker_threads";
import path from "path";
import dotenv from "dotenv";
import { Kafka, CompressionTypes, CompressionCodecs } from "kafkajs";
import bs58 from "bs58";
import LZ4 from "kafkajs-lz4";
import { v4 as uuidv4 } from "uuid";

dotenv.config();
CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;

const username = process.env.username!;
const password = process.env.password!;
const topic = "solana.transactions.proto";
const id = uuidv4();

// Map to keep track of workers per partition
const partitionWorkers = new Map<number, Worker>();

// Spawn and cache a worker per partition
function getWorkerForPartition(partition: number, topic: string) {
  if (!partitionWorkers.has(partition)) {
    const worker = new Worker(path.resolve(__dirname, "./processTokenWorker.js"));

    worker.postMessage({ type: "init", topic });

    worker.on("message", (msg) => {
      if (msg.type === "result") {
        console.log(chalk.cyan("🚀 Pump.fun token creation detected (from worker)"));
        console.dir(msg.meta, { depth: 5 });
      } else if (msg.type === "error") {
        console.error("Worker error:", msg.error);
      }
    });

    worker.on("error", (err) => {
      console.error("Worker thread error:", err);
    });

    partitionWorkers.set(partition, worker);
  }
  return partitionWorkers.get(partition)!;
}

// Kafka client setup
const kafka = new Kafka({
  clientId: username,
  brokers: ["rpk0.bitquery.io:9092", "rpk1.bitquery.io:9092", "rpk2.bitquery.io:9092"],
  sasl: {
    mechanism: "scram-sha-512",
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
  try {
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    await consumer.run({
      autoCommit: false,
      partitionsConsumedConcurrently: 6,
      eachBatchAutoResolve: true,
      //fetchMinBytes: 1,
      //fetchMaxBytes: 1048576,
      eachBatch: async ({ batch, heartbeat, resolveOffset, commitOffsetsIfNecessary }) => {
        const worker = getWorkerForPartition(batch.partition, topic);

        const messages = batch.messages.map((msg) => ({
          value: msg.value,
        }));

        worker.postMessage({ type: "batch", messages });

        for (const message of batch.messages) {
          resolveOffset(message.offset);
        }

        await commitOffsetsIfNecessary();
        await heartbeat();
      },
    });
  } catch (error) {
    console.error("Error running consumer:", error);
  }
};

run().catch(console.error);

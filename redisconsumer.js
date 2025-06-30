const Redis = require("ioredis");
const { extractPumpFunTokenDetails } = require("./ExtractToken2");
const bs58 = require("bs58");
const chalk = require("chalk").default;
const { loadProto } = require("bitquery-protobuf-schema");

const redis = new Redis();
const streamKey = "solana:txs";
const groupName = "pump-group";
const consumerName = `worker-${process.pid}`;
const pumpFunProgram = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

let ParsedIdlBlockMessage = null;

async function init() {
    try {
        ParsedIdlBlockMessage = await loadProto("solana.transactions.proto");

        try {
            await redis.xgroup("CREATE", streamKey, groupName, "0", "MKSTREAM");
        } catch (err) {
            if (!err.message.includes("BUSYGROUP")) throw err;
        }

        console.log(chalk.green(`🚀 Worker ${consumerName} started.`));
        startWorkerLoop();
    } catch (err) {
        console.error("Init error:", err);
    }
}

async function startWorkerLoop() {
    while (true) {
        try {
            const result = await redis.xreadgroup(
                "GROUP", groupName, consumerName,
                "COUNT", 10,
                "BLOCK", 5000,
                "STREAMS", streamKey, ">"
            );

            if (!result) continue;

            for (const [, entries] of result) {
                for (const [id, fields] of entries) {
                    const payloadIndex = fields.findIndex((v, i) => fields[i - 1] === "payload");
                    const base64Payload = fields[payloadIndex];

                    if (!base64Payload) continue;

                    const buffer = Buffer.from(base64Payload, "base64");

                    await processToken(buffer);

                    await redis.xack(streamKey, groupName, id);
                }
            }
        } catch (err) {
            console.error("Worker loop error:", err);
        }
    }
}

function logTimeDiff(decoded) {
    const timestampObj = decoded?.Header?.Timestamp;
    if (timestampObj?.low) {
        const blockTimeMs = timestampObj.low * 1000;
        const timeDiff = Date.now() - blockTimeMs;
        console.log(chalk.green("⏱ BlockTime Diff:"), chalk.red(`${timeDiff} ms`));
    }
}

async function processToken(buffer) {
    try {
        const decoded = ParsedIdlBlockMessage.decode(buffer);
        logTimeDiff(decoded);

        const txs = decoded.Transactions || [];
        for (const tx of txs) {
            if (!tx?.Status?.Success || !tx?.TotalBalanceUpdates?.length) continue;

            const hasPumpProgram = tx?.Header?.Accounts?.some(
                (a) => bs58.encode(a.Address) === pumpFunProgram
            );
            if (!hasPumpProgram) continue;

            const pumpInstructions = tx.ParsedIdlInstructions?.filter(
                (inst) =>
                    inst?.Program?.Name === "pump" && inst?.Program?.Method === "create"
            );

            if (pumpInstructions?.length > 0) {
                const meta = extractPumpFunTokenDetails(tx);
                console.log(chalk.cyan("✅ Pump Token:"), meta);
            }
        }
    } catch (err) {
        console.error("Processing error:", err);
    }
}

init();

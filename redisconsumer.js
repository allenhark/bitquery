const Redis = require('ioredis');
const { extractPumpFunTokenDetails } = require('./ExtractToken2');
const bs58 = require('bs58');
const chalk = require('chalk');
const { loadProto } = require('bitquery-protobuf-schema');

const redis = new Redis();
let lastProcessed = null;
const topic = 'solana.transactions.proto';
const pumpFunProgram = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
let ParsedIdlBlockMessage = null;

async function processMessage(raw) {
    try {
        if (!ParsedIdlBlockMessage) {
            ParsedIdlBlockMessage = await loadProto(topic);
        }
        const buffer = Buffer.from(raw, 'base64');
        const decoded = ParsedIdlBlockMessage.decode(buffer);
        // Log time diff
        const timestampObj = decoded?.Header?.Timestamp;
        if (timestampObj?.low) {
            const blockTimeMs = timestampObj.low * 1000;
            const timeDiff = Date.now() - blockTimeMs;
            console.log(chalk.green('⏱ BlockTime Diff:'), chalk.red(`${timeDiff} ms`));
            if (timeDiff > 2000) {
                console.warn(chalk.yellow('⚠️ WARNING: Consumer lag detected!'));
            }
        }
        const txs = decoded.Transactions || [];
        for (const tx of txs) {
            if (!tx?.Status?.Success || !tx?.TotalBalanceUpdates?.length) continue;
            const hasPumpProgram = tx?.Header?.Accounts?.some(
                (a) => bs58.encode(a.Address) === pumpFunProgram
            );
            if (!hasPumpProgram) continue;
            const pumpInstructions = tx.ParsedIdlInstructions?.filter(
                (inst) =>
                    inst?.Program?.Name === 'pump' &&
                    inst?.Program?.Method === 'create'
            );
            if (pumpInstructions?.length > 0) {
                const meta = extractPumpFunTokenDetails(tx);
                console.log(chalk.cyan('🚀 Pump.fun token creation detected'));
                console.dir(meta, { depth: 5 });
            }
        }
    } catch (err) {
        console.error('Error processing message:', err);
    }
}

async function pollLatest() {
    try {
        const latest = await redis.get('latest_kafka_msg');
        if (latest && latest !== lastProcessed) {
            await processMessage(latest);
            lastProcessed = latest;
        }
    } catch (err) {
        console.error('Error polling Redis:', err);
    }
}

setInterval(pollLatest, 100); // Poll every 100ms

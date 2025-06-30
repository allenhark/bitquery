import { parentPort } from 'worker_threads';
import chalk from 'chalk';
import { extractPumpFunTokenDetails } from './ExtractToken2';
import bs58 from 'bs58';
import { loadProto } from 'bitquery-protobuf-schema';

const pumpFunProgram = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';

const convertBytes = (buffer: Buffer, encoding = 'base58') => {
    if (encoding === 'base58') {
        return bs58.encode(buffer);
    }
    return buffer.toString('hex');
};

const protobufToJson = (msg: any, encoding = 'base58') => {
    const result: any = {};
    for (const [key, value] of Object.entries(msg)) {
        if (Array.isArray(value)) {
            result[key] = value.map((item) => {
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
};

async function processToken(ParsedIdlBlockMessage: any, message: any) {
    try {
        const buffer = message.value;
        const decoded = ParsedIdlBlockMessage.decode(buffer);
        const msgObj = ParsedIdlBlockMessage.toObject(decoded, { bytes: Buffer });
        const jsonOutput = protobufToJson(msgObj) as any;
        const transactions = (jsonOutput as any).Transactions || [];

        // console.log(jsonOutput?.Header)

        if (jsonOutput?.Header?.Timestamp) {
            const blockTime = jsonOutput?.Header?.Timestamp.low ? jsonOutput?.Header?.Timestamp.low * 1000 : Date.now();
            const timeDiff = Date.now() - blockTime;

            console.log(chalk.green('Time Diff :'), chalk.red(timeDiff), 'ms')

        }

        if (transactions.length === 0) return;
        const filteredTxs = transactions.filter((tx: any) =>
            Array.isArray(tx.ParsedIdlInstructions) &&
            tx.TotalBalanceUpdates.length > 0 &&
            tx.Status?.Success === true
        );
        for (const tx of filteredTxs) {
            const hasPumpProgram = tx?.Header?.Accounts?.some((account: any) =>
                account.Address === pumpFunProgram
            );

            // console.log('Has pump fun program: ', hasPumpProgram)
            if (!hasPumpProgram) continue;
            const pumpCreateInstructions = tx.ParsedIdlInstructions.filter((inst: any) =>
                inst?.Program?.Name === 'pump' &&
                inst?.Program?.Method === 'create'
            );
            if (pumpCreateInstructions.length > 0) {
                const meta = extractPumpFunTokenDetails(tx);
                parentPort?.postMessage({ type: 'result', meta });
            }
        }
    } catch (err) {
        parentPort?.postMessage({ type: 'error', error: err instanceof Error ? err.message : String(err) });
    }
}

parentPort?.on('message', async (data) => {
    if (data.type === 'init') {
        // Load proto for this topic
        data.ParsedIdlBlockMessage = await loadProto(data.topic);
        parentPort?.postMessage({ type: 'ready' });
    } else if (data.type === 'batch') {
        const { messages, topic } = data;
        const ParsedIdlBlockMessage = await loadProto(topic);
        for (const message of messages) {
            await processToken(ParsedIdlBlockMessage, message);
        }
        parentPort?.postMessage({ type: 'batchDone' });
    }
}); 

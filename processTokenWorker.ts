import { parentPort } from "worker_threads";
import { extractPumpFunTokenDetails } from "./ExtractToken2";
import bs58 from "bs58";
import chalk from "chalk";
import { loadProto } from "bitquery-protobuf-schema";

const pumpFunProgram = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
let ParsedIdlBlockMessage: any = null;

function logTimeDiff(decoded: any) {
    const timestampObj = decoded?.Header?.Timestamp;
    if (timestampObj?.low) {
        const blockTimeMs = timestampObj.low * 1000;
        const timeDiff = Date.now() - blockTimeMs;

        console.log(chalk.green("⏱ BlockTime Diff:"), chalk.red(`${timeDiff} ms`));

        if (timeDiff > 2000) {
            console.warn(chalk.yellow("⚠️ WARNING: Consumer lag detected!"));
        }
    }
}

async function processToken(buffer: Buffer) {
    try {
        if (!ParsedIdlBlockMessage) {
            throw new Error("ParsedIdlBlockMessage not initialized. Did you forget to send an init message?");
        }

        const decoded = ParsedIdlBlockMessage.decode(buffer);

        logTimeDiff(decoded);

        const txs = decoded.Transactions || [];
        for (const tx of txs) {
            if (!tx?.Status?.Success || !tx?.TotalBalanceUpdates?.length) continue;

            const hasPumpProgram = tx?.Header?.Accounts?.some(
                (a: any) => bs58.encode(a.Address) === pumpFunProgram
            );
            if (!hasPumpProgram) continue;

            const pumpInstructions = tx.ParsedIdlInstructions?.filter(
                (inst: any) =>
                    inst?.Program?.Name === "pump" &&
                    inst?.Program?.Method === "create"
            );

            if (pumpInstructions?.length > 0) {
                const meta = extractPumpFunTokenDetails(tx);
                parentPort?.postMessage({ type: "result", meta });
            }
        }
    } catch (err) {
        parentPort?.postMessage({
            type: "error",
            error: err instanceof Error ? err.message : String(err),
        });
    }
}

parentPort?.on("message", async (data) => {
    if (data.type === "init") {
        ParsedIdlBlockMessage = await loadProto(data.topic);
        parentPort?.postMessage({ type: "ready" });
    } else if (data.type === "batch") {
        if (!ParsedIdlBlockMessage) {
            parentPort?.postMessage({ type: "error", error: "Parser not initialized yet." });
            return;
        }

        const { messages } = data;
        await Promise.all(messages.map((m: any) => processToken(m.value)));

        parentPort?.postMessage({ type: "batchDone" });
    }
});

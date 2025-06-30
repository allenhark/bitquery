"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g = Object.create((typeof Iterator === "function" ? Iterator : Object).prototype);
    return g.next = verb(0), g["throw"] = verb(1), g["return"] = verb(2), typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
var worker_threads_1 = require("worker_threads");
var ExtractToken2_1 = require("./ExtractToken2");
var bs58_1 = __importDefault(require("bs58"));
var chalk_1 = __importDefault(require("chalk"));
var bitquery_protobuf_schema_1 = require("bitquery-protobuf-schema");
var pumpFunProgram = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
var ParsedIdlBlockMessage = null;
function logTimeDiff(decoded) {
    var _a;
    var timestampObj = (_a = decoded === null || decoded === void 0 ? void 0 : decoded.Header) === null || _a === void 0 ? void 0 : _a.Timestamp;
    if (timestampObj === null || timestampObj === void 0 ? void 0 : timestampObj.low) {
        var blockTimeMs = timestampObj.low * 1000;
        var timeDiff = Date.now() - blockTimeMs;
        console.log(chalk_1.default.green("⏱ BlockTime Diff:"), chalk_1.default.red("".concat(timeDiff, " ms")));
        if (timeDiff > 2000) {
            console.warn(chalk_1.default.yellow("⚠️ WARNING: Consumer lag detected!"));
        }
    }
}
function processToken(buffer) {
    return __awaiter(this, void 0, void 0, function () {
        var decoded, txs, _i, txs_1, tx, hasPumpProgram, pumpInstructions, meta;
        var _a, _b, _c, _d, _e;
        return __generator(this, function (_f) {
            try {
                if (!ParsedIdlBlockMessage) {
                    throw new Error("ParsedIdlBlockMessage not initialized. Did you forget to send an init message?");
                }
                decoded = ParsedIdlBlockMessage.decode(buffer);
                logTimeDiff(decoded);
                txs = decoded.Transactions || [];
                for (_i = 0, txs_1 = txs; _i < txs_1.length; _i++) {
                    tx = txs_1[_i];
                    if (!((_a = tx === null || tx === void 0 ? void 0 : tx.Status) === null || _a === void 0 ? void 0 : _a.Success) || !((_b = tx === null || tx === void 0 ? void 0 : tx.TotalBalanceUpdates) === null || _b === void 0 ? void 0 : _b.length))
                        continue;
                    hasPumpProgram = (_d = (_c = tx === null || tx === void 0 ? void 0 : tx.Header) === null || _c === void 0 ? void 0 : _c.Accounts) === null || _d === void 0 ? void 0 : _d.some(function (a) { return bs58_1.default.encode(a.Address) === pumpFunProgram; });
                    if (!hasPumpProgram)
                        continue;
                    pumpInstructions = (_e = tx.ParsedIdlInstructions) === null || _e === void 0 ? void 0 : _e.filter(function (inst) {
                        var _a, _b;
                        return ((_a = inst === null || inst === void 0 ? void 0 : inst.Program) === null || _a === void 0 ? void 0 : _a.Name) === "pump" &&
                            ((_b = inst === null || inst === void 0 ? void 0 : inst.Program) === null || _b === void 0 ? void 0 : _b.Method) === "create";
                    });
                    if ((pumpInstructions === null || pumpInstructions === void 0 ? void 0 : pumpInstructions.length) > 0) {
                        meta = (0, ExtractToken2_1.extractPumpFunTokenDetails)(tx);
                        worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({ type: "result", meta: meta });
                    }
                }
            }
            catch (err) {
                worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({
                    type: "error",
                    error: err instanceof Error ? err.message : String(err),
                });
            }
            return [2 /*return*/];
        });
    });
}
worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.on("message", function (data) { return __awaiter(void 0, void 0, void 0, function () {
    var messages;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                if (!(data.type === "init")) return [3 /*break*/, 2];
                return [4 /*yield*/, (0, bitquery_protobuf_schema_1.loadProto)(data.topic)];
            case 1:
                ParsedIdlBlockMessage = _a.sent();
                worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({ type: "ready" });
                return [3 /*break*/, 4];
            case 2:
                if (!(data.type === "batch")) return [3 /*break*/, 4];
                if (!ParsedIdlBlockMessage) {
                    worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({ type: "error", error: "Parser not initialized yet." });
                    return [2 /*return*/];
                }
                messages = data.messages;
                return [4 /*yield*/, Promise.all(messages.map(function (m) { return processToken(m.value); }))];
            case 3:
                _a.sent();
                worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({ type: "batchDone" });
                _a.label = 4;
            case 4: return [2 /*return*/];
        }
    });
}); });

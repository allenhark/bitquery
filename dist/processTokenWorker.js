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
var chalk_1 = __importDefault(require("chalk"));
var ExtractToken2_1 = require("./ExtractToken2");
var bs58_1 = __importDefault(require("bs58"));
var bitquery_protobuf_schema_1 = require("bitquery-protobuf-schema");
var pumpFunProgram = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
var convertBytes = function (buffer, encoding) {
    if (encoding === void 0) { encoding = 'base58'; }
    if (encoding === 'base58') {
        return bs58_1.default.encode(buffer);
    }
    return buffer.toString('hex');
};
var protobufToJson = function (msg, encoding) {
    if (encoding === void 0) { encoding = 'base58'; }
    var result = {};
    for (var _i = 0, _a = Object.entries(msg); _i < _a.length; _i++) {
        var _b = _a[_i], key = _b[0], value = _b[1];
        if (Array.isArray(value)) {
            result[key] = value.map(function (item) {
                if (typeof item === 'object' && item !== null) {
                    return protobufToJson(item, encoding);
                }
                else {
                    return item;
                }
            });
        }
        else if (value && typeof value === 'object' && Buffer.isBuffer(value)) {
            result[key] = convertBytes(value, encoding);
        }
        else if (value && typeof value === 'object') {
            result[key] = protobufToJson(value, encoding);
        }
        else {
            result[key] = value;
        }
    }
    return result;
};
function processToken(ParsedIdlBlockMessage, message) {
    return __awaiter(this, void 0, void 0, function () {
        var buffer, decoded, msgObj, jsonOutput, transactions, blockTime, timeDiff, filteredTxs, _i, filteredTxs_1, tx, hasPumpProgram, pumpCreateInstructions, meta;
        var _a, _b, _c, _d, _e;
        return __generator(this, function (_f) {
            try {
                buffer = message.value;
                decoded = ParsedIdlBlockMessage.decode(buffer);
                msgObj = ParsedIdlBlockMessage.toObject(decoded, { bytes: Buffer });
                jsonOutput = protobufToJson(msgObj);
                transactions = jsonOutput.Transactions || [];
                // console.log(jsonOutput?.Header)
                if ((_a = jsonOutput === null || jsonOutput === void 0 ? void 0 : jsonOutput.Header) === null || _a === void 0 ? void 0 : _a.Timestamp) {
                    blockTime = ((_b = jsonOutput === null || jsonOutput === void 0 ? void 0 : jsonOutput.Header) === null || _b === void 0 ? void 0 : _b.Timestamp.low) ? ((_c = jsonOutput === null || jsonOutput === void 0 ? void 0 : jsonOutput.Header) === null || _c === void 0 ? void 0 : _c.Timestamp.low) * 1000 : Date.now();
                    timeDiff = Date.now() - blockTime;
                    console.log(chalk_1.default.green('Time Diff :'), chalk_1.default.red(timeDiff), 'ms');
                }
                return [2 /*return*/];
                if (transactions.length === 0)
                    return [2 /*return*/];
                filteredTxs = transactions.filter(function (tx) {
                    var _a;
                    return Array.isArray(tx.ParsedIdlInstructions) &&
                        tx.TotalBalanceUpdates.length > 0 &&
                        ((_a = tx.Status) === null || _a === void 0 ? void 0 : _a.Success) === true;
                });
                for (_i = 0, filteredTxs_1 = filteredTxs; _i < filteredTxs_1.length; _i++) {
                    tx = filteredTxs_1[_i];
                    hasPumpProgram = (_e = (_d = tx === null || tx === void 0 ? void 0 : tx.Header) === null || _d === void 0 ? void 0 : _d.Accounts) === null || _e === void 0 ? void 0 : _e.some(function (account) {
                        return account.Address === pumpFunProgram;
                    });
                    // console.log('Has pump fun program: ', hasPumpProgram)
                    if (!hasPumpProgram)
                        continue;
                    pumpCreateInstructions = tx.ParsedIdlInstructions.filter(function (inst) {
                        var _a, _b;
                        return ((_a = inst === null || inst === void 0 ? void 0 : inst.Program) === null || _a === void 0 ? void 0 : _a.Name) === 'pump' &&
                            ((_b = inst === null || inst === void 0 ? void 0 : inst.Program) === null || _b === void 0 ? void 0 : _b.Method) === 'create';
                    });
                    if (pumpCreateInstructions.length > 0) {
                        meta = (0, ExtractToken2_1.extractPumpFunTokenDetails)(tx);
                        worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({ type: 'result', meta: meta });
                    }
                }
            }
            catch (err) {
                worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({ type: 'error', error: err instanceof Error ? err.message : String(err) });
            }
            return [2 /*return*/];
        });
    });
}
worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.on('message', function (data) { return __awaiter(void 0, void 0, void 0, function () {
    var _a, messages, topic, ParsedIdlBlockMessage, _i, messages_1, message;
    return __generator(this, function (_b) {
        switch (_b.label) {
            case 0:
                if (!(data.type === 'init')) return [3 /*break*/, 2];
                // Load proto for this topic
                _a = data;
                return [4 /*yield*/, (0, bitquery_protobuf_schema_1.loadProto)(data.topic)];
            case 1:
                // Load proto for this topic
                _a.ParsedIdlBlockMessage = _b.sent();
                worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({ type: 'ready' });
                return [3 /*break*/, 8];
            case 2:
                if (!(data.type === 'batch')) return [3 /*break*/, 8];
                messages = data.messages, topic = data.topic;
                return [4 /*yield*/, (0, bitquery_protobuf_schema_1.loadProto)(topic)];
            case 3:
                ParsedIdlBlockMessage = _b.sent();
                _i = 0, messages_1 = messages;
                _b.label = 4;
            case 4:
                if (!(_i < messages_1.length)) return [3 /*break*/, 7];
                message = messages_1[_i];
                return [4 /*yield*/, processToken(ParsedIdlBlockMessage, message)];
            case 5:
                _b.sent();
                _b.label = 6;
            case 6:
                _i++;
                return [3 /*break*/, 4];
            case 7:
                worker_threads_1.parentPort === null || worker_threads_1.parentPort === void 0 ? void 0 : worker_threads_1.parentPort.postMessage({ type: 'batchDone' });
                _b.label = 8;
            case 8: return [2 /*return*/];
        }
    });
}); });

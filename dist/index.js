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
var chalk_1 = __importDefault(require("chalk"));
var worker_threads_1 = require("worker_threads");
var path_1 = __importDefault(require("path"));
var dotenv_1 = __importDefault(require("dotenv"));
var kafkajs_1 = require("kafkajs");
var kafkajs_lz4_1 = __importDefault(require("kafkajs-lz4"));
var uuid_1 = require("uuid");
dotenv_1.default.config();
kafkajs_1.CompressionCodecs[kafkajs_1.CompressionTypes.LZ4] = new kafkajs_lz4_1.default().codec;
var username = process.env.username;
var password = process.env.password;
var topic = "solana.transactions.proto";
var id = (0, uuid_1.v4)();
// Map to keep track of workers per partition
var partitionWorkers = new Map();
// Spawn and cache a worker per partition
function getWorkerForPartition(partition, topic) {
    if (!partitionWorkers.has(partition)) {
        var worker = new worker_threads_1.Worker(path_1.default.resolve(__dirname, "./processTokenWorker.js"));
        worker.postMessage({ type: "init", topic: topic });
        worker.on("message", function (msg) {
            if (msg.type === "result") {
                console.log(chalk_1.default.cyan("🚀 Pump.fun token creation detected (from worker)"));
                console.dir(msg.meta, { depth: 5 });
            }
            else if (msg.type === "error") {
                console.error("Worker error:", msg.error);
            }
        });
        worker.on("error", function (err) {
            console.error("Worker thread error:", err);
        });
        partitionWorkers.set(partition, worker);
    }
    return partitionWorkers.get(partition);
}
// Kafka client setup
var kafka = new kafkajs_1.Kafka({
    clientId: username,
    brokers: ["rpk0.bitquery.io:9092", "rpk1.bitquery.io:9092", "rpk2.bitquery.io:9092"],
    sasl: {
        mechanism: "scram-sha-512",
        username: username,
        password: password,
    },
});
var consumer = kafka.consumer({
    groupId: "".concat(username, "-").concat(id),
    sessionTimeout: 10000,
    heartbeatInterval: 3000,
    maxWaitTimeInMs: 25,
    allowAutoTopicCreation: false,
});
var run = function () { return __awaiter(void 0, void 0, void 0, function () {
    var error_1;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                _a.trys.push([0, 4, , 5]);
                return [4 /*yield*/, consumer.connect()];
            case 1:
                _a.sent();
                return [4 /*yield*/, consumer.subscribe({ topic: topic, fromBeginning: false })];
            case 2:
                _a.sent();
                return [4 /*yield*/, consumer.run({
                        autoCommit: false,
                        partitionsConsumedConcurrently: 6,
                        eachBatchAutoResolve: true,
                        //fetchMinBytes: 1,
                        //fetchMaxBytes: 1048576,
                        eachBatch: function (_a) { return __awaiter(void 0, [_a], void 0, function (_b) {
                            var worker, messages, _i, _c, message;
                            var batch = _b.batch, heartbeat = _b.heartbeat, resolveOffset = _b.resolveOffset, commitOffsetsIfNecessary = _b.commitOffsetsIfNecessary;
                            return __generator(this, function (_d) {
                                switch (_d.label) {
                                    case 0:
                                        worker = getWorkerForPartition(batch.partition, topic);
                                        messages = batch.messages.map(function (msg) { return ({
                                            value: msg.value,
                                        }); });
                                        worker.postMessage({ type: "batch", messages: messages });
                                        for (_i = 0, _c = batch.messages; _i < _c.length; _i++) {
                                            message = _c[_i];
                                            resolveOffset(message.offset);
                                        }
                                        return [4 /*yield*/, commitOffsetsIfNecessary()];
                                    case 1:
                                        _d.sent();
                                        return [4 /*yield*/, heartbeat()];
                                    case 2:
                                        _d.sent();
                                        return [2 /*return*/];
                                }
                            });
                        }); },
                    })];
            case 3:
                _a.sent();
                return [3 /*break*/, 5];
            case 4:
                error_1 = _a.sent();
                console.error("Error running consumer:", error_1);
                return [3 /*break*/, 5];
            case 5: return [2 /*return*/];
        }
    });
}); };
run().catch(console.error);

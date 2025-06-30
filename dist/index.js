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
// Import required modules
var Kafka = require('kafkajs').Kafka;
var bs58 = require('bs58');
var _a = require('kafkajs'), CompressionTypes = _a.CompressionTypes, CompressionCodecs = _a.CompressionCodecs;
var LZ4 = require('kafkajs-lz4');
var uuidv4 = require('uuid').v4;
//const config = require('./config.json');  //credentials imported from JSON file
// Enable LZ4 compression
CompressionCodecs[CompressionTypes.LZ4] = new LZ4().codec;
var dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
// Configuration
var username = process.env.username; //''; //credentials imported from JSON file
var password = process.env.password; //'';
var topic = 'solana.transactions.proto';
var id = '312jdssdj'; //uuidv4();
// Map to keep track of workers per partition
var partitionWorkers = new Map();
// Helper to get or create a worker for a partition
function getWorkerForPartition(partition, topic) {
    if (!partitionWorkers.has(partition)) {
        var worker = new worker_threads_1.Worker(path_1.default.resolve(__dirname, './processTokenWorker.js'));
        worker.postMessage({ type: 'init', topic: topic });
        worker.on('message', function (msg) {
            if (msg.type === 'result') {
                console.log(chalk_1.default.cyan('🚀 Pump.fun token creation detected (from worker)'));
                console.log(msg.meta);
            }
            else if (msg.type === 'error') {
                console.error('Worker error:', msg.error);
            }
        });
        worker.on('error', function (err) {
            console.error('Worker thread error:', err);
        });
        partitionWorkers.set(partition, worker);
    }
    return partitionWorkers.get(partition);
}
// Initialize Kafka Client (Non-SSL)
var kafka = new Kafka({
    clientId: username,
    brokers: ['rpk0.bitquery.io:9092', 'rpk1.bitquery.io:9092', 'rpk2.bitquery.io:9092'],
    sasl: {
        mechanism: 'scram-sha-512',
        username: username,
        password: password,
    },
});
// Function to convert bytes to base58
var convertBytes = function (buffer, encoding) {
    if (encoding === void 0) { encoding = 'base58'; }
    if (encoding === 'base58') {
        return bs58.default.encode(buffer);
    }
    return buffer.toString('hex');
};
// Recursive function to convert Protobuf messages to JSON
var protobufToJson = function (msg, encoding) {
    if (encoding === void 0) { encoding = 'base58'; }
    var result = {};
    for (var _i = 0, _a = Object.entries(msg); _i < _a.length; _i++) {
        var _b = _a[_i], key = _b[0], value = _b[1];
        if (Array.isArray(value)) {
            result[key] = value.map(function (item, idx) {
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
// Initialize consumer with more aggressive settings
var consumer = kafka.consumer({
    groupId: "".concat(username, "-").concat(id),
    sessionTimeout: 60000,
    heartbeatInterval: 5000,
    allowAutoTopicCreation: false,
    maxWaitTimeInMs: 50,
    maxBytes: 2097152,
    fetchMaxBytes: 1048576,
    fetchMinBytes: 1,
});
// Run the consumer
var run = function () { return __awaiter(void 0, void 0, void 0, function () {
    var error_1;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                _a.trys.push([0, 4, , 5]);
                return [4 /*yield*/, consumer.connect()];
            case 1:
                _a.sent();
                return [4 /*yield*/, consumer.subscribe({
                        topic: topic,
                        fromBeginning: false
                    })];
            case 2:
                _a.sent();
                return [4 /*yield*/, consumer.run({
                        autoCommit: true,
                        autoCommitInterval: 500,
                        autoCommitThreshold: 50,
                        partitionsConsumedConcurrently: 4,
                        eachBatch: function (_a) { return __awaiter(void 0, [_a], void 0, function (_b) {
                            var worker;
                            var batch = _b.batch, partition = _b.partition;
                            return __generator(this, function (_c) {
                                worker = getWorkerForPartition(partition, topic);
                                worker.postMessage({ type: 'batch', messages: batch.messages, topic: topic });
                                return [2 /*return*/];
                            });
                        }); },
                    })];
            case 3:
                _a.sent();
                return [3 /*break*/, 5];
            case 4:
                error_1 = _a.sent();
                console.error('Error running consumer:', error_1);
                return [3 /*break*/, 5];
            case 5: return [2 /*return*/];
        }
    });
}); };
// Start the consumer
run().catch(console.error);

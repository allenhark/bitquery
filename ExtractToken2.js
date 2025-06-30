"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.extractPumpFunTokenDetails = extractPumpFunTokenDetails;
var web3_js_1 = require("@solana/web3.js");
var DEFAULT_COMMITMENT = "processed";
function extractPumpFunTokenDetails(tokenTx) {
    var _a, _b, _c, _d, _e, _f, _g, _h, _j, _k, _l, _m;
    var result = {};
    var signature = tokenTx.Signature;
    result.txSignature = signature;
    var createInstr = (_a = tokenTx === null || tokenTx === void 0 ? void 0 : tokenTx.ParsedIdlInstructions) === null || _a === void 0 ? void 0 : _a.find(function (i) {
        var _a, _b;
        return ((_a = i.Program) === null || _a === void 0 ? void 0 : _a.Method) === "create" &&
            ((_b = i.Program) === null || _b === void 0 ? void 0 : _b.Name) === "pump";
    });
    if (!createInstr)
        throw new Error("Create instruction not found");
    var mintAccount = (_c = (_b = createInstr.Accounts) === null || _b === void 0 ? void 0 : _b[0]) === null || _c === void 0 ? void 0 : _c.Address;
    if (!mintAccount)
        throw new Error("Mint account not found");
    result.mint = new web3_js_1.PublicKey(mintAccount);
    var creatorArg = (_d = createInstr.Arguments) === null || _d === void 0 ? void 0 : _d.find(function (arg) { return arg.Name === "creator"; });
    var creatorAddress = creatorArg === null || creatorArg === void 0 ? void 0 : creatorArg.Address;
    if (!creatorAddress)
        throw new Error("Creator address not found");
    result.tokenCreator = new web3_js_1.PublicKey(creatorAddress);
    result.feeRecipient = new web3_js_1.PublicKey(creatorAddress);
    var allAccounts = ((_f = (_e = tokenTx === null || tokenTx === void 0 ? void 0 : tokenTx.Header) === null || _e === void 0 ? void 0 : _e.Accounts) === null || _f === void 0 ? void 0 : _f.map(function (acc) { return acc.Address; })) || [];
    result.allAccounts = allAccounts.map(function (a) { return new web3_js_1.PublicKey(a); });
    var bondingCurve = (_g = result.allAccounts) === null || _g === void 0 ? void 0 : _g.find(function (pk) {
        return pk.toBase58() !== result.mint.toBase58() &&
            pk.toBase58() !== result.tokenCreator.toBase58();
    });
    if (!bondingCurve)
        throw new Error("Bonding curve account not found");
    result.bondingCurve = bondingCurve;
    var nameArg = (_h = createInstr === null || createInstr === void 0 ? void 0 : createInstr.Arguments) === null || _h === void 0 ? void 0 : _h.find(function (arg) { return arg.Name === "name"; });
    var symbolArg = (_j = createInstr === null || createInstr === void 0 ? void 0 : createInstr.Arguments) === null || _j === void 0 ? void 0 : _j.find(function (arg) { return arg.Name === "symbol"; });
    var uriArg = (_k = createInstr === null || createInstr === void 0 ? void 0 : createInstr.Arguments) === null || _k === void 0 ? void 0 : _k.find(function (arg) { return arg.Name === "uri"; });
    result.tokenName = (nameArg === null || nameArg === void 0 ? void 0 : nameArg.String) || "";
    result.tokenSymbol = (symbolArg === null || symbolArg === void 0 ? void 0 : symbolArg.String) || "";
    result.tokenUri = (uriArg === null || uriArg === void 0 ? void 0 : uriArg.String) || "";
    result.metadata = { name: result.tokenName, symbol: result.tokenSymbol, uri: result.tokenUri };
    // Estimate amount purchased
    var balanceUpdates = (tokenTx === null || tokenTx === void 0 ? void 0 : tokenTx.BalanceUpdates) || (tokenTx === null || tokenTx === void 0 ? void 0 : tokenTx.TotalBalanceUpdates) || [];
    var bondingCurveIndex = (_m = (_l = tokenTx === null || tokenTx === void 0 ? void 0 : tokenTx.Header) === null || _l === void 0 ? void 0 : _l.Accounts) === null || _m === void 0 ? void 0 : _m.findIndex(function (acc) { return acc.Address === (bondingCurve === null || bondingCurve === void 0 ? void 0 : bondingCurve.toBase58()); });
    var bondingCurveBalanceUpdate = balanceUpdates.find(function (b) { return b.AccountIndex === bondingCurveIndex; });
    if ((bondingCurveBalanceUpdate === null || bondingCurveBalanceUpdate === void 0 ? void 0 : bondingCurveBalanceUpdate.PreBalance) && (bondingCurveBalanceUpdate === null || bondingCurveBalanceUpdate === void 0 ? void 0 : bondingCurveBalanceUpdate.PostBalance)) {
        var deltaLamports = BigInt(bondingCurveBalanceUpdate.PostBalance.low) -
            BigInt(bondingCurveBalanceUpdate.PreBalance.low);
        result.amountPurchased = Number(deltaLamports) / web3_js_1.LAMPORTS_PER_SOL;
    }
    else {
        result.amountPurchased = 0;
    }
    result.userAta = undefined;
    return result;
}

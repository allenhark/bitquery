import {
    PublicKey,
    Commitment,
    LAMPORTS_PER_SOL,
} from "@solana/web3.js";

type TokenDetails = {
    mint: PublicKey;
    bondingCurve: PublicKey;
    feeRecipient: PublicKey;
    tokenCreator: PublicKey;
    userAta: PublicKey;
    metadata: any;
    amountPurchased: number;
    allAccounts: PublicKey[];
    txSignature: string;

    tokenName: string;
    tokenSymbol: string;
    tokenUri: string;
};

const DEFAULT_COMMITMENT: Commitment = "processed";

export function extractPumpFunTokenDetails(tokenTx: any): TokenDetails {
    const result: Partial<TokenDetails> = {};

    const signature = tokenTx.Signature;
    result.txSignature = signature;

    const createInstr = tokenTx?.ParsedIdlInstructions?.find(
        (i: any) =>
            i.Program?.Method === "create" &&
            i.Program?.Name === "pump"
    );
    if (!createInstr) throw new Error("Create instruction not found");

    const mintAccount = createInstr.Accounts?.[0]?.Address;
    if (!mintAccount) throw new Error("Mint account not found");
    result.mint = new PublicKey(mintAccount);

    const creatorArg = createInstr.Arguments?.find((arg: any) => arg.Name === "creator");
    const creatorAddress = creatorArg?.Address;
    if (!creatorAddress) throw new Error("Creator address not found");
    result.tokenCreator = new PublicKey(creatorAddress);
    result.feeRecipient = new PublicKey(creatorAddress);

    const allAccounts = tokenTx?.Header?.Accounts?.map((acc: any) => acc.Address) || [];
    result.allAccounts = allAccounts.map((a: string) => new PublicKey(a));

    const bondingCurve = result.allAccounts?.find(
        (pk) =>
            pk.toBase58() !== result.mint!.toBase58() &&
            pk.toBase58() !== result.tokenCreator!.toBase58()
    );
    if (!bondingCurve) throw new Error("Bonding curve account not found");
    result.bondingCurve = bondingCurve;

    const nameArg = createInstr?.Arguments?.find((arg: any) => arg.Name === "name");
    const symbolArg = createInstr?.Arguments?.find((arg: any) => arg.Name === "symbol");
    const uriArg = createInstr?.Arguments?.find((arg: any) => arg.Name === "uri");

    result.tokenName = nameArg?.String || "";
    result.tokenSymbol = symbolArg?.String || "";
    result.tokenUri = uriArg?.String || "";
    result.metadata = { name: result.tokenName, symbol: result.tokenSymbol, uri: result.tokenUri };

    // Estimate amount purchased
    const balanceUpdates = tokenTx?.BalanceUpdates || tokenTx?.TotalBalanceUpdates || [];
    const bondingCurveIndex = tokenTx?.Header?.Accounts?.findIndex(
        (acc: any) => acc.Address === bondingCurve?.toBase58()
    );

    const bondingCurveBalanceUpdate = balanceUpdates.find(
        (b: any) => b.AccountIndex === bondingCurveIndex
    );

    if (bondingCurveBalanceUpdate?.PreBalance && bondingCurveBalanceUpdate?.PostBalance) {
        const deltaLamports =
            BigInt(bondingCurveBalanceUpdate.PostBalance.low) -
            BigInt(bondingCurveBalanceUpdate.PreBalance.low);
        result.amountPurchased = Number(deltaLamports) / LAMPORTS_PER_SOL;
    } else {
        result.amountPurchased = 0;
    }

    result.userAta = undefined!;
    return result as TokenDetails;
}


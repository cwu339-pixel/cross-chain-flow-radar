import { ethers } from "ethers";
import fs from "fs";

const RPC = process.env.ZETA_RPC || "https://zetachain-athens-evm.blockpi.network/v1/rpc/public";
const REG = (process.env.REGISTRY || process.env.REGISTRY_ADDRESS || "").toLowerCase();

let tx = process.argv[2];
if (!tx) { console.error("Usage: node scripts/decode.js <tx-hash>"); process.exit(1); }
if (!tx.startsWith("0x")) tx = "0x" + tx;

const provider = new ethers.JsonRpcProvider(RPC);

// 👇 与你的合约事件完全一致（uint256）
const SIG = "BriefingPublished(string,string,bytes32,bool,uint256,string,string)";
const TOPIC0 = ethers.id(SIG);
const iface = new ethers.Interface([`event ${SIG}`]);

const receipt = await provider.getTransactionReceipt(tx);
if (!receipt) { console.error("Receipt not found (wrong network or tx hash?)"); process.exit(1); }

// 只保留：匹配合约地址(如提供) + topic0 正确 的日志
const logs = receipt.logs.filter(l => {
  const okAddr = !REG || l.address.toLowerCase() === REG;
  const okTop  = (l.topics?.[0] || "").toLowerCase() === TOPIC0.toLowerCase();
  return okAddr && okTop;
});

if (logs.length === 0) {
  console.error("No BriefingPublished logs found for this tx.");
  console.error("Contract filter:", REG || "(none)");
  console.error("Available topic0s:", receipt.logs.map(l => l.topics?.[0]).join(", "));
  process.exit(2);
}

// 解析第一条匹配日志（通常就一条）
const log = logs[0];
const parsed = iface.parseLog(log);
// 使用“按索引”读取，避免名字不对造成 undefined
const out = {
  contract: log.address,
  txHash: tx,
  blockNumber: receipt.blockNumber,
  day: parsed.args[0],
  chain: parsed.args[1],
  summaryHash: parsed.args[2],
  hasAnomaly: parsed.args[3],
  evidenceRows: Number(parsed.args[4]),
  model: parsed.args[5],
  uri: parsed.args[6],
};

fs.mkdirSync("proofs", { recursive: true });
const file = `proofs/proof-${tx}.json`;
fs.writeFileSync(file, JSON.stringify(out, null, 2));
console.log("Saved →", file);
console.log(out);

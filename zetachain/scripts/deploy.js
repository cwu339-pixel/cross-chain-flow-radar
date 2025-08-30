import hre from "hardhat";

async function main() {
  const F = await hre.ethers.getContractFactory("RadarBriefingRegistry");
  const c = await F.deploy();

  // ethers v6: 等待部署 & 读取地址
  await c.waitForDeployment();
  const addr = await c.getAddress();

  console.log("RadarBriefingRegistry deployed to:", addr);
}

main().catch((e) => { console.error(e); process.exit(1); });

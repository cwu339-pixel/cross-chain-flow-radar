import "@nomicfoundation/hardhat-ethers";

/** @type import('hardhat/config').HardhatUserConfig */
export default {
  solidity: "0.8.20",
  networks: {
    zetachain_testnet: {
      url: process.env.ZETA_RPC || "https://zetachain-athens-evm.blockpi.network/v1/rpc/public",
      chainId: 7001,
      accounts: [process.env.PRIVATE_KEY || "0x0000000000000000000000000000000000000000000000000000000000000000"],
    },
    zetachain_mainnet: {
      url: process.env.ZETA_RPC || "https://zetachain-evm.blockpi.network/v1/rpc/public",
      chainId: 7000,
      accounts: [process.env.PRIVATE_KEY || "0x0000000000000000000000000000000000000000000000000000000000000000"],
    },
  },
};

# Cross-Chain Flow Radar (ETH-only MVP)

> **Core Flow:** Daily ETL → per chain/bridge/token NetFlow (USD) → anomaly explanation with Gemini → on-chain attestation on ZetaChain (Testnet).

A Minimum Viable Product (MVP) for monitoring and explaining cross-chain stablecoin flows, focused on Ethereum.

## 1. System Architecture

The system is composed of three main components that run sequentially.

### 1.1 ETL (BigQuery Scheduled Query)
- **Extracts** token transfers where `to` or `from` equals a known bridge address.
- **Filters** for major stablecoins (`USDT`, `USDC`, `DAI`, `TUSD`, `USDP`, `FRAX`, `USDe`) and treats their value as **1:1 USD**.
- **Aggregates** metrics by `day × chain × bridge × token`, calculating:
  - `in_amount_usd`: Total USD value transferred **to** the bridge (deposits).
  - `out_amount_usd`: Total USD value transferred **from** the bridge (withdrawals).
  - `net_amount_usd`: The net flow (`in_amount_usd - out_amount_usd`).
  - `tx_count`: The number of transfer transactions.
  - `unique_wallets`: The count of unique counterparty wallets.
- **Writes** results to `xchain_radar.flows_daily`.

### 1.2 Briefing Service (Cloud Run)
- **Service:** `xchain-briefing` deployed in `us-central1` (Python 3.11).
- **Endpoint:** `GET /explain?day=YYYY-MM-DD&chain=ethereum`
- **Logic:**
  1.  Computes **contrast evidence** by comparing the target day's flow against the previous day and the 7-day average.
  2.  Calls Google's Vertex AI (`gemini-1.5-flash-002` model) to generate an English briefing.
  3.  **If no anomaly** is detected, it explains the deltas and provides context.
  4.  **If an anomaly** is detected, it highlights the key `bridge × token` pairs and suggests hypotheses.
  5.  Upserts the briefing into `xchain_radar.daily_briefings`.

### 1.3 On-chain Attestation (ZetaChain)
- **Network:** ZetaChain Athens-3 (Testnet, `chainId: 7001`).
- **Contract:** `RadarBriefingRegistry.sol`.
- **Process:** After a briefing is generated, an off-chain script (`attest/attest.py`) publishes a `BriefingPublished` event to the smart contract.
- **Event Definition:**
  ```solidity
  event BriefingPublished(
      string day,
      string chain,
      bytes32 summaryHash,
      bool hasAnomaly,
      uint256 evidenceRows,
      string model,
      string uri
  );


## 2. Cloud & Data Infrastructure

- **Google Cloud Project:** `faefw-468503`
- **BigQuery Dataset:** `xchain_radar` (Location: US, Data Timezone: Asia/Tokyo)

### 2.1 Data Tables
- **`flows_daily`**: Aggregated daily metrics for each `day×chain×bridge×token`.
- **`daily_briefings`**: Stores the LLM-generated summary text and a JSON snapshot of the source data used as evidence.
- **`address_book_bridges_v2`**: A table mapping bridge names to their on-chain addresses.
  - **Seeds (Ethereum):** Stargate, Across, cBridge.

### 2.2 Data Model (`flows_daily`)
| Column | Type | Description |
| :--- | :--- | :--- |
| `day` | `DATE` | The date of the flow (in Asia/Tokyo timezone). |
| `chain` | `STRING` | The blockchain, e.g., `ethereum`. |
| `bridge` | `STRING` | The bridge name, e.g., `stargate`. |
| `token_symbol` | `STRING` | The stablecoin symbol, e.g., `USDC`. |
| `in_amount_usd` | `BIGNUMERIC` | Sum of USD value flowing into the bridge. |
| `out_amount_usd` | `BIGNUMERIC`| Sum of USD value flowing out of the bridge. |
| `net_amount_usd` | `BIGNUMERIC`| Net flow (`in - out`). |
| `tx_count` | `INT64` | Total number of transfers. |
| `unique_wallets`| `INT64` | Count of unique counterparty wallets. |


---

## 3. How to Reproduce

### 3.1 ETL
The ETL is a BigQuery Scheduled Query. To run it manually, execute the contents of the following file in the BigQuery console:
- **File:** `sql/etl_yesterday_eth.sql`

### 3.2 Briefing Service
The service endpoint requires an OIDC identity token for authentication.
- **Command:**
  ```bash
  # Obtain a token for your gcloud identity or a service account
  TOKEN=$(gcloud auth print-identity-token)
  
  # Set the Cloud Run service URL
  RUN_URL="<YOUR_CLOUD_RUN_SERVICE_URL>"
  
  curl --http1.1 -H "Authorization: Bearer $TOKEN" \
    "$RUN_URL?day=2025-08-29&chain=ethereum"



  好的，完全理解。

您说得对，整个文件放在一个大文本块里的确不方便预览和修改，看起来也不够清晰。我们换回模块化的方式，这样最灵活。

下面，我将项目的 README.md 分解成了 8个独立的模块。您可以一部分一部分地按顺序复制，然后粘贴到您的 GitHub 文件中。每个模块都是一个带“复制”按钮的代码框，这次不会有任何问题了。

第一部分：标题和概述
这是文档的开头，包含项目名称和核心流程简介。

Markdown

# Cross-Chain Flow Radar (ETH-only MVP)

> **Core Flow:** Daily ETL → per chain/bridge/token NetFlow (USD) → anomaly explanation with Gemini → on-chain attestation on ZetaChain (Testnet).

A Minimum Viable Product (MVP) for monitoring and explaining cross-chain stablecoin flows, focused on Ethereum.
第二部分：系统架构 (System Architecture)
这部分详细描述了项目的三个核心组件：ETL、简报服务和链上证明。

Markdown

## 1. System Architecture

The system is composed of three main components that run sequentially.

### 1.1 ETL (BigQuery Scheduled Query)
- **Extracts** token transfers where `to` or `from` equals a known bridge address.
- **Filters** for major stablecoins (`USDT`, `USDC`, `DAI`, `TUSD`, `USDP`, `FRAX`, `USDe`) and treats their value as **1:1 USD**.
- **Aggregates** metrics by `day × chain × bridge × token`, calculating:
  - `in_amount_usd`: Total USD value transferred **to** the bridge (deposits).
  - `out_amount_usd`: Total USD value transferred **from** the bridge (withdrawals).
  - `net_amount_usd`: The net flow (`in_amount_usd - out_amount_usd`).
  - `tx_count`: The number of transfer transactions.
  - `unique_wallets`: The count of unique counterparty wallets.
- **Writes** results to `xchain_radar.flows_daily`.

### 1.2 Briefing Service (Cloud Run)
- **Service:** `xchain-briefing` deployed in `us-central1` (Python 3.11).
- **Endpoint:** `GET /explain?day=YYYY-MM-DD&chain=ethereum`
- **Logic:**
  1.  Computes **contrast evidence** by comparing the target day's flow against the previous day and the 7-day average.
  2.  Calls Google's Vertex AI (`gemini-1.5-flash-002` model) to generate an English briefing.
  3.  **If no anomaly** is detected, it explains the deltas and provides context.
  4.  **If an anomaly** is detected, it highlights the key `bridge × token` pairs and suggests hypotheses.
  5.  Upserts the briefing into `xchain_radar.daily_briefings`.

### 1.3 On-chain Attestation (ZetaChain)
- **Network:** ZetaChain Athens-3 (Testnet, `chainId: 7001`).
- **Contract:** `RadarBriefingRegistry.sol`.
- **Process:** After a briefing is generated, an off-chain script (`attest/attest.py`) publishes a `BriefingPublished` event to the smart contract.
- **Event Definition:**
  ```solidity
  event BriefingPublished(
      string day,
      string chain,
      bytes32 summaryHash,
      bool hasAnomaly,
      uint256 evidenceRows,
      string model,
      string uri
  );
A decoding script (zetachain/scripts/decode.js) can be used to read the event from a transaction hash and print a human-readable proof.


## 2. Cloud & Data Infrastructure

- **Google Cloud Project:** `faefw-468503`
- **BigQuery Dataset:** `xchain_radar` (Location: US, Data Timezone: Asia/Tokyo)

### 2.1 Data Tables
- **`flows_daily`**: Aggregated daily metrics for each `day×chain×bridge×token`.
- **`daily_briefings`**: Stores the LLM-generated summary text and a JSON snapshot of the source data used as evidence.
- **`address_book_bridges_v2`**: A table mapping bridge names to their on-chain addresses.
  - **Seeds (Ethereum):** Stargate, Across, cBridge.

### 2.2 Data Model (`flows_daily`)
| Column | Type | Description |
| :--- | :--- | :--- |
| `day` | `DATE` | The date of the flow (in Asia/Tokyo timezone). |
| `chain` | `STRING` | The blockchain, e.g., `ethereum`. |
| `bridge` | `STRING` | The bridge name, e.g., `stargate`. |
| `token_symbol` | `STRING` | The stablecoin symbol, e.g., `USDC`. |
| `in_amount_usd` | `BIGNUMERIC` | Sum of USD value flowing into the bridge. |
| `out_amount_usd` | `BIGNUMERIC`| Sum of USD value flowing out of the bridge. |
| `net_amount_usd` | `BIGNUMERIC`| Net flow (`in - out`). |
| `tx_count` | `INT64` | Total number of transfers. |
| `unique_wallets`| `INT64` | Count of unique counterparty wallets. |


## 3. How to Reproduce

### 3.1 ETL
The ETL is a BigQuery Scheduled Query. To run it manually, execute the contents of the following file in the BigQuery console:
- **File:** `sql/etl_yesterday_eth.sql`

### 3.2 Briefing Service
The service endpoint requires an OIDC identity token for authentication.
- **Command:**
  ```bash
  # Obtain a token for your gcloud identity or a service account
  TOKEN=$(gcloud auth print-identity-token)
  
  # Set the Cloud Run service URL
  RUN_URL="<YOUR_CLOUD_RUN_SERVICE_URL>"
  
  curl --http1.1 -H "Authorization: Bearer $TOKEN" \
    "$RUN_URL?day=2025-08-29&chain=ethereum"

### 3.3 On-chain Attestation
Network: Athens-3 (chainId: 7001)

Contract Address: 0x5201535153B7719715df898F82196a9948805dE4

Publish Proof (Example):
# Set environment variables
export ZETA_RPC="[https://zetachain-athens-evm.blockpi.network/v1/rpc/public](https://zetachain-athens-evm.blockpi.network/v1/rpc/public)"
export ZETA_CONTRACT="0x5201535153B7719715df898F82196a9948805dE4"
export ZETA_CHAIN_ID=7001
export ZETA_PRIVATE_KEY="0x<YOUR_TESTNET_PRIVATE_KEY>"

# Run the attestation script for a date range
python attest/attest.py --chain ethereum --start 2025-08-23 --end 2025-08-29




## 4. Automation & Scheduling

1.  **BigQuery Scheduled Query:**
    - **Schedule:** Runs daily at `07:55 Asia/Tokyo`.
    - **Action:** Executes `sql/etl_yesterday_eth.sql` to process the previous day's data.

2.  **Cloud Scheduler → Cloud Run:**
    - **Schedule:** Runs daily at `08:05 Asia/Tokyo`.
    - **Action:** Triggers the Cloud Run service (`GET /explain?...`) to generate the briefing for the previous day.
    - **Authentication:** Uses OIDC with the service account `cc-radar-sa@faefw-468503.iam.gserviceaccount.com`, which has the `roles/run.invoker` permission.


## 5. Repository Layout

├── cloudrun/
│   ├── main.py         # Cloud Run service entrypoint
│   └── requirements.txt
├── sql/
│   └── etl_yesterday_eth.sql # BigQuery daily ETL script
├── attest/
│   └── attest.py       # Script to publish briefings to ZetaChain
└── zetachain/
├── contracts/RadarBriefingRegistry.sol
├── hardhat.config.js
├── scripts/
│   ├── deploy.js
│   └── decode.js
└── proofs/
└── proof-0x....json # Example of a decoded attestation receipt


## 6. Security & Cost Considerations

- **Keys:** The ZetaChain testnet private key should only be stored as an environment variable and never committed to the repository.
- **LLM:** The model call has a capped `max_output_tokens` and a safe `temperature` setting to control cost and output predictability.
- **Costs:** BigQuery scans are partitioned by day to limit cost. The stablecoin filter significantly reduces the volume of data processed.

## 7. Roadmap

- [ ] **Multi-Chain Coverage:** Add support for chains like Arbitrum, Optimism, BSC, and Polygon.
- [ ] **Non-Stable Asset Support:** Integrate with hourly token price feeds to analyze non-stable assets.
- [ ] **Real-time Alerts:** Implement near-real-time anomaly detection using Cloud Functions and Pub/Sub.
- [ ] **Bot Integration:** Create a Telegram/Discord bot for daily briefing notifications.
- [ ] **Dashboard:** Build a Looker Studio dashboard for interactive data exploration with `chain → bridge → token` drill-down functionality.

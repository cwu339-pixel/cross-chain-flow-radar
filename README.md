# Cross-Chain Flow Radar (ETH-only MVP)

Daily ETL → per chain/bridge/token NetFlow (USD) → anomaly explanation with Gemini → **on-chain attestation on ZetaChain (Testnet)**.

- **Cloud**: Google Cloud (BigQuery + Cloud Run + Cloud Scheduler)
- **LLM**: Vertex AI — `gemini-1.5-flash-002` (us-central1)
- **Chain focus**: Ethereum (stablecoin 1:1 USD); extendable to more chains
- **On-chain**: ZetaChain Athens-3 (chainId 7001), emitting a proof event

---

## 1) Architecture

1. **ETL (BigQuery Scheduled Query)**
   - Extracts token transfers where `to` or `from` equals a known **bridge address**.
   - Filters **stablecoins** (USDT/USDC/DAI/TUSD/USDP/FRAX/USDe) → treated **1:1 USD**.
   - Aggregates to **day × chain × bridge × token**:
     - `in_amount_usd`: to bridge  
     - `out_amount_usd`: from bridge  
     - `net_amount_usd = in - out`  
     - `tx_count`, `unique_wallets` (counterparty dedup)
   - Writes to:
     - `xchain_radar.flows_data`
     - `xchain_radar.flows_daily` (same grain for MVP)

2. **Briefing Service (Cloud Run)**
   - Endpoint: `GET /explain?day=YYYY-MM-DD&chain=ethereum`
   - Logic:
     - If a bridge anomaly view exists (optional), use it as a filter.
     - Else compute a **contrast evidence** (today vs yesterday vs 7d avg).
     - Call Vertex AI to produce an **English briefing**:
       - **No anomaly** → explain deltas & context.
       - **Anomaly** → list bridge×token highlights & hypotheses.
   - Upserts into `xchain_radar.daily_briefings (day, model, summary_text, source_rows_json)`.

3. **On-chain Attestation (ZetaChain)**
   - Contract: `RadarBriefingRegistry.sol` (Athens-3)
   - After generating the briefing, an **off-chain script** (`attest/attest.py`) publishes
     - `event BriefingPublished(string day,string chain,bytes32 summaryHash,bool hasAnomaly,uint256 evidenceRows,string model,string uri)`
   - Decoding script (`zetachain/scripts/decode.js`) prints a human-readable proof.

---

## 2) Cloud & Data

- **Project**: `faefw-468503`
- **Dataset**: `xchain_radar` (Location=`US`, Timezone=`Asia/Tokyo`)
- **Tables**
  - `flows_data` — day×chain×bridge×token aggregated metrics
  - `flows_daily` — same grain (MVP)
  - `daily_briefings` — LLM output & evidence snapshot (JSON)

**Address Book**
- `address_book_bridges_v2(chain, bridge, bridge_address, notes)`
- Seeds (Ethereum): Stargate, Across, cBridge (see SQL).

---

## 3) Reproduce (minimal)

### 3.1 ETL (BigQuery Scheduled Query)
- File: `sql/etl_yesterday_eth.sql`  
  Runs at **07:55 Asia/Tokyo** daily, writes ETH results for **yesterday**.

### 3.2 Briefing Service (Cloud Run)
- Service: `xchain-briefing` (us-central1, Python 3.11)
- Requirements: `cloudrun/requirements.txt`
- Endpoint:
  ```bash
  # needs Cloud Run Identity Token as Bearer
  curl --http1.1 -H "Authorization: Bearer <TOKEN>" \
    "<RUN_URL>?day=YYYY-MM-DD&chain=ethereum"

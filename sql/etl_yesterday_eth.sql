-- =========================
-- ETH YESTERDAY ETL (Asia/Tokyo window)
-- Writes into: faefw-468503.xchain_radar.flows_data / flows_daily
-- =========================

DECLARE project_id STRING DEFAULT 'faefw-468503';
DECLARE dataset_id STRING DEFAULT 'xchain_radar';
DECLARE d DATE DEFAULT DATE_SUB(CURRENT_DATE("Asia/Tokyo"), INTERVAL 1 DAY);

-- 0) Address book (bridges) ensure + seed (ETH only)
EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.%s.address_book_bridges_v2`(
  chain STRING, bridge STRING, bridge_address STRING, notes STRING
)
""", project_id, dataset_id);

MERGE `%s.%s.address_book_bridges_v2` T
USING (
  SELECT LOWER(chain) AS chain, LOWER(bridge) AS bridge,
         LOWER(bridge_address) AS bridge_address, notes
  FROM UNNEST([
    STRUCT('ethereum' AS chain,'stargate' AS bridge,'0xdf0770df86a8034b3efef0a1bb3c889b8332ff56' AS bridge_address,'seed' AS notes),
    STRUCT('ethereum','stargate','0x38ea452219524bb87e18de1c24d3bb59510bd783','seed'),
    STRUCT('ethereum','across',  '0x5c7bcd6e7de5423a257d81b442095a1a6ced35c5','seed'),
    STRUCT('ethereum','cbridge', '0x5427fefa711eff984124bfbb1ab6fbf5e3da1820','seed')
  ])
) S
ON  T.chain = S.chain AND T.bridge = S.bridge AND T.bridge_address = S.bridge_address
WHEN NOT MATCHED THEN
  INSERT (chain, bridge, bridge_address, notes)
  VALUES (S.chain, S.bridge, S.bridge_address, S.notes);

-- 1) Ensure target tables
EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.%s.flows_data` (
  day DATE,
  chain STRING,
  bridge STRING,
  token_symbol STRING,
  in_amount_usd BIGNUMERIC,
  out_amount_usd BIGNUMERIC,
  net_amount_usd BIGNUMERIC,
  tx_count INT64,
  unique_wallets INT64
)
PARTITION BY day
CLUSTER BY chain, bridge, token_symbol
""", project_id, dataset_id);

EXECUTE IMMEDIATE FORMAT("""
CREATE TABLE IF NOT EXISTS `%s.%s.flows_daily` (
  day DATE,
  chain STRING,
  bridge STRING,
  token_symbol STRING,
  in_amount_usd BIGNUMERIC,
  out_amount_usd BIGNUMERIC,
  net_amount_usd BIGNUMERIC,
  tx_count INT64,
  unique_wallets INT64
)
PARTITION BY day
CLUSTER BY chain, bridge, token_symbol
""", project_id, dataset_id);

-- 2) Time window (Tokyo → UTC)
DECLARE ts_start TIMESTAMP DEFAULT TIMESTAMP(d, "Asia/Tokyo");
DECLARE ts_end   TIMESTAMP DEFAULT TIMESTAMP(DATE_ADD(d, INTERVAL 1 DAY), "Asia/Tokyo");

-- 3) Compute & upsert
WITH
book AS (
  SELECT chain, bridge, bridge_address
  FROM `%s.%s.address_book_bridges_v2`
  WHERE chain = 'ethereum'
),
stable AS (
  SELECT 'USDT' AS sym UNION ALL
  SELECT 'USDC' UNION ALL
  SELECT 'DAI'  UNION ALL
  SELECT 'TUSD' UNION ALL
  SELECT 'USDP' UNION ALL
  SELECT 'FRAX' UNION ALL
  SELECT 'USDE'
),
raw AS (
  SELECT
    d                           AS day,
    'ethereum'                  AS chain,
    LOWER(COALESCE(b.bridge,'unknown')) AS bridge,
    UPPER(tk.symbol)            AS token_symbol,
    tr.token_address,
    tr.from_address             AS from_addr,
    tr.to_address               AS to_addr,
    tr.block_timestamp,
    -- float amount (safe for intermediate), later cast to BIGNUMERIC
    CAST(tr.value AS FLOAT64) / POW(10, CAST(tk.decimals AS INT64)) AS amt_token_f,
    -- stablecoins 1:1 USD
    CASE WHEN UPPER(tk.symbol) IN (SELECT sym FROM stable)
         THEN CAST(tr.value AS FLOAT64) / POW(10, CAST(tk.decimals AS INT64))
         ELSE 0 END AS amt_usd_f,
    (LOWER(tr.to_address)   = b.bridge_address)  AS to_is_bridge,
    (LOWER(tr.from_address) = b.bridge_address)  AS from_is_bridge
  FROM `bigquery-public-data.crypto_ethereum.token_transfers` tr
  JOIN `bigquery-public-data.crypto_ethereum.tokens` tk
    ON LOWER(tk.address) = LOWER(tr.token_address)
  JOIN book b
    ON LOWER(tr.to_address)   = b.bridge_address
    OR LOWER(tr.from_address) = b.bridge_address
  WHERE tr.block_timestamp >= ts_start
    AND tr.block_timestamp <  ts_end
    AND UPPER(tk.symbol) IN (SELECT sym FROM stable)
),
agg AS (
  SELECT
    day, chain, bridge, token_symbol,
    SUM(CASE WHEN to_is_bridge   THEN amt_usd_f ELSE 0 END) AS in_usd_f,
    SUM(CASE WHEN from_is_bridge THEN amt_usd_f ELSE 0 END) AS out_usd_f,
    COUNT(*) AS tx_count,
    COUNT(DISTINCT CASE
      WHEN to_is_bridge   THEN LOWER(from_addr)
      WHEN from_is_bridge THEN LOWER(to_addr)
      ELSE NULL END) AS unique_wallets
  FROM raw
  GROUP BY day, chain, bridge, token_symbol
),
ins AS (
  SELECT
    day, chain, bridge, token_symbol,
    CAST(in_usd_f  AS BIGNUMERIC) AS in_amount_usd,
    CAST(out_usd_f AS BIGNUMERIC) AS out_amount_usd,
    CAST(in_usd_f - out_usd_f AS BIGNUMERIC) AS net_amount_usd,
    tx_count, unique_wallets
  FROM agg
)

-- Upsert flows_data
MERGE `%s.%s.flows_data` T
USING ins S
ON  T.day=S.day AND T.chain=S.chain AND T.bridge=S.bridge AND T.token_symbol=S.token_symbol
WHEN MATCHED THEN UPDATE SET
  in_amount_usd  = S.in_amount_usd,
  out_amount_usd = S.out_amount_usd,
  net_amount_usd = S.net_amount_usd,
  tx_count       = S.tx_count,
  unique_wallets = S.unique_wallets
WHEN NOT MATCHED THEN
  INSERT (day, chain, bridge, token_symbol, in_amount_usd, out_amount_usd, net_amount_usd, tx_count, unique_wallets)
  VALUES (S.day, S.chain, S.bridge, S.token_symbol, S.in_amount_usd, S.out_amount_usd, S.net_amount_usd, S.tx_count, S.unique_wallets);

-- Upsert flows_daily (same grain as flows_data for MVP)
MERGE `%s.%s.flows_daily` T
USING ins S
ON  T.day=S.day AND T.chain=S.chain AND T.bridge=S.bridge AND T.token_symbol=S.token_symbol
WHEN MATCHED THEN UPDATE SET
  in_amount_usd  = S.in_amount_usd,
  out_amount_usd = S.out_amount_usd,
  net_amount_usd = S.net_amount_usd,
  tx_count       = S.tx_count,
  unique_wallets = S.unique_wallets
WHEN NOT MATCHED THEN
  INSERT (day, chain, bridge, token_symbol, in_amount_usd, out_amount_usd, net_amount_usd, tx_count, unique_wallets)
  VALUES (S.day, S.chain, S.bridge, S.token_symbol, S.in_amount_usd, S.out_amount_usd, S.net_amount_usd, S.tx_count, S.unique_wallets);

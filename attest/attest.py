import os, json, argparse, datetime as dt
from zoneinfo import ZoneInfo
from web3 import Web3
from google.cloud import bigquery

ABI = [
  {"inputs":[
    {"internalType":"string","name":"day","type":"string"},
    {"internalType":"string","name":"chain","type":"string"},
    {"internalType":"bytes32","name":"summaryHash","type":"bytes32"},
    {"internalType":"bool","name":"hasAnomaly","type":"bool"},
    {"internalType":"uint256","name":"evidenceRows","type":"uint256"},
    {"internalType":"string","name":"model","type":"string"},
    {"internalType":"string","name":"uri","type":"string"}],
   "name":"publish","outputs":[],"stateMutability":"nonpayable","type":"function"}
]

def keccak32(txt:str)->bytes:
    return Web3.keccak(text=(txt or ""))

def decide_has_anomaly(summary:str)->bool:
    if not summary: return False
    s = summary.lower()
    # 简单启发式：英文/中文关键字
    bad = ["anomal", "abnormal", "irregular", "异常", "显著"]
    good = ["no anomaly", "no significant", "未发现显著异常"]
    if any(g in s for g in good): return False
    if any(b in s for b in bad):  return True
    return False

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", default="ethereum")
    ap.add_argument("--day")                       # 单日
    ap.add_argument("--start")                     # 起止（二选一）
    ap.add_argument("--end")
    ap.add_argument("--tz", default="Asia/Tokyo")  # 和你项目口径一致
    ap.add_argument("--dry", action="store_true")
    args = ap.parse_args()

    tz = ZoneInfo(args.tz)
    if not args.day and not (args.start and args.end):
        # 默认昨天（Tokyo）
        args.day = (dt.datetime.now(tz).date() - dt.timedelta(days=1)).isoformat()

    project = os.getenv("GOOGLE_CLOUD_PROJECT", "faefw-468503")
    dataset = os.getenv("XCHAIN_DATASET", "xchain_radar")
    table   = f"{project}.{dataset}.daily_briefings"

    # BigQuery 读取简报
    bq = bigquery.Client(project=project)
    if args.day:
        sql = f"""SELECT day, model, summary_text, source_rows_json
                  FROM `{table}` WHERE day = DATE(@d)"""
        params = [bigquery.ScalarQueryParameter("d","STRING",args.day)]
    else:
        sql = f"""SELECT day, model, summary_text, source_rows_json
                  FROM `{table}` WHERE day BETWEEN DATE(@s) AND DATE(@e)
                  ORDER BY day"""
        params = [bigquery.ScalarQueryParameter("s","STRING",args.start),
                  bigquery.ScalarQueryParameter("e","STRING",args.end)]
    rows = list(bq.query(sql, job_config=bigquery.QueryJobConfig(
                query_parameters=params), location=os.getenv("BQ_LOCATION","US")).result())

    if not rows:
        print("No rows from daily_briefings for given date(s)."); return

    # Web3 合约
    rpc     = os.getenv("ZETA_RPC","https://zetachain-athens-evm.blockpi.network/v1/rpc/public")
    pk      = os.getenv("ZETA_PRIVATE_KEY") or os.getenv("PRIVATE_KEY")
    cid     = int(os.getenv("ZETA_CHAIN_ID","7001"))
    addr    = os.getenv("ZETA_CONTRACT")  # 你刚部署的合约地址
    assert pk and addr, "ZETA_PRIVATE_KEY/PRIVATE_KEY and ZETA_CONTRACT are required"

    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30}))
    acct = w3.eth.account.from_key(pk)
    contract = w3.eth.contract(address=Web3.to_checksum_address(addr), abi=ABI)

    nonce = w3.eth.get_transaction_count(acct.address)
    gas_price = w3.eth.gas_price

    for r in rows:
        day = r["day"].isoformat()
        model = r.get("model","")
        summary = r.get("summary_text","") or ""
        src_json = r.get("source_rows_json") or "[]"
        try:
            ev_rows = len(json.loads(src_json))
        except Exception:
            ev_rows = 0

        has_anom = decide_has_anomaly(summary)
        h = keccak32(summary)

        # 可选：把 daily_briefings 的网页链接当 uri（或留空）
        uri = ""

        tx = contract.functions.publish(day, args.chain, h, has_anom, ev_rows, model, uri)\
            .build_transaction({
                "from": acct.address,
                "nonce": nonce,
                "chainId": cid,
                "gas": 200000,
                "gasPrice": gas_price
            })

        if args.dry:
            print(f"[DRY] {day} → hash={h.hex()} rows={ev_rows} anom={has_anom}"); 
            nonce += 1
            continue

        signed = acct.sign_transaction(tx)
        txh = w3.eth.send_raw_transaction(signed.raw_transaction)
        print(f"[OK] {day} tx={txh.hex()}")
        nonce += 1

if __name__ == "__main__":
    main()

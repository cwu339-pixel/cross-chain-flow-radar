import os, json, logging, requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from google.cloud import bigquery
import vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig

# ===== Config =====
logging.basicConfig(level=logging.INFO)

PROJECT         = os.environ.get("GOOGLE_CLOUD_PROJECT", "faefw-468503")
DATASET         = os.getenv("XCHAIN_DATASET", "xchain_radar")
BQ_LOCATION     = os.getenv("BQ_LOCATION", "US")

MODEL           = os.getenv("GENAI_MODEL", "gemini-1.5-flash-002")
GENAI_LOCATION  = os.getenv("GENAI_LOCATION", "us-central1")
TZ              = ZoneInfo(os.getenv("LOCAL_TZ", "Asia/Tokyo"))
SEND_ON_FALLBACK= os.getenv("SEND_ON_FALLBACK", "0") == "1"

FLOWS_TABLE     = f"{PROJECT}.{DATASET}.flows_daily"
ANOMS_VIEW      = f"{PROJECT}.{DATASET}.bridge_flows_anoms"   # optional view: day, chain, bridge, zscore_30d, net_usd, is_anom_bridge
BRIEFINGS_TABLE = f"{PROJECT}.{DATASET}.daily_briefings"

PROMPT_ANOMALY = (
    "You are a risk analyst for cross-chain monitoring. Based on the provided anomalous bridges and evidence, "
    "write a concise English 'Cross-Chain Flow Briefing'. Include:\n"
    "1) One-line conclusion: which chain/bridge shows a significant anomaly;\n"
    "2) Bullet points: bridge×token net flow, tx count, unique wallets;\n"
    "3) 2–3 cautious hypotheses (e.g., arbitrage, asset migration, bridge rebalancing);\n"
    "4) Action items. Tone: factual, concise, 150–220 words."
)

PROMPT_NO_ANOMALY = (
    "You are a risk analyst for cross-chain monitoring. You are given chain-level totals and top bridge×token net flows "
    "with comparisons (today vs yesterday vs 7d avg). Produce an English 'No Significant Anomaly' note:\n"
    "1) Chain-level: in/out/net vs yesterday and 7d avg (direction, magnitude, typical range);\n"
    "2) Top 2–3 bridge×token items: net flow, share of chain net, diffs vs yesterday/7d avg;\n"
    "3) Why this is 'no anomaly' (e.g., within historical band, routine settlement/routing);\n"
    "4) One actionable note. Tone: factual, 150–220 words."
)

# ===== Utils =====
def _yesterday_local_iso() -> str:
    return (datetime.now(TZ).date() - timedelta(days=1)).isoformat()

def _get_param(request, name: str, default: str | None = None) -> str | None:
    v = request.args.get(name)
    if v: return v
    if request.is_json:
        try:
            body = request.get_json(silent=True) or {}
            return body.get(name, default)
        except Exception:
            return default
    return default

def _bq() -> bigquery.Client:
    return bigquery.Client(project=PROJECT)

# ===== BigQuery helpers =====
def _fetch_anom_bridges(bq: bigquery.Client, day_iso: str, chain: str) -> list[str]:
    """Optional: if ANOMS_VIEW exists, pick anomalous bridges; otherwise return []."""
    try:
        sql = f"""
        SELECT bridge
        FROM `{ANOMS_VIEW}`
        WHERE day = DATE(@d) AND chain = @c AND is_anom_bridge
        ORDER BY ABS(zscore_30d) DESC, ABS(net_usd) DESC
        LIMIT 12
        """
        job = bq.query(sql,
                       bigquery.QueryJobConfig(query_parameters=[
                           bigquery.ScalarQueryParameter("d", "STRING", day_iso),
                           bigquery.ScalarQueryParameter("c", "STRING", chain),
                       ]),
                       location=BQ_LOCATION)
        return [r["bridge"] for r in job.result()]
    except Exception:
        # View not present → treat as no anomaly filter
        return []

def _fetch_bridge_evidence(bq: bigquery.Client, day_iso: str, chain: str,
                           bridges: list[str] | None, limit: int = 200) -> list[dict]:
    filter_clause = "AND bridge IN UNNEST(@bridges)" if bridges else ""
    sql = f"""
    SELECT
      @c AS chain, DATE(@d) AS day, bridge, token_symbol,
      ROUND(SUM(in_amount_usd),  2) AS in_usd,
      ROUND(SUM(out_amount_usd), 2) AS out_usd,
      ROUND(SUM(net_amount_usd), 2) AS net_usd,
      SUM(tx_count)                 AS tx_count,
      SUM(unique_wallets)           AS unique_wallets
    FROM `{FLOWS_TABLE}`
    WHERE day = DATE(@d) AND chain = @c {filter_clause}
    GROUP BY bridge, token_symbol
    ORDER BY ABS(net_usd) DESC
    LIMIT {limit}
    """
    params = [
        bigquery.ScalarQueryParameter("d", "STRING", day_iso),
        bigquery.ScalarQueryParameter("c", "STRING", chain),
    ]
    if bridges:
        params.append(bigquery.ArrayQueryParameter("bridges", "STRING", bridges))
    job = bq.query(sql, bigquery.QueryJobConfig(query_parameters=params), location=BQ_LOCATION)
    return [dict(r) for r in job.result()]

def _fetch_contrast_rows(bq: bigquery.Client, day_iso: str, chain: str) -> list[dict]:
    """Return one _CHAIN row + several _BRIDGE rows with DoD / vs7d comparisons."""
    sql = f"""
    DECLARE d DATE   DEFAULT DATE(@d);
    DECLARE c STRING DEFAULT @c;

    WITH chain_today AS (
      SELECT
        CAST(SUM(in_amount_usd)  AS FLOAT64) AS in_usd_d,
        CAST(SUM(out_amount_usd) AS FLOAT64) AS out_usd_d,
        CAST(SUM(net_amount_usd) AS FLOAT64) AS net_usd_d,
        SUM(tx_count)                         AS txs_d,
        SUM(unique_wallets)                   AS uw_d
      FROM `{FLOWS_TABLE}` WHERE day=d AND chain=c
    ),
    chain_prev1 AS (
      SELECT
        CAST(SUM(in_amount_usd)  AS FLOAT64) AS in_usd_p1,
        CAST(SUM(out_amount_usd) AS FLOAT64) AS out_usd_p1,
        CAST(SUM(net_amount_usd) AS FLOAT64) AS net_usd_p1,
        SUM(tx_count)                         AS txs_p1,
        SUM(unique_wallets)                   AS uw_p1
      FROM `{FLOWS_TABLE}` WHERE day=DATE_SUB(d, INTERVAL 1 DAY) AND chain=c
    ),
    chain_7d AS (
      SELECT
        AVG(in_usd)  AS in_usd_7avg,
        AVG(out_usd) AS out_usd_7avg,
        AVG(net_usd) AS net_usd_7avg,
        AVG(txs)     AS txs_7avg,
        AVG(uw)      AS uw_7avg
      FROM (
        SELECT day,
               CAST(SUM(in_amount_usd)  AS FLOAT64) AS in_usd,
               CAST(SUM(out_amount_usd) AS FLOAT64) AS out_usd,
               CAST(SUM(net_amount_usd) AS FLOAT64) AS net_usd,
               SUM(tx_count)                         AS txs,
               SUM(unique_wallets)                   AS uw
        FROM `{FLOWS_TABLE}`
        WHERE chain=c
          AND day BETWEEN DATE_SUB(d, INTERVAL 7 DAY) AND DATE_SUB(d, INTERVAL 1 DAY)
        GROUP BY day
      )
    ),
    chain_ctx AS (
      SELECT
        '_CHAIN'  AS level,
        '_TOTAL'  AS bridge,
        '_ALL'    AS token_symbol,
        d         AS day,
        c         AS chain,
        t.in_usd_d,  t.out_usd_d,  t.net_usd_d,
        t.txs_d,     t.uw_d,
        p.in_usd_p1, p.out_usd_p1, p.net_usd_p1,
        p.txs_p1,    p.uw_p1,
        s.in_usd_7avg, s.out_usd_7avg, s.net_usd_7avg,
        s.txs_7avg,    s.uw_7avg,
        SAFE_DIVIDE(t.net_usd_d - p.net_usd_p1, NULLIF(p.net_usd_p1,0))    AS net_dod_pct,
        SAFE_DIVIDE(t.net_usd_d - s.net_usd_7avg, NULLIF(s.net_usd_7avg,0)) AS net_vs7d_pct,
        CAST(NULL AS FLOAT64) AS net_share_of_chain
      FROM chain_today t
      LEFT JOIN chain_prev1 p ON TRUE
      LEFT JOIN chain_7d   s  ON TRUE
    ),
    bridge_today AS (
      SELECT
        d AS day, c AS chain, bridge, token_symbol,
        CAST(SUM(in_amount_usd)  AS FLOAT64) AS in_usd_d,
        CAST(SUM(out_amount_usd) AS FLOAT64) AS out_usd_d,
        CAST(SUM(net_amount_usd) AS FLOAT64) AS net_usd_d,
        SUM(tx_count)                         AS txs_d,
        SUM(unique_wallets)                   AS uw_d
      FROM `{FLOWS_TABLE}` WHERE day=d AND chain=c
      GROUP BY bridge, token_symbol
    ),
    bridge_prev1 AS (
      SELECT
        bridge, token_symbol,
        CAST(SUM(in_amount_usd)  AS FLOAT64) AS in_usd_p1,
        CAST(SUM(out_amount_usd) AS FLOAT64) AS out_usd_p1,
        CAST(SUM(net_amount_usd) AS FLOAT64) AS net_usd_p1
      FROM `{FLOWS_TABLE}` WHERE day=DATE_SUB(d, INTERVAL 1 DAY) AND chain=c
      GROUP BY bridge, token_symbol
    ),
    bridge_7d AS (
      SELECT
        bridge, token_symbol,
        AVG(in_usd)  AS in_usd_7avg,
        AVG(out_usd) AS out_usd_7avg,
        AVG(net_usd) AS net_usd_7avg
      FROM (
        SELECT bridge, token_symbol,
               CAST(SUM(in_amount_usd)  AS FLOAT64) AS in_usd,
               CAST(SUM(out_amount_usd) AS FLOAT64) AS out_usd,
               CAST(SUM(net_amount_usd) AS FLOAT64) AS net_usd
        FROM `{FLOWS_TABLE}`
        WHERE chain=c
          AND day BETWEEN DATE_SUB(d, INTERVAL 7 DAY) AND DATE_SUB(d, INTERVAL 1 DAY)
        GROUP BY bridge, token_symbol
      )
      GROUP BY bridge, token_symbol
    ),
    chain_total_net AS (SELECT SUM(net_usd_d) AS chain_net_d FROM bridge_today),
    bridge_ctx AS (
      SELECT
        '_BRIDGE'           AS level,
        bt.bridge, bt.token_symbol,
        bt.day, bt.chain,
        bt.in_usd_d,  bt.out_usd_d,  bt.net_usd_d,
        bt.txs_d,     bt.uw_d,
        bp.in_usd_p1, bp.out_usd_p1, bp.net_usd_p1,
        CAST(NULL AS INT64)    AS txs_p1,
        CAST(NULL AS INT64)    AS uw_p1,
        b7.in_usd_7avg, b7.out_usd_7avg, b7.net_usd_7avg,
        CAST(NULL AS FLOAT64)  AS txs_7avg,
        CAST(NULL AS FLOAT64)  AS uw_7avg,
        SAFE_DIVIDE(bt.net_usd_d - bp.net_usd_p1, NULLIF(bp.net_usd_p1,0))    AS net_dod_pct,
        SAFE_DIVIDE(bt.net_usd_d - b7.net_usd_7avg, NULLIF(b7.net_usd_7avg,0)) AS net_vs7d_pct,
        SAFE_DIVIDE(bt.net_usd_d, NULLIF(ct.chain_net_d,0))                    AS net_share_of_chain
      FROM bridge_today bt
      LEFT JOIN bridge_prev1 bp USING(bridge, token_symbol)
      LEFT JOIN bridge_7d   b7 USING(bridge, token_symbol)
      LEFT JOIN chain_total_net ct ON TRUE
    )
    SELECT level, bridge, token_symbol, day, chain,
           in_usd_d, out_usd_d, net_usd_d, txs_d, uw_d,
           in_usd_p1, out_usd_p1, net_usd_p1, txs_p1, uw_p1,
           in_usd_7avg, out_usd_7avg, net_usd_7avg, txs_7avg, uw_7avg,
           net_dod_pct, net_vs7d_pct, net_share_of_chain
    FROM (
      SELECT * FROM chain_ctx
      UNION ALL
      SELECT * FROM bridge_ctx
    )
    ORDER BY CASE WHEN level='_CHAIN' THEN 0 ELSE 1 END,
             ABS(COALESCE(net_usd_d,0)) DESC
    """
    job = bq.query(sql,
                   bigquery.QueryJobConfig(query_parameters=[
                       bigquery.ScalarQueryParameter("d", "STRING", day_iso),
                       bigquery.ScalarQueryParameter("c", "STRING", chain),
                   ]),
                   location=BQ_LOCATION)
    return [dict(r) for r in job.result()]

def _merge_briefing(bq: bigquery.Client, day_iso: str, text: str, rows_or_json):
    """Upsert into daily_briefings (stores summary and evidence snapshot)."""
    if isinstance(rows_or_json, str):
        src = rows_or_json[:900000]
    else:
        src = json.dumps(rows_or_json, ensure_ascii=False, default=str)[:900000]
    sql = f"""
    MERGE `{BRIEFINGS_TABLE}` T
    USING (
      SELECT DATE(@day) AS day,
             @model   AS model,
             @summary AS summary_text,
             @src     AS source_rows_json,
             CURRENT_TIMESTAMP() AS created_at
    ) S
    ON T.day = S.day
    WHEN MATCHED THEN
      UPDATE SET
        model            = S.model,
        summary_text     = S.summary_text,
        source_rows_json = S.source_rows_json,
        created_at       = S.created_at
    WHEN NOT MATCHED THEN
      INSERT (day, model, summary_text, source_rows_json, created_at)
      VALUES (S.day, S.model, S.summary_text, S.source_rows_json, S.created_at);
    """
    job = bq.query(sql,
                   bigquery.QueryJobConfig(query_parameters=[
                       bigquery.ScalarQueryParameter("day", "STRING", day_iso),
                       bigquery.ScalarQueryParameter("model", "STRING", MODEL),
                       bigquery.ScalarQueryParameter("summary", "STRING", text),
                       bigquery.ScalarQueryParameter("src", "STRING", src),
                   ]),
                   location=BQ_LOCATION)
    job.result()
    logging.info(f"[MERGE] wrote briefing for {day_iso}")

def _send_telegram(text: str):
    token = os.getenv("TG_TOKEN")
    chat  = os.getenv("TG_CHAT")
    if not token or not chat:
        return
    try:
        payload = {"chat_id": chat, "text": text[:3900],
                   "parse_mode": "HTML", "disable_web_page_preview": True}
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json=payload, timeout=10)
        r.raise_for_status()
    except Exception as e:
        logging.exception(f"[SEND] Telegram failed: {type(e).__name__}: {e}")

def _fallback_summary(day_iso: str, rows: list[dict], reason: str) -> str:
    head = f"[Cross-Chain Flow Briefing | {day_iso}]\nConclusion: {reason}."
    if rows:
        items = []
        for r in rows[:5]:
            items.append(
                f"- {r.get('chain')}/{r.get('bridge')} {r.get('token_symbol')}: "
                f"net≈{r.get('net_usd', r.get('net_amount_usd', 0))}, "
                f"tx={r.get('tx_count', 0)}, wallets={r.get('unique_wallets', 0)}"
            )
        body = "Top flows:\n" + "\n".join(items)
    else:
        body = "Top flows: none."
    tail = "Action: keep watching major bridges/tokens; set threshold alerts for large addresses."
    return "\n".join([head, body, tail])

# ===== Cloud Run entrypoint =====
def explain(request):
    """
    GET /?day=YYYY-MM-DD&chain=ethereum
    POST {"day":"...", "chain":"..."}
    """
    day   = _get_param(request, "day") or _yesterday_local_iso()
    chain = (_get_param(request, "chain") or "ethereum").lower()
    rev   = os.environ.get("K_REVISION", "unknown")
    logging.info(f"[INVOKE] day={day} chain={chain} rev={rev}")

    bq = _bq()

    # 1) Try anomalies first (if view exists)
    try:
        bridges = _fetch_anom_bridges(bq, day, chain)
    except Exception:
        logging.exception("fetch_anom_bridges failed")
        bridges = []

    if bridges:
        # 2) With anomalies: gather evidence and ask the model
        try:
            ev = _fetch_bridge_evidence(bq, day, chain, bridges, limit=200)
        except Exception:
            logging.exception("fetch_bridge_evidence failed")
            ev = []

        fallback = False
        reason   = ""
        try:
            vertexai.init(project=PROJECT, location=GENAI_LOCATION)
            model = GenerativeModel(MODEL)
            payload = {"date": day, "chain": chain, "anomaly_bridges": bridges[:12], "evidence": ev[:100]}
            resp = model.generate_content(
                [PROMPT_ANOMALY, json.dumps(payload, ensure_ascii=False, default=str)],
                generation_config=GenerationConfig(temperature=0.2, max_output_tokens=900),
            )
            text = (getattr(resp, "text", None) or "").strip()
            if not text:
                raise RuntimeError("model returned empty text")
        except Exception as e:
            logging.exception(f"Vertex call failed: {type(e).__name__}: {e}")
            text = _fallback_summary(day, ev, "Anomaly detected but model failed (fallback)")
            fallback = True
            reason   = "vertex_failed"

        try:
            _merge_briefing(bq, day, text, ev)
        except Exception:
            logging.exception("merge_briefing failed")

        try:
            if SEND_ON_FALLBACK or not fallback:
                _send_telegram(text)
        except Exception:
            logging.exception("notify failed")

        return (
            json.dumps({"ok": True, "day": day, "chain": chain, "rev": rev,
                        "wrote": True, "fallback": fallback, "reason": reason,
                        "rows": len(ev), "model": MODEL}),
            200, {"Content-Type": "application/json"}
        )

    # 3) No anomaly → build contrast evidence + model explanation
    try:
        contrast_rows = _fetch_contrast_rows(bq, day, chain)
    except Exception:
        logging.exception("fetch_contrast_rows failed")
        contrast_rows = []

    try:
        vertexai.init(project=PROJECT, location=GENAI_LOCATION)
        model = GenerativeModel(MODEL)
        payload = {"date": day, "chain": chain, "contrast": contrast_rows}
        resp = model.generate_content(
            [PROMPT_NO_ANOMALY, json.dumps(payload, ensure_ascii=False, default=str)],
            generation_config=GenerationConfig(temperature=0.2, max_output_tokens=900),
        )
        text = (getattr(resp, "text", None) or "").strip()
        if not text:
            raise RuntimeError("model returned empty text")
        fallback = False
        reason   = "no_anomaly"
    except Exception as e:
        logging.exception(f"Vertex call failed (no anomaly): {type(e).__name__}: {e}")
        try:
            tops = _fetch_bridge_evidence(bq, day, chain, bridges=None, limit=20)
        except Exception:
            tops = []
        text     = _fallback_summary(day, tops, "No significant anomaly")
        contrast_rows = contrast_rows or tops
        fallback = True
        reason   = "no_anomaly_fallback"

    try:
        _merge_briefing(bq, day, text, contrast_rows)
    except Exception:
        logging.exception("merge_briefing failed (no anomaly)")

    try:
        if SEND_ON_FALLBACK or not fallback:
            _send_telegram(text)
    except Exception:
        logging.exception("notify failed (no anomaly)")

    return (
        json.dumps({"ok": True, "day": day, "chain": chain, "rev": rev,
                    "wrote": True, "fallback": fallback, "reason": reason,
                    "rows": len(contrast_rows), "model": MODEL}),
        200, {"Content-Type": "application/json"}
    )

# -*- coding: utf-8 -*-
"""
app.py

Cloud Run version of dispatch_sync_keep_fields.py

- POST /run      -> execute sync
- GET  /health   -> health check
- Optional auth  -> set RUN_TOKEN env var, then client sends header: X-Run-Token

"""

from __future__ import annotations

import io
import os
import re
import sys
import json
import base64
import logging
import datetime as dt
from typing import Dict, Any, Optional, List, Iterable, Tuple

import numpy as np
import pandas as pd
import requests
from flask import Flask, jsonify, request
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pyodbc

import firebase_admin
from firebase_admin import credentials, db

# ===================== logging =====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("dispatch_sync")

app = Flask(__name__)

# ===================== config from env =====================
HANA_SERVERNODE = os.getenv("HANA_SERVERNODE", "")
HANA_UID = os.getenv("HANA_UID", "")
HANA_PWD = os.getenv("HANA_PWD", "")
SAP_CLIENT = os.getenv("SAP_CLIENT", "800")

ORDERLIST_PUBLIC_DL = os.getenv("ORDERLIST_PUBLIC_DL", "")
BP_PUBLIC_DL = os.getenv("BP_PUBLIC_DL", "")

USE_LOCAL_FILES = os.getenv("USE_LOCAL_FILES", "false").lower() == "true"
ORDERLIST_LOCAL_PATH = os.getenv("ORDERLIST_LOCAL_PATH", "/tmp/Planning Schedule 2025.xlsm")
BP_LOCAL_PATH = os.getenv("BP_LOCAL_PATH", "/tmp/BP Proposal.xlsx")

FIREBASE_DB_URL = os.getenv("FIREBASE_DB_URL", "")
FIREBASE_TARGET_ROOT = os.getenv("FIREBASE_TARGET_ROOT", "Dispatch")

# 推荐把 Firebase service account JSON 放在 Secret Manager / 环境变量
# 二选一：
# 1) FIREBASE_SA_JSON          -> 直接放完整 JSON 字符串
# 2) FIREBASE_SA_JSON_BASE64   -> 放 base64 后的 JSON
# 3) FIREBASE_SA_PATH          -> 本地文件路径（仅本地调试）
FIREBASE_SA_JSON = os.getenv("FIREBASE_SA_JSON")
FIREBASE_SA_JSON_BASE64 = os.getenv("FIREBASE_SA_JSON_BASE64")
FIREBASE_SA_PATH = os.getenv("FIREBASE_SA_PATH", "firebase-adminsdk.json")

RUN_TOKEN = os.getenv("RUN_TOKEN", "")
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"

DELETE_BATCH_SIZE = int(os.getenv("DELETE_BATCH_SIZE", "400"))
PATCH_BATCH_SIZE = int(os.getenv("PATCH_BATCH_SIZE", "900"))

DSN_TMPL = (
    "DRIVER={%s};"
    "SERVERNODE=%s;"
    "UID=%s;"
    "PWD=%s;"
)

HANA_ODBC_DRIVER = os.getenv("HANA_ODBC_DRIVER", "HDBODBC")
HANA_ODBC_DRIVER_FALLBACKS = [
    d.strip() for d in os.getenv("HANA_ODBC_DRIVER_FALLBACKS", "HDBODBC32").split(",") if d.strip()
]
HANA_DB_PROVIDER = os.getenv("HANA_DB_PROVIDER", "auto").strip().lower()  # auto | odbc | hdbcli

KEEP_FIELDS = {
    "EstimatedPickupAt",
    "TransportCompany",
    "OnHold", "OnHoldAt", "OnHoldBy",
    "InvalidStock", "InvalidStockAt", "InvalidStockBy",
    "TemporaryLeavingWithoutPGI",
}

# ===================== SQL =====================
BASELINE_SQL_0024_0026 = rf"""
SELECT
    a."VBELN",
    a."MATNR",
    a."KALAB",
    a."LGORT",
    b."BUDAT_MKPF_MAX",
    b."BUDAT_MKPF_MIN"
FROM "SAPHANADB"."NSDM_V_MSKA" a
LEFT JOIN (
    SELECT
        "KDAUF",
        "MATNR",
        "LGORT",
        MAX("BUDAT_MKPF") AS "BUDAT_MKPF_MAX",
        MIN("BUDAT_MKPF") AS "BUDAT_MKPF_MIN"
    FROM "SAPHANADB"."NSDM_V_MSEG"
    WHERE "MANDT" = '{SAP_CLIENT}'
      AND "WERKS" = 3111
      AND "KDPOS" = 10
    GROUP BY "KDAUF", "MATNR", "LGORT"
) b
    ON a."VBELN" = b."KDAUF"
   AND a."MATNR" = b."MATNR"
   AND a."LGORT" = b."LGORT"
WHERE
    a."MANDT" = '{SAP_CLIENT}'
    AND a."WERKS" = 3111
    AND a."MATNR" LIKE 'Z%'
    AND a."LGORT" IN ('0024','0026')
    AND a."KALAB" <> 0
"""

ENRICH_SQL = rf"""
WITH mseg_gr AS (
    SELECT
        "MANDT",
        "EBELN",
        "EBELP",
        MIN("BUDAT_MKPF") AS "GR_DATE"
    FROM "SAPHANADB"."NSDM_V_MSEG"
    WHERE
        "MANDT" = '{SAP_CLIENT}'
        AND "EBELN" IS NOT NULL
        AND "BWART" IN ('101','103')
    GROUP BY "MANDT","EBELN","EBELP"
),
ekpo_open_nogr AS (
    SELECT
        p."MANDT",
        p."EBELN",
        p."EBELP",
        p."CREATIONDATE",
        SUBSTRING(
            p."TXZ01",
            1,
            CASE
                WHEN INSTR(p."TXZ01", ' ') > 0 THEN INSTR(p."TXZ01", ' ') - 1
                ELSE LENGTH(p."TXZ01")
            END
        ) AS "SERNR_PREFIX"
    FROM "SAPHANADB"."EKPO" p
    JOIN "SAPHANADB"."EKKO" h
        ON h."MANDT" = p."MANDT"
       AND h."EBELN" = p."EBELN"
    LEFT JOIN mseg_gr gr
        ON gr."MANDT" = p."MANDT"
       AND gr."EBELN" = p."EBELN"
       AND gr."EBELP" = p."EBELP"
    WHERE
        p."MANDT" = '{SAP_CLIENT}'
        AND p."WERKS"='3111'
        AND p."MATKL"='Z003'
        AND LOWER(p."TXZ01") LIKE '% to %'
        AND COALESCE(p."LOEKZ",'') = ''
        AND COALESCE(h."LOEKZ",'') = ''
        AND COALESCE(p."ELIKZ",'') <> 'X'
        AND gr."EBELN" IS NULL
)
SELECT
    vbap."VBELN",
    vbap."KUNWE_ANA" AS "SHIPTO",
    objk."SERNR" AS "CHASSIS",
    ek."EBELN" AS "PO_NO",
    ek."CREATIONDATE" AS "PO_DATE"
FROM "SAPHANADB"."VBAP" vbap
LEFT JOIN "SAPHANADB"."SER02" s
    ON vbap."MANDT" = s."MANDT"
   AND vbap."VBELN" = s."SDAUFNR"
   AND s."POSNR" = 10
LEFT JOIN "SAPHANADB"."OBJK" objk
    ON s."MANDT" = objk."MANDT"
   AND s."OBKNR" = objk."OBKNR"
LEFT JOIN ekpo_open_nogr ek
    ON ek."MANDT" = vbap."MANDT"
   AND objk."SERNR" = ek."SERNR_PREFIX"
WHERE
    vbap."MANDT" = '{SAP_CLIENT}'
    AND vbap."POSNR" = 10
"""

VIN_SQL_TMPL = rf"""
SELECT DISTINCT
    obj."SERNR",
    a."SERNR2" AS "vin_number"
FROM "SAPHANADB"."SER02" s
JOIN "SAPHANADB"."OBJK" obj
     ON s."MANDT" = obj."MANDT"
    AND s."OBKNR" = obj."OBKNR"
LEFT JOIN (
    SELECT DISTINCT "MANDT","SERNR","SERNR2"
    FROM "SAPHANADB"."ZTSD002"
    WHERE "MANDT" = '{SAP_CLIENT}'
      AND "WERKS" = '3091'
) a
    ON a."MANDT" = obj."MANDT"
   AND a."SERNR" = obj."SERNR"
WHERE
    s."MANDT" = '{SAP_CLIENT}'
    AND s."POSNR" = '000010'
    AND obj."SERNR" IN ({{in_list}})
"""

# ===================== helpers =====================
def validate_required_env():
    required = {
        "HANA_SERVERNODE": HANA_SERVERNODE,
        "HANA_UID": HANA_UID,
        "HANA_PWD": HANA_PWD,
        "ORDERLIST_PUBLIC_DL": ORDERLIST_PUBLIC_DL if not USE_LOCAL_FILES else "LOCAL",
        "BP_PUBLIC_DL": BP_PUBLIC_DL if not USE_LOCAL_FILES else "LOCAL",
        "FIREBASE_DB_URL": FIREBASE_DB_URL,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")

def hana_query(sql: str) -> pd.DataFrame:
    provider = HANA_DB_PROVIDER
    if provider not in {"auto", "odbc", "hdbcli"}:
        raise RuntimeError("Invalid HANA_DB_PROVIDER. Use one of: auto, odbc, hdbcli")

    if provider in {"auto", "odbc"}:
        try:
            return hana_query_via_odbc(sql)
        except Exception as e:
            if provider == "odbc":
                raise
            log.warning("ODBC query failed, trying hdbcli fallback: %s", e)

    return hana_query_via_hdbcli(sql)


def hana_query_via_odbc(sql: str) -> pd.DataFrame:
    with pyodbc.connect(build_hana_dsn(), autocommit=True) as conn:
        return pd.read_sql(sql, conn)


def parse_hana_servernode(servernode: str) -> Tuple[str, int]:
    raw = (servernode or "").strip()
    if not raw:
        raise RuntimeError("HANA_SERVERNODE is required")

    if ":" in raw:
        host, port = raw.rsplit(":", 1)
        host = host.strip()
        try:
            return host, int(port)
        except ValueError as ex:
            raise RuntimeError(f"Invalid HANA_SERVERNODE port: {servernode}") from ex

    return raw, 30015


def hana_query_via_hdbcli(sql: str) -> pd.DataFrame:
    try:
        from hdbcli import dbapi
    except Exception as ex:
        raise RuntimeError(
            "hdbcli is not installed. Add `hdbcli` to requirements or set HANA_DB_PROVIDER=odbc with a working HANA ODBC driver."
        ) from ex

    host, port = parse_hana_servernode(HANA_SERVERNODE)
    conn = dbapi.connect(address=host, port=port, user=HANA_UID, password=HANA_PWD)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def resolve_hana_driver() -> str:
    available = set(pyodbc.drivers())
    candidates = [HANA_ODBC_DRIVER, *HANA_ODBC_DRIVER_FALLBACKS]

    for driver in candidates:
        if driver in available:
            if driver != HANA_ODBC_DRIVER:
                log.warning(
                    "Preferred ODBC driver '%s' not found. Using fallback '%s'.",
                    HANA_ODBC_DRIVER,
                    driver,
                )
            return driver

    if available:
        raise RuntimeError(
            "No SAP HANA ODBC driver found. "
            f"Tried: {', '.join(candidates)}. "
            f"Available ODBC drivers: {', '.join(sorted(available))}. "
            "Install SAP HANA client (HDBODBC/HDBODBC32) or set HANA_ODBC_DRIVER."
        )

    raise RuntimeError(
        "No ODBC drivers are registered in this runtime. "
        "Install unixODBC driver packages and SAP HANA client, then redeploy."
    )


def build_hana_dsn() -> str:
    return DSN_TMPL % (resolve_hana_driver(), HANA_SERVERNODE, HANA_UID, HANA_PWD)

def http_get_with_retry(url: str, timeout: int = 60) -> bytes:
    sess = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    sess.mount("https://", HTTPAdapter(max_retries=retry))
    headers = {"User-Agent": "Mozilla/5.0"}
    u = url.replace(" ", "%20")
    candidates = [u] if "download=1" in u else [u, (u + ("&" if "?" in u else "?") + "download=1")]
    last_err: Optional[Exception] = None
    for cand in candidates:
        try:
            resp = sess.get(cand, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            last_err = e
            log.warning("Download failed: %s -> %s", cand, e)
    raise last_err if last_err else RuntimeError("Download failed")

def looks_like_excel_zip(b: bytes) -> bool:
    return len(b) > 4 and b[:2] == b"PK" and (b.find(b"xl/") != -1)

def fetch_excel_bytes(public_dl: str) -> bytes:
    content = http_get_with_retry(public_dl)
    if not looks_like_excel_zip(content):
        raise ValueError("Downloaded content is not an Excel file. Check SharePoint link.")
    return content

def read_local_bytes(path: str) -> bytes:
    with open(path, "rb") as f:
        return f.read()

def read_excel_bytes_select_sheet(xls_bytes: bytes, preferred_name: str) -> pd.DataFrame:
    xfile = pd.ExcelFile(io.BytesIO(xls_bytes), engine="openpyxl")
    sheets = xfile.sheet_names
    if preferred_name in sheets:
        return pd.read_excel(xfile, sheet_name=preferred_name)

    def norm(ss: str) -> str:
        return re.sub(r"\s+", "", ss or "", flags=re.UNICODE).lower()

    target = norm(preferred_name)
    for sname in sheets:
        if norm(sname) == target:
            return pd.read_excel(xfile, sheet_name=sname)
    for sname in sheets:
        if target in norm(sname):
            return pd.read_excel(xfile, sheet_name=sname)
    return pd.read_excel(xfile, sheet_name=sheets[0])

def parse_to_date(x) -> Optional[dt.date]:
    if pd.isna(x):
        return None
    if isinstance(x, dt.datetime):
        return x.date()
    if isinstance(x, dt.date):
        return x
    ts = pd.to_datetime(x, errors="coerce", dayfirst=True)
    return None if pd.isna(ts) else ts.date()

def to_str_no_leading_zeros(x) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip()
    s2 = s.lstrip("0")
    return s2 if s2 != "" else "0"

def normalize_out_value(v):
    if v is None:
        return None
    if isinstance(v, (np.floating, np.integer)):
        return None if pd.isna(v) else v.item()
    if isinstance(v, pd.Timestamp):
        return None if pd.isna(v) else v.strftime("%Y-%m-%d")
    if isinstance(v, dt.datetime):
        return v.isoformat()
    if isinstance(v, dt.date):
        return v.isoformat()
    if isinstance(v, float) and pd.isna(v):
        return None
    if isinstance(v, str):
        s = v.strip()
        return None if s == "" else s
    return v

def canon_chassis(x: Optional[str]) -> Optional[str]:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    s = str(x).strip().upper()
    if not s:
        return None
    s = re.sub(r"[^A-Z0-9]", "", s)
    return s or None

def clean_chassis_for_excel(x) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip().replace("-", "")
    s = re.sub(r"[^A-Za-z0-9]", "", s)
    return s or None

FB_ILLEGAL = re.compile(r"[.\$\[\]#/]")
def fb_key_from_chassis(chassis_raw: Optional[str]) -> Optional[str]:
    if chassis_raw is None or (isinstance(chassis_raw, float) and pd.isna(chassis_raw)):
        return None
    s = str(chassis_raw).strip()
    if not s:
        return None
    if FB_ILLEGAL.search(s):
        s = FB_ILLEGAL.sub("_", s)
    return s

def key_base(k: str) -> str:
    return k.split("__", 1)[0] if "__" in k else k

def pick_node_chassis(node: Any, key: str) -> Optional[str]:
    if isinstance(node, dict):
        for fn in ["Chassis No", "CHASSIS", "Chassis", "chassis", "chassisNo"]:
            v = node.get(fn)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
    return key_base(key).strip()

def chunks_list(lst: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def fetch_vin_map(clean_list: List[str], chunk_size: int = 400) -> Dict[str, str]:
    vals = sorted(set([x for x in clean_list if x]))
    if not vals:
        return {}
    out: Dict[str, str] = {}
    for part in chunks_list(vals, chunk_size):
        in_list = ",".join("'" + p.replace("'", "''") + "'" for p in part)
        sql = VIN_SQL_TMPL.format(in_list=in_list)
        df_v = hana_query(sql)
        if df_v is None or df_v.empty:
            continue
        cols = {c.lower(): c for c in df_v.columns}
        ser_col = cols.get("sernr")
        vin_col = cols.get("vin_number")
        for _, r in df_v.iterrows():
            ser = r.get(ser_col)
            vin = r.get(vin_col)
            ser_clean = clean_chassis_for_excel(ser)
            vin2 = None if pd.isna(vin) else str(vin).strip()
            if ser_clean and vin2:
                out[ser_clean] = vin2
    return out

def firebase_init():
    if firebase_admin._apps:
        return

    if FIREBASE_SA_JSON:
        info = json.loads(FIREBASE_SA_JSON)
        cred = credentials.Certificate(info)
    elif FIREBASE_SA_JSON_BASE64:
        raw = base64.b64decode(FIREBASE_SA_JSON_BASE64).decode("utf-8")
        info = json.loads(raw)
        cred = credentials.Certificate(info)
    else:
        cred = credentials.Certificate(FIREBASE_SA_PATH)

    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})

def keep_score(node: Any) -> int:
    if not isinstance(node, dict):
        return 0
    return sum(1 for f in KEEP_FIELDS if f in node)

# ===================== build df_final =====================
def build_final(orderlist_bytes: bytes, bp_bytes: bytes) -> pd.DataFrame:
    base = hana_query(BASELINE_SQL_0024_0026).copy()
    base = base.rename(columns={
        "VBELN": "Sales Order",
        "MATNR": "Material No",
        "KALAB": "KALAB",
        "LGORT": "Storage Location",
        "BUDAT_MKPF_MAX": "Goods Issue Date",
        "BUDAT_MKPF_MIN": "Goods Receipt Date",
    })
    base["Goods Receipt Date"] = base["Goods Receipt Date"].apply(parse_to_date)

    enrich = hana_query(ENRICH_SQL).copy()
    enrich = enrich.rename(columns={
        "VBELN": "Sales Order",
        "SHIPTO": "Ship-to Code",
        "CHASSIS": "Chassis No",
        "PO_NO": "Matched PO No",
        "PO_DATE": "PO Creation Date",
    })
    enrich["PO Creation Date"] = enrich["PO Creation Date"].apply(parse_to_date)
    enrich = enrich.sort_values(by=["Sales Order", "PO Creation Date"], ascending=[True, False], na_position="last")
    enrich = enrich.drop_duplicates(subset=["Sales Order"], keep="first")

    df = base.merge(enrich, how="left", on=["Sales Order"], validate="m:1")
    df["Ship-to Code"] = df["Ship-to Code"].apply(to_str_no_leading_zeros)
    df["SO Number"] = df["Sales Order"].apply(to_str_no_leading_zeros)

    ol = read_excel_bytes_select_sheet(orderlist_bytes, "Orderlist")
    for c in ["Chassis", "Regent Production", "Model", "Dealer", "Customer"]:
        if c not in ol.columns:
            ol[c] = None
    ol["Chassis_Clean"] = ol["Chassis"].apply(clean_chassis_for_excel)
    ol_keep = ol[["Chassis_Clean", "Regent Production", "Model", "Dealer", "Customer"]].copy()
    rp = ol_keep["Regent Production"].astype(str).str.strip()
    ol_keep["_rank"] = np.where(rp == "Ready for Dispatch", 0, 1)
    ol_keep = ol_keep.sort_values(by=["_rank"], ascending=True, na_position="last")
    ol_dedup = ol_keep.drop_duplicates(subset=["Chassis_Clean"], keep="first").drop(columns=["_rank"])

    bp = read_excel_bytes_select_sheet(bp_bytes, "BP")
    for c in ["Abbrev.", "Delivery to (SAP Code)"]:
        if c not in bp.columns:
            bp[c] = None

    def delivery_to_text(v):
        if pd.isna(v):
            return None
        try:
            return str(int(v))
        except Exception:
            return (str(v).strip().lstrip("0") or "0")

    bp["DeliveryTo_Text"] = bp["Delivery to (SAP Code)"].apply(delivery_to_text)
    bp_dedup = bp[["Abbrev.", "DeliveryTo_Text"]].drop_duplicates(subset=["DeliveryTo_Text"], keep="first")

    df["Chassis_Clean"] = df["Chassis No"].apply(clean_chassis_for_excel)
    df = df.merge(bp_dedup, how="left", left_on="Ship-to Code", right_on="DeliveryTo_Text")
    df = df.merge(ol_dedup, how="left", on="Chassis_Clean")

    def _clean_text(x) -> Optional[str]:
        if pd.isna(x):
            return None
        s = str(x).strip()
        return s if s else None

    def dealer_check_row(r):
        a = _clean_text(r.get("Abbrev."))
        d = _clean_text(r.get("Dealer"))
        if a is None and d is not None:
            return "Mismatch"
        if a is None or d is None:
            return "no Reference"
        return "OK" if a == d else "Mismatch"

    def status_check(rp_val):
        rp2 = _clean_text(rp_val)
        if rp2 is None:
            return "no Reference"
        return "OK" if rp2 == "Ready for Dispatch" else "invalid stock"

    df["DealerCheck"] = df.apply(dealer_check_row, axis=1)
    df["Statuscheck"] = df["Regent Production"].apply(status_check)

    vin_map = fetch_vin_map([x for x in df["Chassis_Clean"].tolist() if x])
    df["Vin Number"] = df["Chassis_Clean"].apply(lambda x: vin_map.get(x, None) if x else None)

    final = pd.DataFrame({
        "Sales Order": df["Sales Order"],
        "SO Number": df["SO Number"],
        "Ship-to Code": df["Ship-to Code"],
        "SAP Data": df["Abbrev."],
        "Chassis No": df["Chassis No"],
        "Vin Number": df["Vin Number"],
        "Goods Receipt Date": df["Goods Receipt Date"],
        "GR to GI Days": None,
        "Matched PO No": df["Matched PO No"],
        "Regent Production": df["Regent Production"],
        "Model": df["Model"],
        "Scheduled Dealer": df["Dealer"],
        "Customer": df["Customer"],
        "DealerCheck": df["DealerCheck"],
        "Statuscheck": df["Statuscheck"],
    })

    today = dt.date.today()
    final["GR to GI Days"] = final["Goods Receipt Date"].apply(
        lambda d: None if d is None or pd.isna(d) else (today - d).days
    )
    return final

# ===================== Firebase sync =====================
def sync_firebase(df_final: pd.DataFrame, root_path: str):
    firebase_init()
    ref_root = db.reference(root_path)

    existing = ref_root.get() or {}
    if not isinstance(existing, dict):
        existing = {}

    sap_set = set()
    sap_rows: List[Dict[str, Any]] = []
    for _, r in df_final.iterrows():
        ch = r.get("Chassis No")
        cc = canon_chassis(ch)
        if cc:
            sap_set.add(cc)
        sap_rows.append(r.to_dict())

    log.info("SAP rows=%s | SAP chassis unique=%s", len(df_final), len(sap_set))
    log.info("Firebase existing nodes=%s", len(existing))

    fb_map: Dict[str, List[str]] = {}
    fb_chassis_of_key: Dict[str, str] = {}
    for k, v in existing.items():
        ch = pick_node_chassis(v, k)
        cc = canon_chassis(ch)
        if not cc:
            continue
        fb_chassis_of_key[k] = cc
        fb_map.setdefault(cc, []).append(k)

    keep_key_for_chassis: Dict[str, str] = {}
    for cc, keys in fb_map.items():
        keys_sorted = sorted(keys, key=lambda kk: (-keep_score(existing.get(kk)), kk))
        keep_key_for_chassis[cc] = keys_sorted[0]

    to_delete: List[str] = []
    for k, v in existing.items():
        ch_cc = fb_chassis_of_key.get(k)
        if not ch_cc:
            to_delete.append(k)
            continue
        if ch_cc not in sap_set:
            to_delete.append(k)
            continue
        if keep_key_for_chassis.get(ch_cc) != k:
            to_delete.append(k)

    if DRY_RUN:
        log.info("DRY_RUN=True -> skip deletion")
    else:
        for part in chunks_list(to_delete, DELETE_BATCH_SIZE):
            ref_root.update({kk: None for kk in part})

    if not DRY_RUN and to_delete:
        existing = ref_root.get() or {}
        if not isinstance(existing, dict):
            existing = {}

    fb_map = {}
    keep_key_for_chassis = {}
    for k, v in existing.items():
        cc = canon_chassis(pick_node_chassis(v, k))
        if cc:
            fb_map.setdefault(cc, []).append(k)
    for cc, keys in fb_map.items():
        keys_sorted = sorted(keys, key=lambda kk: (-keep_score(existing.get(kk)), kk))
        keep_key_for_chassis[cc] = keys_sorted[0]

    SYNC_FIELDS = [
        "Sales Order", "SO Number", "Ship-to Code", "SAP Data", "Chassis No", "Vin Number",
        "Goods Receipt Date", "GR to GI Days", "Matched PO No",
        "Regent Production", "Model", "Scheduled Dealer", "Customer", "DealerCheck", "Statuscheck",
    ]
    SYNC_FIELDS = [f for f in SYNC_FIELDS if f not in KEEP_FIELDS]

    patch: Dict[str, Any] = {}
    created = 0
    updated = 0

    for i, row in enumerate(sap_rows):
        ch_raw = row.get("Chassis No")
        cc = canon_chassis(ch_raw)

        if cc and cc in keep_key_for_chassis:
            key = keep_key_for_chassis[cc]
        else:
            base_key = fb_key_from_chassis(ch_raw) or f"NOCHASSIS__ROW-{i}"
            key = base_key
            n = 1
            while key in existing:
                n += 1
                key = f"{base_key}__{n}"
            existing[key] = {}
            created += 1

        for f in SYNC_FIELDS:
            v = normalize_out_value(row.get(f))
            if v is None:
                continue
            patch[f"{key}/{f}"] = v
        updated += 1

    log.info("To patch update rows=%s (created=%s)", updated, created)
    log.info("Patch fields count=%s", len(patch))

    if DRY_RUN:
        log.info("DRY_RUN=True -> skip patch update")
        return

    items = list(patch.items())
    for i in range(0, len(items), PATCH_BATCH_SIZE):
        ref_root.update(dict(items[i:i + PATCH_BATCH_SIZE]))

    log.info("Patch update done.")

# ===================== runner =====================
def run_sync() -> Dict[str, Any]:
    validate_required_env()

    if USE_LOCAL_FILES:
        orderlist_bytes = read_local_bytes(ORDERLIST_LOCAL_PATH)
        bp_bytes = read_local_bytes(BP_LOCAL_PATH)
    else:
        orderlist_bytes = fetch_excel_bytes(ORDERLIST_PUBLIC_DL)
        bp_bytes = fetch_excel_bytes(BP_PUBLIC_DL)

    df_final = build_final(orderlist_bytes, bp_bytes)
    sync_firebase(df_final, FIREBASE_TARGET_ROOT)

    return {
        "ok": True,
        "rows": int(len(df_final)),
        "target_root": FIREBASE_TARGET_ROOT,
        "dry_run": DRY_RUN,
        "timestamp": dt.datetime.utcnow().isoformat() + "Z",
    }

# ===================== routes =====================
@app.get("/health")
def health():
    return jsonify({"ok": True, "service": "dispatch-sync"})

@app.post("/run")
def run_endpoint():
    try:
        if RUN_TOKEN:
            req_token = request.headers.get("X-Run-Token", "")
            if req_token != RUN_TOKEN:
                return jsonify({"ok": False, "error": "Unauthorized"}), 401

        result = run_sync()
        return jsonify(result), 200

    except Exception as e:
        log.exception("Sync failed")
        return jsonify({
            "ok": False,
            "error": str(e),
            "timestamp": dt.datetime.utcnow().isoformat() + "Z",
        }), 500

# ===================== local entry =====================
if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)

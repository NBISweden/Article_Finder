import os
import re
import time
import json
import csv
import argparse
from pathlib import Path
from typing import List, Dict, Any

import requests
import pandas as pd
from wos_credentials import get_wos_api_key


DATABASE_ID = "WOS"
OPTION_VIEW = "FR"
PAGE_SIZE = 100
SLEEP_BETWEEN_CALLS = 0.25

MAX_QUERY_CHARS = 1400
MAX_DOIS_PER_QUERY = 25

ENABLE_FALLBACK_DO_SINGLE = True
SLEEP_BETWEEN_FALLBACKS = 0.15

DOI_RE = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.I)

WOS_COLUMNS_ORDER = [
    "UT",
    "Title",
    "Journal",
    "Year",
    "DOI",
    "Authors",
    "AuthorEmails",
    "Abstract",
    "FundingText",
    "FundingAgencies",
    "GrantNumbers",
    "AuthorKeywords",
    "KeywordsPlus",
    "WoSCategoriesTraditional",
    "WoSCategoriesExtended",
    "Found",
    "DOI_from_record",
]


def clean_text(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, str):
        return re.sub(r"\s+", " ", x).strip()
    if isinstance(x, (list, tuple)):
        return clean_text(" ".join([clean_text(i) for i in x if clean_text(i)]))
    return clean_text(str(x))


def as_list(x):
    if x is None:
        return []
    return x if isinstance(x, list) else [x]


def extract_text(obj) -> str:
    if obj is None:
        return ""
    if isinstance(obj, str):
        return obj.strip()
    if isinstance(obj, list):
        return " ".join(extract_text(x) for x in obj if extract_text(x)).strip()
    if isinstance(obj, dict):
        for k in ["p", "content", "value", "#text", "text"]:
            if k in obj:
                return extract_text(obj[k])
        return " ".join(extract_text(v) for v in obj.values() if extract_text(v)).strip()
    return str(obj).strip()


def dedupe_keep_order(items):
    seen = set()
    out = []
    for x in items:
        x = clean_text(x)
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def normalize_doi(d: Any) -> str:
    if d is None:
        return ""
    d = str(d).strip()

    d = re.sub(r"^https?://(dx\.)?doi\.org/", "", d, flags=re.I).strip()
    d = re.sub(r"^doi:\s*", "", d, flags=re.I).strip()

    d = d.strip("\"'“”’` ")
    d = d.rstrip(" .;,)\t\r\n]}>")
    d = d.lower()
    d = d.replace("\u00a0", " ").strip()
    return d


def get_with_retry(session: requests.Session, url: str, *, params=None, timeout=60, max_tries=8):
    tries = 0
    while True:
        tries += 1
        resp = session.get(url, params=params, timeout=timeout)

        if resp.status_code == 429:
            wait_s = min(5 * (2 ** (tries - 1)), 60)
            print(f"429 rate limit -> sleep {wait_s:.0f}s (try {tries}/{max_tries})")
            time.sleep(wait_s)
            if tries >= max_tries:
                resp.raise_for_status()
            continue

        if 500 <= resp.status_code < 600:
            wait_s = min(2 * tries, 20)
            print(f"{resp.status_code} server error -> sleep {wait_s:.0f}s (try {tries}/{max_tries})")
            time.sleep(wait_s)
            if tries >= max_tries:
                resp.raise_for_status()
            continue

        if resp.status_code >= 400:
            print("HTTP", resp.status_code, "URL:", resp.request.url)
            print("Response head:", resp.text[:1200])
            resp.raise_for_status()

        return resp


def deep_get(obj: Any, keys: List[str]) -> Any:
    cur = obj
    for k in keys:
        if cur is None:
            return None
        if isinstance(cur, dict):
            cur = cur.get(k)
        else:
            return None
    return cur


def read_doi_table(path: str) -> pd.DataFrame:
    p = Path(path)
    suf = p.suffix.lower()

    if suf in [".xlsx", ".xls"]:
        df = pd.read_excel(p, dtype=str)
    elif suf == ".tsv":
        df = pd.read_csv(p, dtype=str, sep="\t", engine="python")
    elif suf == ".csv":
        try:
            df = pd.read_csv(p, dtype=str)
            if len(df.columns) == 1 and ";" in df.columns[0]:
                df = pd.read_csv(p, dtype=str, sep=";")
        except pd.errors.ParserError:
            df = pd.read_csv(p, dtype=str, engine="python")
            if len(df.columns) == 1 and ";" in df.columns[0]:
                df = pd.read_csv(p, dtype=str, sep=";", engine="python")
    else:
        dois = [ln.strip() for ln in p.read_text(encoding="utf-8").splitlines() if ln.strip()]
        df = pd.DataFrame({"DOI": dois})

    df = df.fillna("")
    df.columns = [c.strip() for c in df.columns]
    return df


def find_doi_column(df: pd.DataFrame) -> str:
    for col in ["DOI", "doi", "Doi", "doi "]:
        if col in df.columns:
            return col
    raise ValueError(f"No DOI column found. Columns: {list(df.columns)}")


def extract_dois_from_table(df: pd.DataFrame, doi_col: str) -> List[str]:
    raw = df[doi_col].astype(str).tolist()
    seen = set()
    out: List[str] = []
    for d in raw:
        nd = normalize_doi(d)
        if not nd:
            continue
        if nd not in seen:
            seen.add(nd)
            out.append(nd)
    return out


def build_usr_query_do(dois: List[str]) -> str:
    inner = " OR ".join([f"\"{d}\"" for d in dois])
    return f"DO=({inner})"


def build_usr_query_do_single(doi: str) -> str:
    return f"DO=(\"{doi}\")"


def chunk_dois(dois: List[str]) -> List[List[str]]:
    chunks: List[List[str]] = []
    cur: List[str] = []
    cur_len = 0

    for d in dois:
        term = f"\"{d}\""
        add_len = len(term) + (4 if cur else 0)
        if cur and (len(cur) >= MAX_DOIS_PER_QUERY or (cur_len + add_len) > MAX_QUERY_CHARS):
            chunks.append(cur)
            cur = [d]
            cur_len = len(term)
        else:
            cur.append(d)
            cur_len = cur_len + add_len if cur else len(term)

    if cur:
        chunks.append(cur)
    return chunks


def extract_records_any(data: Dict) -> List[Dict]:
    try:
        r = data.get("Data", {}).get("Records", {}).get("records", {})
        if isinstance(r, dict):
            rec = r.get("REC", [])
            if isinstance(rec, dict):
                return [rec]
            if isinstance(rec, list):
                return [x for x in rec if isinstance(x, dict)]
        if isinstance(r, list):
            return [x for x in r if isinstance(x, dict)]
    except Exception:
        pass

    candidates: List[List[Dict]] = []

    def walk(obj: Any):
        if isinstance(obj, dict):
            if "REC" in obj:
                v = obj["REC"]
                if isinstance(v, dict):
                    candidates.append([v])
                elif isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                    candidates.append(v)

            if "records" in obj:
                v = obj["records"]
                if isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                    candidates.append(v)
                elif isinstance(v, dict) and "REC" in v:
                    vv = v["REC"]
                    if isinstance(vv, dict):
                        candidates.append([vv])
                    elif isinstance(vv, list) and vv and all(isinstance(x, dict) for x in vv):
                        candidates.append(vv)

            for vv in obj.values():
                walk(vv)
        elif isinstance(obj, list):
            for x in obj:
                walk(x)

    walk(data)
    if not candidates:
        return []

    best = max(candidates, key=len)
    with_uid = [x for x in best if isinstance(x, dict) and ("UID" in x or "uid" in x)]
    return with_uid if with_uid else best


def extract_uid(rec: Dict) -> str:
    return rec.get("UID") or rec.get("uid") or ""


def _try_extract_doi_from_identifiers(ident_obj: Any) -> List[str]:
    found: List[str] = []
    if ident_obj is None:
        return found

    if isinstance(ident_obj, dict) and "identifier" in ident_obj:
        ident_obj = ident_obj["identifier"]

    items = ident_obj if isinstance(ident_obj, list) else [ident_obj]

    for it in items:
        if not isinstance(it, dict):
            continue

        t = (it.get("@type") or it.get("type") or it.get("@name") or "").lower()
        if "doi" in t:
            val = it.get("#text") or it.get("value") or it.get("content") or it.get("text") or it.get("@value") or ""
            nd = normalize_doi(val)
            if nd:
                found.append(nd)

        if not found:
            s = json.dumps(it, ensure_ascii=False)
            m = DOI_RE.search(s)
            if m:
                found.append(normalize_doi(m.group(0)))

    return found


def extract_doi_from_record(rec: Dict) -> str:
    ident1 = deep_get(rec, ["dynamic_data", "cluster_related", "identifiers"])
    for d in _try_extract_doi_from_identifiers(ident1):
        return d

    ident2 = deep_get(rec, ["static_data", "summary", "identifiers"])
    for d in _try_extract_doi_from_identifiers(ident2):
        return d

    ident3 = deep_get(rec, ["static_data", "fullrecord_metadata", "identifiers"])
    for d in _try_extract_doi_from_identifiers(ident3):
        return d

    summary = deep_get(rec, ["static_data", "summary"])
    if summary is not None:
        s = json.dumps(summary, ensure_ascii=False)
        m = DOI_RE.search(s)
        if m:
            return normalize_doi(m.group(0))

    cluster = deep_get(rec, ["dynamic_data", "cluster_related"])
    if cluster is not None:
        s = json.dumps(cluster, ensure_ascii=False)
        m = DOI_RE.search(s)
        if m:
            return normalize_doi(m.group(0))

    return ""


def pick_title_from_titles(titles_obj: Any, wanted_type: str) -> str:
    if not titles_obj:
        return ""

    if isinstance(titles_obj, dict) and "title" in titles_obj:
        titles_list = titles_obj.get("title")
    else:
        titles_list = titles_obj

    for t in as_list(titles_list):
        if not isinstance(t, dict):
            continue
        ttype = str(t.get("@type") or t.get("type") or "").lower()
        if ttype == wanted_type.lower():
            return clean_text(t.get("#text") or t.get("content") or t.get("value") or t.get("text") or "")

    return ""


def get_pubyear(rec: Dict) -> str:
    pubyear = deep_get(rec, ["static_data", "summary", "pub_info", "pubyear"])
    if pubyear:
        return clean_text(pubyear)
    pubyear2 = deep_get(rec, ["static_data", "summary", "pub_info", "pubYear"])
    return clean_text(pubyear2)


def get_authors(rec: Dict) -> str:
    names = deep_get(rec, ["static_data", "summary", "names", "name"])
    out = []
    for n in as_list(names):
        if not isinstance(n, dict):
            continue
        fn = n.get("full_name") or n.get("fullName") or n.get("display_name") or n.get("name")
        if fn:
            out.append(clean_text(fn))
    return "; ".join(dedupe_keep_order(out))


def get_author_emails(rec: Dict) -> str:
    names = deep_get(rec, ["static_data", "summary", "names", "name"])
    emails = []
    for n in as_list(names):
        if isinstance(n, dict):
            e = n.get("email_addr") or n.get("email") or n.get("emailAddress")
            if e:
                emails.append(clean_text(e))
    return "; ".join(dedupe_keep_order(emails))


def get_abstract(rec: Dict) -> str:
    abstracts = deep_get(rec, ["static_data", "fullrecord_metadata", "abstracts", "abstract"])
    text = " ".join(extract_text(a) for a in as_list(abstracts)).strip()
    return clean_text(text)


def get_keywords(rec: Dict):
    author_keywords = []
    kw_block = deep_get(rec, ["static_data", "fullrecord_metadata", "keywords", "keyword"])
    for kw in as_list(kw_block):
        author_keywords.append(clean_text(extract_text(kw)))

    keywords_plus = []
    kp_block = deep_get(rec, ["static_data", "item", "keywords_plus", "keyword"])
    for kw in as_list(kp_block):
        keywords_plus.append(clean_text(extract_text(kw)))

    author_keywords = dedupe_keep_order([x for x in author_keywords if x])
    keywords_plus = dedupe_keep_order([x for x in keywords_plus if x])
    return "; ".join(author_keywords), "; ".join(keywords_plus)


def get_categories(rec: Dict):
    subjects = deep_get(rec, ["static_data", "fullrecord_metadata", "category_info", "subjects", "subject"])
    trad, ext = [], []
    for s in as_list(subjects):
        if isinstance(s, dict):
            asc = (s.get("ascatype") or s.get("@ascatype") or "").lower()
            txt = extract_text(s.get("subject")) or extract_text(s)
            txt = clean_text(txt)
            if not txt:
                continue
            if asc == "extended":
                ext.append(txt)
            else:
                trad.append(txt)
        else:
            txt = clean_text(extract_text(s))
            if txt:
                trad.append(txt)

    trad = dedupe_keep_order(trad)
    ext = dedupe_keep_order(ext)
    return "; ".join(trad), "; ".join(ext)


def get_funding(rec: Dict):
    static = rec.get("static_data", {}) or {}
    full_md = static.get("fullrecord_metadata", {}) or {}

    fund_candidates = []

    for candidate in [static.get("fund_ack"), full_md.get("fund_ack")]:
        for fa in as_list(candidate):
            if isinstance(fa, dict):
                fund_candidates.append(fa)

    fund_text = ""
    agencies, grants = [], []

    for fa in fund_candidates:
        fund_text = fund_text or extract_text(
            fa.get("fund_text") or fa.get("funding_text") or fa.get("text")
        )

        for ag in as_list(fa.get("fund_agency")):
            agencies.append(extract_text(ag))

        for gr in as_list(fa.get("grant_no") or fa.get("grant_number")):
            grants.append(extract_text(gr))

        grants_block = fa.get("grants") or {}
        grant_list = as_list(grants_block.get("grant")) if isinstance(grants_block, dict) else []
        for g in grant_list:
            if not isinstance(g, dict):
                continue
            agencies.append(extract_text(g.get("grant_agency") or g.get("agency") or g.get("funding_agency")))
            grants.append(extract_text(g.get("grant_id") or g.get("grant_number") or g.get("grant_no")))

    agencies = dedupe_keep_order([clean_text(x) for x in agencies if clean_text(x)])
    grants = dedupe_keep_order([clean_text(x) for x in grants if clean_text(x)])
    return clean_text(fund_text), "; ".join(agencies), "; ".join(grants)


def flatten_record(rec: Dict) -> Dict[str, str]:
    row: Dict[str, str] = {}

    row["UT"] = extract_uid(rec)

    titles_obj = deep_get(rec, ["static_data", "summary", "titles"])
    row["Title"] = pick_title_from_titles(titles_obj, "item")
    row["Journal"] = pick_title_from_titles(titles_obj, "source")

    row["Year"] = get_pubyear(rec)

    row["DOI_from_record"] = extract_doi_from_record(rec)

    row["Authors"] = get_authors(rec)
    row["AuthorEmails"] = get_author_emails(rec)

    row["Abstract"] = get_abstract(rec)

    ftxt, fags, gnums = get_funding(rec)
    row["FundingText"] = ftxt
    row["FundingAgencies"] = fags
    row["GrantNumbers"] = gnums

    akw, kp = get_keywords(rec)
    row["AuthorKeywords"] = akw
    row["KeywordsPlus"] = kp

    cat_trad, cat_ext = get_categories(rec)
    row["WoSCategoriesTraditional"] = cat_trad
    row["WoSCategoriesExtended"] = cat_ext

    return row

def run_fetch_by_doi(
    doi_list_path: str,
    out_dir: Path,
    out_csv: Path | None = None,
    page_size: int = 100,
    sleep_between_calls: float = 0.25,
):
    if page_size <= 0:
        raise ValueError("page_size must be greater than 0.")
    if sleep_between_calls < 0:
        raise ValueError("sleep_between_calls must be 0 or greater.")

    api_key = get_wos_api_key()
    
    out_dir.mkdir(parents=True, exist_ok=True)

    if out_csv is None:
        out_csv = out_dir / "wos_results_by_doi.csv"
   
    input_df = read_doi_table(doi_list_path)
    doi_col_original = find_doi_column(input_df)

    if doi_col_original != "DOI_input":
        input_df.rename(columns={doi_col_original: "DOI_input"}, inplace=True)

    input_df["DOI_input"] = input_df["DOI_input"].fillna("").astype(str)
    input_df["DOI_norm"] = input_df["DOI_input"].apply(normalize_doi)

    dois = extract_dois_from_table(input_df, "DOI_input")
    print(f"Loaded {len(dois)} normalized DOI(s) from input rows={len(input_df)}")

    chunks = chunk_dois(dois)
    print(f"Will query in {len(chunks)} chunk(s) (max {MAX_DOIS_PER_QUERY} DOI/chunk)")

    base = "https://api.clarivate.com/api/wos"
    headers = {"X-ApiKey": api_key, "Accept": "application/json"}

    raw_pages = out_dir / "raw_pages.jsonl"
    found_map: Dict[str, Dict] = {}

    with requests.Session() as session:
        session.headers.update(headers)

        for ci, doi_chunk in enumerate(chunks, start=1):
            usr_query = build_usr_query_do(doi_chunk)
            chunk_set = set(doi_chunk)

            print(f"\nChunk {ci}/{len(chunks)}: {len(doi_chunk)} DOI(s)")
            seed_params = {
                "databaseId": DATABASE_ID,
                "usrQuery": usr_query,
                "count": 1,
                "firstRecord": 1,
                "optionView": "SR",
            }
            seed_json = get_with_retry(session, base, params=seed_params).json()
            total_found = int((seed_json.get("QueryResult") or {}).get("RecordsFound", 0) or 0)
            print("RecordsFound:", total_found)

            if total_found == 0:
                continue

            for first in range(1, total_found + 1, page_size):
                batch = min(page_size, total_found - first + 1)
                params = {
                    "databaseId": DATABASE_ID,
                    "usrQuery": usr_query,
                    "count": batch,
                    "firstRecord": first,
                    "optionView": OPTION_VIEW,
                    "links": "true",
                }
                data = get_with_retry(session, base, params=params).json()

                with raw_pages.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"phase": 1, "chunk": ci, "first": first, "data": data}, ensure_ascii=False) + "\n")

                recs = extract_records_any(data)
                print(f"  Fetched {first}-{first + batch - 1}: extracted {len(recs)} record(s)")

                for r in recs:
                    d_rec = extract_doi_from_record(r)
                    if d_rec and d_rec in chunk_set and d_rec not in found_map:
                        found_map[d_rec] = r

                time.sleep(sleep_between_calls)

        if ENABLE_FALLBACK_DO_SINGLE:
            missing = [d for d in dois if d not in found_map]
            print(f"\nDO= phase done. Missing after DO=: {len(missing)}")

            for idx, d in enumerate(missing, start=1):
                usr_query = build_usr_query_do_single(d)
                params = {
                    "databaseId": DATABASE_ID,
                    "usrQuery": usr_query,
                    "count": 1,
                    "firstRecord": 1,
                    "optionView": OPTION_VIEW,
                    "links": "true",
                }

                try:
                    js = get_with_retry(session, base, params=params).json()
                except Exception:
                    time.sleep(SLEEP_BETWEEN_FALLBACKS)
                    continue

                with raw_pages.open("a", encoding="utf-8") as f:
                    f.write(json.dumps({"phase": 2, "doi": d, "data": js}, ensure_ascii=False) + "\n")

                recs = extract_records_any(js)
                if recs:
                    found_map[d] = recs[0]

                if idx % 25 == 0:
                    print(f"  fallback checked {idx}/{len(missing)}")

                time.sleep(SLEEP_BETWEEN_FALLBACKS)

    final_missing = [d for d in dois if d not in found_map]
    (out_dir / "missing_dois.txt").write_text("\n".join(final_missing), encoding="utf-8")
    print(f"\nFinal found DOIs: {len(found_map)} / {len(dois)}")
    print(f"Final missing DOIs written to: {out_dir / 'missing_dois.txt'}")

    flat_rows = []
    for doi_norm_key, rec in found_map.items():
        row = flatten_record(rec)
        row["DOI_norm"] = doi_norm_key
        row["DOI"] = doi_norm_key
        row["Found"] = "yes"
        flat_rows.append(row)

    wos_df = pd.DataFrame(flat_rows)

    merged = input_df.merge(wos_df, on="DOI_norm", how="left", suffixes=("_input", ""))
    merged["Found"] = merged["Found"].fillna("no")
    merged["DOI_input"] = merged["DOI_input"].fillna("").astype(str)

    input_cols = [c for c in input_df.columns if c != "DOI_norm"]
    wos_cols_present = [c for c in WOS_COLUMNS_ORDER if c in merged.columns]
    extra_cols = [c for c in merged.columns if c not in input_cols + wos_cols_present + ["DOI_norm"]]
    final_cols = input_cols + wos_cols_present + extra_cols

    merged[final_cols].to_csv(out_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    print(f"Saved FINAL CSV: {out_csv}")

    missing_rows = merged[merged["Found"] == "no"].copy()
    out_missing_csv = out_dir / "missing_rows_with_input_columns.csv"
    missing_rows[final_cols].to_csv(out_missing_csv, index=False, encoding="utf-8-sig", quoting=csv.QUOTE_MINIMAL)
    print(f"Saved missing rows: {out_missing_csv}")

    return {"out_dir": out_dir, "summary_csv": out_csv}


def build_argparser():
    ap = argparse.ArgumentParser(description="Fetch WoS full records using a DOI list, output CSV")
    ap.add_argument("--doi-list", required=True, help="Path to DOI list file (.xlsx/.csv/.tsv/.txt)")
    ap.add_argument("--out-dir", default="WOS_by_DOI", help="Output directory")
    ap.add_argument("--out-csv", default="", help="Override output CSV path")
    ap.add_argument("--page-size", type=int, default=100, help="Page size for WoS fetch")
    ap.add_argument("--sleep", type=float, default=0.25, help="Sleep between API calls")
    return ap

def main():
    args = build_argparser().parse_args()
    out_dir = Path(args.out_dir)
    out_csv = Path(args.out_csv) if args.out_csv else None

    run_fetch_by_doi(
        args.doi_list,
        out_dir=out_dir,
        out_csv=out_csv,
        page_size=args.page_size,
        sleep_between_calls=args.sleep,
    )

if __name__ == "__main__":
    main()

import os
import time
import json
import re
from pathlib import Path
import requests
import pandas as pd
from dotenv import load_dotenv

# SETTINGS

USR_QUERY = "CU=(Sweden) AND PY=2025"
DATABASE_ID = "WOS"
PAGE_SIZE = 100
MAX_RECORDS = None  

OUT_DIR = Path("WOS_fetched_results")
SAVE_DEBUG_FIRST_PAGE = True
SLEEP_BETWEEN_CALLS = 0.25

# Helpers

DOI_RE = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.I)

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

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
        if "p" in obj:
            return extract_text(obj["p"])
        if "content" in obj and isinstance(obj["content"], str):
            return obj["content"].strip()
        if "value" in obj and isinstance(obj["value"], str):
            return obj["value"].strip()
        return " ".join(extract_text(v) for v in obj.values() if extract_text(v)).strip()
    return ""

def dedupe_keep_order(items):
    seen = set()
    out = []
    for x in items:
        x = (x or "").strip()
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out

def write_jsonl(path: Path, obj: dict):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

def get_with_retry(session: requests.Session, url: str, *, params=None, timeout=60, max_tries=8):
    tries = 0
    while True:
        tries += 1
        resp = session.get(url, params=params, timeout=timeout)

        if resp.status_code == 429:
            wait_s = min(5 * (2 ** (tries - 1)), 60)
            print(f"429 rate limit → sleep {wait_s:.0f}s (try {tries}/{max_tries})")
            time.sleep(wait_s)
            if tries >= max_tries:
                resp.raise_for_status()
            continue

        if 500 <= resp.status_code < 600:
            wait_s = min(2 * tries, 20)
            print(f"{resp.status_code} server error → sleep {wait_s:.0f}s (try {tries}/{max_tries})")
            time.sleep(wait_s)
            if tries >= max_tries:
                resp.raise_for_status()
            continue

        if resp.status_code >= 400:
            print("HTTP", resp.status_code, "URL:", resp.request.url)
            print("Response head:", resp.text[:1200])
            resp.raise_for_status()

        return resp

def get_query_id_and_total(seed_json: dict):
    qr = (seed_json.get("QueryResult") or {})
    total = int(qr.get("RecordsFound", 0) or 0)

    qid = None
    for k in ["QueryID", "QueryId", "queryId", "queryID"]:
        if k in qr:
            qid = qr[k]
            break
    return qid, total

def get_unique_id(rec: dict) -> str:
    return rec.get("UID", "") or rec.get("uid", "")

def extract_records_any(data):

    try:
        r = data.get("Data", {}).get("Records", {}).get("records", {})
        if isinstance(r, dict):
            rec = r.get("REC", [])
            if isinstance(rec, dict):
                return [rec]
            if isinstance(rec, list):
                return rec
        if isinstance(r, list):
            return r
    except Exception:
        pass


    candidates = []

    def walk(obj):
        if isinstance(obj, dict):
            # direct REC
            if "REC" in obj:
                v = obj["REC"]
                if isinstance(v, dict):
                    candidates.append([v])
                elif isinstance(v, list) and v and all(isinstance(x, dict) for x in v):
                    candidates.append(v)
            # sometimes "records" directly holds list/dict
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
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for x in obj:
                walk(x)

    walk(data)
    if not candidates:
        return []

    best = max(candidates, key=len)


    with_uid = [x for x in best if isinstance(x, dict) and ("UID" in x or "uid" in x)]
    return with_uid if with_uid else best

def pick_title(summary: dict, wanted_type: str) -> str:
    titles_block = (summary.get("titles") or {})
    titles_list = titles_block.get("title")
    for t in as_list(titles_list):
        if isinstance(t, dict) and str(t.get("type", "")).lower() == wanted_type.lower():
            return t.get("content") or t.get("value") or ""
    return ""

def get_doi(rec: dict) -> str:
    static = rec.get("static_data", {}) or {}
    summary = static.get("summary", {}) or {}
    full_md = static.get("fullrecord_metadata", {}) or {}

    for ids_block in [summary.get("identifiers"), full_md.get("identifiers")]:
        if isinstance(ids_block, dict):
            for it in as_list(ids_block.get("identifier")):
                if not isinstance(it, dict):
                    continue
                t = str(it.get("type") or it.get("@type") or "").lower()
                if t == "doi":
                    return it.get("value") or it.get("content") or ""

    m = DOI_RE.search(str(rec))
    return m.group(0) if m else ""

def get_abstract(rec: dict) -> str:
    static = rec.get("static_data", {}) or {}
    full_md = static.get("fullrecord_metadata", {}) or {}
    abstracts = full_md.get("abstracts", {}) or {}
    abs_items = as_list(abstracts.get("abstract"))
    text = " ".join(extract_text(a) for a in abs_items).strip()
    return " ".join(text.split())

def get_funding(rec: dict):
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

    agencies = dedupe_keep_order(agencies)
    grants = dedupe_keep_order(grants)
    return fund_text, "; ".join(agencies), "; ".join(grants)

def get_keywords(rec: dict):
    static = rec.get("static_data", {}) or {}
    full_md = static.get("fullrecord_metadata", {}) or {}
    item = static.get("item", {}) or {}

    author_keywords = []
    kw_block = full_md.get("keywords", {}) or {}
    for kw in as_list(kw_block.get("keyword")):
        author_keywords.append(extract_text(kw))

    keywords_plus = []
    kp_block = item.get("keywords_plus", {}) or {}
    for kw in as_list(kp_block.get("keyword")):
        keywords_plus.append(extract_text(kw))

    author_keywords = dedupe_keep_order(author_keywords)
    keywords_plus = dedupe_keep_order(keywords_plus)

    return "; ".join(author_keywords), "; ".join(keywords_plus)

def get_categories(rec: dict):
    static = rec.get("static_data", {}) or {}
    full_md = static.get("fullrecord_metadata", {}) or {}
    cat_info = full_md.get("category_info", {}) or {}
    subjects_block = (cat_info.get("subjects") or {})

    trad, ext = [], []
    for s in as_list(subjects_block.get("subject")):
        if isinstance(s, dict):
            asc = (s.get("ascatype") or s.get("@ascatype") or "").lower()
            txt = extract_text(s.get("subject")) or extract_text(s)
            txt = (txt or "").strip()
            if not txt:
                continue
            if asc == "extended":
                ext.append(txt)
            else:
                trad.append(txt)
        else:
            txt = extract_text(s)
            if txt:
                trad.append(txt)

    trad = dedupe_keep_order(trad)
    ext = dedupe_keep_order(ext)
    return "; ".join(trad), "; ".join(ext)

def get_author_emails(rec: dict) -> str:
    static = rec.get("static_data", {}) or {}
    summary = static.get("summary", {}) or {}
    names = (summary.get("names") or {}).get("name")
    emails = []
    for n in as_list(names):
        if isinstance(n, dict):
            e = n.get("email_addr") or n.get("email") or n.get("emailAddress")
            if e:
                emails.append(extract_text(e))
    return "; ".join(dedupe_keep_order(emails))

def make_summary_row(rec: dict) -> dict:
    static = rec.get("static_data", {}) or {}
    summary = static.get("summary", {}) or {}
    pub_info = summary.get("pub_info", {}) or {}

    ut = get_unique_id(rec)
    title = pick_title(summary, "item")
    journal = pick_title(summary, "source")
    year = pub_info.get("pubyear", "")

    authors = (summary.get("names") or {}).get("name")
    authors_str = "; ".join(
        n.get("full_name", "")
        for n in as_list(authors)
        if isinstance(n, dict) and n.get("full_name")
    )

    doi = get_doi(rec)
    abstract = get_abstract(rec)
    funding_text, funding_agencies, grant_numbers = get_funding(rec)
    author_kw, kw_plus = get_keywords(rec)
    cat_trad, cat_ext = get_categories(rec)
    emails = get_author_emails(rec)

    return {
        "UT": ut,
        "Title": title,
        "Journal": journal,
        "Year": year,
        "DOI": doi,
        "Authors": authors_str,
        "AuthorEmails": emails,
        "Abstract": abstract,
        "FundingText": funding_text,
        "FundingAgencies": funding_agencies,
        "GrantNumbers": grant_numbers,
        "AuthorKeywords": author_kw,
        "KeywordsPlus": kw_plus,
        "WoSCategoriesTraditional": cat_trad,
        "WoSCategoriesExtended": cat_ext,
    }

# Main

def main():
    load_dotenv()
    api_key = os.getenv("WOS_API_KEY")
    if not api_key:
        raise ValueError("Missing API key. Put WOS_API_KEY=... in your .env file.")

    base = "https://api.clarivate.com/api/wos"
    headers = {"X-ApiKey": api_key, "Accept": "application/json"}

    ensure_dir(OUT_DIR)

    records_jsonl = OUT_DIR / "records_full.jsonl"
    debug_first_page = OUT_DIR / "debug_first_page.json"
    results_xlsx = OUT_DIR / "wos_results.xlsx"
    results_csv = OUT_DIR / "wos_results.csv"

    # fresh
    if records_jsonl.exists():
        records_jsonl.unlink()

    summary_rows = []

    with requests.Session() as session:
        session.headers.update(headers)

        # Seed query
        seed_params = {
            "databaseId": DATABASE_ID,
            "usrQuery": USR_QUERY,
            "count": 0,
            "firstRecord": 1,
            "optionView": "SR",
        }
        print("Seed query:", USR_QUERY)
        seed_json = get_with_retry(session, base, params=seed_params).json()
        query_id, total_found = get_query_id_and_total(seed_json)

        print("Total found:", total_found, "QueryID:", query_id)
        if total_found == 0:
            print("No records.")
            return

        total_to_fetch = min(total_found, MAX_RECORDS) if MAX_RECORDS else total_found
        print(f"Will fetch {total_to_fetch} record(s). PAGE_SIZE={PAGE_SIZE}")

        # Always use main endpoint (most consistent schema)
        for first in range(1, total_to_fetch + 1, PAGE_SIZE):
            batch = min(PAGE_SIZE, total_to_fetch - first + 1)

            params = {
                "databaseId": DATABASE_ID,
                "usrQuery": USR_QUERY,
                "count": batch,
                "firstRecord": first,
                "optionView": "FR",
                "links": "true",
            }

            print(f"Fetching FR records {first}-{first+batch-1} ...")
            data = get_with_retry(session, base, params=params).json()

            if SAVE_DEBUG_FIRST_PAGE and first == 1:
                debug_first_page.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                print("Saved debug:", debug_first_page)

            recs = extract_records_any(data)
            print(f"  Extracted {len(recs)} record(s) from this page.")
            if not recs:
                print("  ❌ Could not extract records. Open debug_first_page.json and search for 'REC' to see structure.")
                break

            for rec in recs:
                ut = get_unique_id(rec)
                write_jsonl(records_jsonl, {"UT": ut, "record": rec})
                summary_rows.append(make_summary_row(rec))

            time.sleep(SLEEP_BETWEEN_CALLS)

    # Save Excel + CSV summary
    df = pd.DataFrame(summary_rows)
    print("Total summary rows:", len(df))

    df.to_excel(results_xlsx, index=False)
    df.to_csv(results_csv, index=False, encoding="utf-8")

    print("\nDONE.")
    print("Full raw dump:", records_jsonl)
    print("Excel results :", results_xlsx)
    print("CSV results   :", results_csv)
    print("Output folder :", OUT_DIR.resolve())

if __name__ == "__main__":
    main()

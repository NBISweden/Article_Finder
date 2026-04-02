import time
import json
import re
import argparse
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
from wos_credentials import get_wos_api_key

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
        for k in ["p", "content", "value", "#text"]:
            if k in obj:
                return extract_text(obj[k])
        return " ".join(extract_text(v) for v in obj.values() if extract_text(v)).strip()
    return str(obj).strip()


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

def validate_date_range(start_date: str | None, end_date: str | None):
    if bool(start_date) != bool(end_date):
        raise ValueError("Provide both start_date and end_date, or neither.")

    if not start_date and not end_date:
        return

    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError("Dates must be in YYYY-MM-DD format.") from e

    if start > end:
        raise ValueError("start_date must be earlier than or equal to end_date.")

def parse_iso_date(s: str | None):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def is_within_exact_date_range(sortdate: str, start_date: str, end_date: str) -> bool:
    d = parse_iso_date(sortdate)
    if d is None:
        return False

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return start <= d <= end

def parse_json_response(resp: requests.Response) -> dict:
    content_type = resp.headers.get("Content-Type", "")
    text = resp.text or ""

    if not text.strip():
        raise RuntimeError(
            "Empty response body.\n"
            f"Status: {resp.status_code}\n"
            f"URL: {resp.request.url}\n"
            f"Content-Type: {content_type}"
        )

    try:
        return resp.json()
    except requests.exceptions.JSONDecodeError as e:
        raise RuntimeError(
            "Response was not valid JSON.\n"
            f"Status: {resp.status_code}\n"
            f"URL: {resp.request.url}\n"
            f"Content-Type: {content_type}\n"
            f"Body head:\n{text[:1200]}"
        ) from e


def get_json_with_retry(
    session: requests.Session,
    url: str,
    *,
    params=None,
    timeout=60,
    max_tries=8,
):
    last_err = None

    for tries in range(1, max_tries + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)

            if resp.status_code == 429:
                wait_s = min(5 * (2 ** (tries - 1)), 60)
                print(f"429 rate limit -> sleep {wait_s:.0f}s (try {tries}/{max_tries})")
                time.sleep(wait_s)
                continue

            if 500 <= resp.status_code < 600:
                wait_s = min(2 * tries, 20)
                print(f"{resp.status_code} server error -> sleep {wait_s:.0f}s (try {tries}/{max_tries})")
                time.sleep(wait_s)
                continue

            if 400 <= resp.status_code < 500:
                raise RuntimeError(
                    f"HTTP {resp.status_code}\n"
                    f"URL: {resp.request.url}\n"
                    f"Response head:\n{resp.text[:1200]}"
                )

            return parse_json_response(resp)

        except requests.RequestException as e:
            last_err = e
            if tries >= max_tries:
                raise
            wait_s = min(2 * tries, 20)
            print(f"Network error -> sleep {wait_s:.0f}s (try {tries}/{max_tries})")
            time.sleep(wait_s)

        except RuntimeError as e:
            last_err = e

            msg = str(e)
            is_retryable_parse_error = (
                "Empty response body" in msg or
                "Response was not valid JSON" in msg
            )

            if not is_retryable_parse_error or tries >= max_tries:
                raise

            wait_s = min(2 * tries, 20)
            print(f"Request/parse error -> sleep {wait_s:.0f}s (try {tries}/{max_tries})")
            time.sleep(wait_s)

    raise last_err

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
    for root in [
        data.get("Records", {}),
        data.get("Data", {}).get("Records", {}),
    ]:
        try:
            r = root.get("records", {})
            if isinstance(r, dict):
                rec = r.get("REC", [])
                if isinstance(rec, dict):
                    return [rec]
                if isinstance(rec, list) and rec:
                    return rec
            elif isinstance(r, list) and r:
                return r
        except Exception:
            pass

    candidates = []

    def walk(obj):
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
            val = t.get("content") or t.get("value") or ""
            return str(val).strip()
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
                    val = it.get("value") or it.get("content") or ""
                    return str(val).strip()
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

    #year = str(pub_info.get("pubyear", "")).strip()
    #pubmonth = str(pub_info.get("pubmonth", "")).strip()
    #coverdate = str(pub_info.get("coverdate", "")).strip()
    sortdate = str(pub_info.get("sortdate", "")).strip()

    authors = (summary.get("names") or {}).get("name")
    author_names = []
    for n in as_list(authors):
        if not isinstance(n, dict):
            continue
        full_name = n.get("full_name")
        if full_name is None:
            continue
        full_name = str(full_name).strip()
        if full_name:
            author_names.append(full_name)

    authors_str = "; ".join(author_names)

    doi = get_doi(rec)
    emails = get_author_emails(rec)
    abstract = get_abstract(rec)

    funding_text, funding_agencies, grant_numbers = get_funding(rec)
    author_kw, kw_plus = get_keywords(rec)
    cat_trad, cat_ext = get_categories(rec)

    return {
        "UT": ut,
        "Title": title,
        "Journal": journal,
        "SortDate": sortdate,
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

def run_fetch_query(
    usr_query: str,
    out_dir: Path,
    database_id: str = "WOS",
    page_size: int = 100,
    max_records: int | None = None,
    save_debug_first_page: bool = True,
    sleep_between_calls: float = 0.25,
    summary_csv: Path | None = None,
    start_date: str = None,
    end_date: str = None,
):
    if page_size <= 0:
        raise ValueError("page_size must be greater than 0.")
    if sleep_between_calls < 0:
        raise ValueError("sleep_between_calls must be 0 or greater.")
    if max_records is not None and max_records <= 0:
        raise ValueError("max_records must be greater than 0 when provided.")
    
    validate_date_range(start_date, end_date)

    api_key = get_wos_api_key()
    
    base = "https://api.clarivate.com/api/wos"
    headers = {"X-ApiKey": api_key, "Accept": "application/json"}
    ensure_dir(out_dir)

    records_jsonl = out_dir / "records_full.jsonl"
    debug_first_page = out_dir / "debug_first_page.json"
    if summary_csv is None:
        summary_csv = out_dir / "wos_results.csv"

    partial_csv = out_dir / "wos_results.partial.csv" 

    if records_jsonl.exists():
        records_jsonl.unlink()

    if partial_csv.exists():
        partial_csv.unlink()
    
    if debug_first_page.exists():
        debug_first_page.unlink()
    summary_rows = []
    fetched_count = 0
    seed_params = {
        "databaseId": database_id,
        "usrQuery": usr_query,
        "count": 0,
        "firstRecord": 1,
        "optionView": "SR",
    }

    if start_date and end_date:
        seed_params["publishTimeSpan"] = f"{start_date}+{end_date}"

    with requests.Session() as session:
        session.headers.update(headers)

        print(f"Seed query: {usr_query}")
        if "publishTimeSpan" in seed_params:
            print(f"Time span: {seed_params['publishTimeSpan']}")
        
        seed_json = get_json_with_retry(session, base, params=seed_params)
        query_id, total_found = get_query_id_and_total(seed_json)

        print(f"Total found: {total_found}")
        if total_found == 0:
            pd.DataFrame().to_csv(summary_csv, index=False, encoding="utf-8")
            return {"out_dir": out_dir, "summary_csv": summary_csv}

        if not query_id:
            raise RuntimeError("Seed response did not include a query_id / QueryID.")

        total_to_fetch = min(total_found, max_records) if max_records else total_found
        print(f"Fetching {total_to_fetch} record(s) using Full Record (FR) view...")

        query_url = f"{base}/query/{query_id}"

        for first in range(1, total_to_fetch + 1, page_size):
            batch = min(page_size, total_to_fetch - first + 1)

            params = {
                "count": batch,
                "firstRecord": first,
                "optionView": "FR",
                "links": "true",
            }

            data = get_json_with_retry(session, query_url, params=params)
            
            if save_debug_first_page and first == 1:
                debug_first_page.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

            recs = extract_records_any(data)
            if not recs:
                raise RuntimeError(
                    f"No records extracted on page starting at firstRecord={first}. "
                    f"Check {debug_first_page} for schema."
                )

            for rec in recs:
                fetched_count += 1
                row = make_summary_row(rec)

                if start_date and end_date:
                    if not is_within_exact_date_range(row["SortDate"], start_date, end_date):
                        continue

                ut = get_unique_id(rec)
                write_jsonl(records_jsonl, {"UT": ut, "record": rec})
                summary_rows.append(row)

            pd.DataFrame(summary_rows).to_csv(partial_csv, index=False, encoding="utf-8")
            print(f" Fetched: {fetched_count}/{total_to_fetch} | Kept: {len(summary_rows)}", flush=True)
            time.sleep(sleep_between_calls)

    df = pd.DataFrame(summary_rows)
    df.to_csv(summary_csv, index=False, encoding="utf-8")

    try:
        partial_csv.unlink(missing_ok=True)
    except PermissionError:
        print(f"Could not remove partial file because it is open: {partial_csv}")
    except OSError as e:
        print(f"Could not remove partial file {partial_csv}: {e}")

    print(f"Finished. Results saved to {summary_csv}")
    return {"summary_csv": summary_csv}



def build_argparser():
    ap = argparse.ArgumentParser()
    ap.add_argument("--usr-query", required=True)
    ap.add_argument("--out-dir", default="WOS_fetched_results")
    ap.add_argument("--database-id", default="WOS")
    ap.add_argument("--page-size", type=int, default=100)
    ap.add_argument("--max-records", type=int, default=0)
    ap.add_argument("--sleep", type=float, default=0.25)
    ap.add_argument("--no-debug-first-page", action="store_true")
    ap.add_argument("--summary-csv", default="")
    ap.add_argument("--start-date", default=None)  # YYYY-MM-DD
    ap.add_argument("--end-date", default=None)    
    return ap


def main():
    args = build_argparser().parse_args()
    out_dir = Path(args.out_dir)
    max_records = None if args.max_records == 0 else args.max_records
    summary_csv = Path(args.summary_csv) if args.summary_csv else None

    run_fetch_query(
        usr_query=args.usr_query,
        out_dir=out_dir,
        database_id=args.database_id,
        page_size=args.page_size,
        max_records=max_records,
        save_debug_first_page=not args.no_debug_first_page,
        sleep_between_calls=args.sleep,
        summary_csv=summary_csv,
        start_date=args.start_date,
        end_date=args.end_date,
    )


if __name__ == "__main__":
    main()
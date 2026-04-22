import argparse
import re
from pathlib import Path
import multiprocessing
import numpy as np 
from rapidfuzz import fuzz 

import pandas as pd
import yaml


def dedupe_keep_order(items):
    seen = set()
    out = []
    for x in items:
        x = (x or "").strip()
        if not x:
            continue
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def load_keywords(keyword_file: str):
    with open(keyword_file, "r", encoding="utf-8") as f:
        kw = yaml.safe_load(f) or {}

    include_terms = kw.get("include_terms", []) or []
    exclude_terms = kw.get("exclude_terms", []) or []
    exclude_terms_category = kw.get("exclude_terms_category", []) or []
    include_terms_category = kw.get("include_terms_category", []) or []

    if not (include_terms or exclude_terms or include_terms_category or exclude_terms_category):
        raise ValueError(
            "keyword.yml has no rules. Provide at least one of: "
            "include_terms, exclude_terms, include_terms_category, exclude_terms_category."
        )

    if exclude_terms_category:
        exclude_category_regex = re.compile("|".join(map(re.escape, exclude_terms_category)), re.IGNORECASE)
    else:
        exclude_category_regex = re.compile(r"a^")

    if include_terms_category:
        include_category_regex = re.compile("|".join(map(re.escape, include_terms_category)), re.IGNORECASE)
    else:
        include_category_regex = re.compile(r"a^")

    normal_terms = [t for t in include_terms if t.lower() != "score" and t != "SCoRe"]

    if normal_terms:
        normal_include_regex = re.compile(
            r"(?<!\w)(" + "|".join(map(re.escape, normal_terms)) + r")(?!\w)",
            re.IGNORECASE
        )
    else:
        normal_include_regex = re.compile(r"a^")

    score_regex = re.compile(r"(?<!\w)SCoRe(?!\w)") if "SCoRe" in include_terms else re.compile(r"a^")

    if exclude_terms:
        exclude_regex = re.compile("|".join(map(re.escape, exclude_terms)), re.IGNORECASE)
    else:
        exclude_regex = re.compile(r"a^")

    return (
        include_terms,
        exclude_terms,
        exclude_terms_category,
        include_terms_category,
        normal_include_regex,
        score_regex,
        exclude_regex,
        exclude_category_regex,
        include_category_regex,
    )

def extract_include_sentence(
    text: str,
    normal_include_regex: re.Pattern,
    score_regex: re.Pattern,
    include_terms: list[str] | None = None,
    use_fuzzy: bool = True,
    fuzzy_threshold: int = 95,
):
    if not isinstance(text, str):
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    for s in sentences:
        if score_regex.search(s) or normal_include_regex.search(s):
            return s.strip()
        if use_fuzzy and include_terms and fuzzy_include_match(s, include_terms, threshold=fuzzy_threshold):
            return s.strip()
    return ""


def normalize_whitespace(text: str) -> str:
    if not isinstance(text, str):
        return ""
    return re.sub(r"\s+", " ", text).strip()


def norm_simple(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace(".", " ")
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def fuzzy_best_phrase(text: str, include_terms: list[str], threshold: int = 95) -> str:
    if not isinstance(text, str): return ""
    text_norm = norm_simple(text)
    if not text_norm: return ""

    text_tokens = text_norm.split()
    best_phrase = ""
    best_score = -1.0

    for term in include_terms:
        term_norm = norm_simple(term)
        if not term_norm: continue

        if term_norm in text_norm:
            return term

        term_tokens = term_norm.split()
        n = len(term_tokens)
        min_window = max(1, n - 1)
        max_window = min(len(text_tokens), n + 1)

        for w in range(min_window, max_window + 1):
            for i in range(len(text_tokens) - w + 1):
                cand = " ".join(text_tokens[i:i + w])
                score = fuzz.ratio(term_norm, cand)
                if score > best_score:
                    best_score = score
                    best_phrase = term
                if score >= threshold:
                    return term

    return best_phrase if best_score >= threshold else ""

def process_chunk(chunk_data, normal_include_regex, score_regex, include_terms, fuzzy_threshold):
    results = [
        analyze_include_match(t, normal_include_regex, score_regex, include_terms, fuzzy_threshold)
        for t in chunk_data
    ]
    
    return pd.Series(results, index=chunk_data.index)

def process_chunk_wrapper(args):
    return process_chunk(*args)

def fuzzy_include_match(text: str, include_terms: list[str], threshold: int = 95) -> bool:
    return bool(fuzzy_best_phrase(text, include_terms, threshold=threshold))

def analyze_include_match(
    text: str,
    normal_include_regex: re.Pattern,
    score_regex: re.Pattern,
    include_terms: list[str] | None = None,
    fuzzy_threshold: int = 95,
):
    if not isinstance(text, str) or not text.strip():
        return False, "", ""

    text = normalize_whitespace(text)

    m = score_regex.search(text)
    if m:
        matched_term = m.group(0)
        matched_sentence = extract_include_sentence(text, normal_include_regex, score_regex, use_fuzzy=False)
        return True, matched_term, matched_sentence

    m2 = normal_include_regex.search(text)
    if m2:
        matched_term = m2.group(0)
        matched_sentence = extract_include_sentence(text, normal_include_regex, score_regex, use_fuzzy=False)
        return True, matched_term, matched_sentence

    if include_terms:
        matched_term = fuzzy_best_phrase(text, include_terms, threshold=fuzzy_threshold)
        if matched_term:
            sentences = re.split(r"(?<=[.!?])\s+", text)
            for s in sentences:
                if fuzzy_include_match(s, [matched_term], threshold=fuzzy_threshold):
                    return True, matched_term, s.strip()

    return False, "", ""

def contributor_lastname_candidates(fullname: str, max_words: int = 4) -> set[str]:
    toks = [t for t in re.split(r"\s+", fullname.strip()) if t]
    if len(toks) < 2:
        return set()

    rest = toks[1:]
    cands = set()
    for k in range(1, min(max_words, len(rest)) + 1):
        cands.add(norm_simple(" ".join(rest[-k:])))
    return cands


def parse_author_token(author_token: str):
    tok = (author_token or "").strip()
    if not tok:
        return ("", "", "")

    s = norm_simple(tok)

    if "," in s:
        last, first = [x.strip() for x in s.split(",", 1)]
        first_parts = [p for p in first.split() if p]
        firstword = first_parts[0] if first_parts else ""
        if len(firstword) == 1:
            return (last, "", firstword)
        return (last, firstword, firstword[:1] if firstword else "")

    parts = [p for p in s.split() if p]
    if len(parts) == 1:
        return (parts[0], "", "")
    if len(parts) >= 2:
        if len(parts[-1]) == 1:
            return (parts[-2], "", parts[-1])
        return (parts[-1], parts[0], parts[0][:1])

    return ("", "", "")


def make_flexible_name_pattern(name: str) -> str:
    parts = [re.escape(p) for p in re.split(r"\s+", name.strip()) if p]
    return r"(?<!\w)" + r"[\s\-]+".join(parts) + r"(?!\w)"


def build_ack_patterns(fullname: str, max_words: int = 4):
    fullname = (fullname or "").strip()
    toks = [t for t in re.split(r"\s+", fullname) if t]
    if len(toks) < 2:
        return "", []

    first = toks[0]
    first_initial = re.escape(first[:1])

    full_name_pattern = make_flexible_name_pattern(fullname)

    initial_last_patterns = []
    for ln in contributor_lastname_candidates(fullname, max_words=max_words):
        ln_parts = [re.escape(x) for x in re.split(r"\s+", ln) if x]
        ln_pat = r"[\s\-]+".join(ln_parts)

        initial_last_patterns.append(
            r"(?<!\w)" + first_initial + r"\.?\s+" + ln_pat + r"(?!\w)"
        )

    return full_name_pattern, initial_last_patterns


def run_filter(
    wos_csv: str,
    keyword_file: str | None,
    out_filtered: str,
    Contributor_csv: str | None = None,
    out_Contributor_checked: str | None = None,
    out_merged: str | None = None,
    do_keyword_filter: bool = True,
    use_fuzzy: bool = True,
    fuzzy_threshold: int = 95,
):
    df = pd.read_csv(wos_csv, sep=",")
    df = df.reset_index(drop=True)
    df = df.dropna(axis=1, how="all")
    df = df.loc[:, (df != "").any(axis=0)]

    category_cols = [c for c in df.columns if re.search(r"categor", c, re.IGNORECASE)]

    text_cols = [
        c for c in df.columns
        if re.search(r"title|abstract|fund|keyword|ack", c, re.IGNORECASE)
        and c not in category_cols
    ]

    df["fulltext"] = df[text_cols].fillna("").astype(str).agg(" ".join, axis=1).apply(normalize_whitespace)

    if category_cols:
        df["category_text"] = df[category_cols].fillna("").astype(str).agg(" ".join, axis=1).apply(normalize_whitespace)
    else:
        df["category_text"] = ""

    df["matched_term"] = ""
    df["matched_sentence"] = ""

    keyword_filtered = None
    keyword_filtered_path = None
    Contributor_checked = None
    Contributor_checked_path = None
    merged_path = None

    if keyword_file and str(keyword_file).strip():
        (
            include_terms,
            exclude_terms,
            exclude_terms_category,
            include_terms_category,
            NORMAL_INCLUDE_REGEX,
            SCORE_REGEX,
            EXCLUDE_REGEX,
            EXCLUDE_CATEGORY_REGEX,
            INCLUDE_CATEGORY_REGEX,
        ) = load_keywords(keyword_file)

        df["exclude_match"] = df["fulltext"].str.contains(EXCLUDE_REGEX, na=False)
        df["exclude_category_match"] = df["category_text"].str.contains(EXCLUDE_CATEGORY_REGEX, na=False)

        if do_keyword_filter:
            print(f"Starting Keyword Filter with Parallel Processing...")
            
            num_cores = max(1, multiprocessing.cpu_count() - 1)
            num_chunks = num_cores * 4

            n = len(df)
            chunk_size = (n // num_chunks) + 1
            chunks = [df["fulltext"].iloc[i : i + chunk_size] for i in range(0, n, chunk_size)]

            tasks = [
                (chunk, NORMAL_INCLUDE_REGEX, SCORE_REGEX, include_terms, fuzzy_threshold) 
                for chunk in chunks
            ]
            
            results_list = []
            with multiprocessing.Pool(num_cores) as pool:
                for i, result_chunk in enumerate(pool.imap(process_chunk_wrapper, tasks), 1):
                    results_list.append(result_chunk)
                    percent = (i / len(chunks)) * 100
                    print(f"Filter Progress: {percent:.1f}% ({i}/{len(chunks)} completed)", flush=True)

            results = pd.concat(results_list).sort_index()

            df["include_match"] = [x[0] for x in results]
            df["matched_term"] = [x[1] for x in results]
            df["matched_sentence"] = [x[2] for x in results]
            df["include_category_match"] = df["category_text"].str.contains(INCLUDE_CATEGORY_REGEX, na=False)

            include_only = df[df["include_match"]]

            if include_terms_category:
                keep_cat = df["include_category_match"]
            else:
                keep_cat = True

            keyword_filtered = df[
                df["include_match"] & keep_cat & ~df["exclude_match"] & ~df["exclude_category_match"]
            ].copy()

            if include_terms:
                print(f"Total INCLUDE matches: {len(include_only)}")
            else:
                print(f"Total INCLUDE matches: {len(include_only)} (include_terms empty -> include step disabled)")
            print(f"Excluded due to EXCLUSION terms (main text): {int(df['exclude_match'].sum())}")
            print(f"Excluded due to CATEGORY exclusions: {int(df['exclude_category_match'].sum())}")
            if include_terms_category:
                print(f"Included due to CATEGORY inclusion: {int(df['include_category_match'].sum())}")
            else:
                print("Included due to CATEGORY inclusion: N/A (include_terms_category empty -> category include disabled)")
            print(f"KEYWORD kept: {len(keyword_filtered)}")

            Path(out_filtered).parent.mkdir(parents=True, exist_ok=True)
            keyword_filtered.to_csv(out_filtered, index=False)
            keyword_filtered_path = out_filtered
        else:
            df["include_match"] = False
            df["include_category_match"] = False
            include_only = pd.DataFrame(columns=df.columns)

            print("Keyword include filter skipped; exclusion rules still applied")
            print(f"Excluded due to EXCLUSION terms (main text): {int(df['exclude_match'].sum())}")
            print(f"Excluded due to CATEGORY exclusions: {int(df['exclude_category_match'].sum())}")

        df_exclude = df[~df["exclude_match"] & ~df["exclude_category_match"]].copy()

    else:
        df["include_match"] = False
        df["exclude_match"] = False
        df["exclude_category_match"] = False
        df["include_category_match"] = False
        include_only = pd.DataFrame(columns=df.columns)
        df_exclude = df.copy()

        print("Keyword rules skipped")

    if Contributor_csv and str(Contributor_csv).strip() and Path(Contributor_csv).is_file():
        df_Contributor = df_exclude.copy()

        Contributor_df = pd.read_csv(Contributor_csv, sep=";")
        if "Name" not in Contributor_df.columns:
            raise ValueError('Contributor file must contain a column named "Name".')

        Contributor_names = (
            Contributor_df["Name"]
            .dropna()
            .astype(str)
            .str.strip()
        )
        Contributor_names = [n for n in Contributor_names if n]
        Contributor_names = sorted(set(Contributor_names), key=len, reverse=True)

        def get_lastname(name: str) -> str:
            parts = re.split(r"\s+", name.strip())
            return parts[-1] if parts else ""

        Contributor_lastnames = sorted(
            set(get_lastname(n) for n in Contributor_names if get_lastname(n)),
            key=len,
            reverse=True
        )

        contributor_full_lookup = {}
        contributor_initial_lookup = {}

        for full in Contributor_names:
            full = (full or "").strip()
            if not full:
                continue

            toks = [t for t in re.split(r"\s+", full) if t]
            if len(toks) < 2:
                continue

            contributor_first = norm_simple(toks[0])
            contributor_fi = contributor_first[:1] if contributor_first else ""

            for ln in contributor_lastname_candidates(full, max_words=4):
                if not ln:
                    continue
                contributor_full_lookup.setdefault((ln, contributor_first), set()).add(full)
                if contributor_fi:
                    contributor_initial_lookup.setdefault((ln, contributor_fi), set()).add(full)

        def find_PI_staff_in_authors(authors_text: str):
            if not isinstance(authors_text, str) or not authors_text.strip():
                return ("", "")

            author_items = [a.strip() for a in authors_text.split(";") if a.strip()]
            list_hits = []
            paper_hits = []

            for a in author_items:
                a_last, a_firstword, a_fi = parse_author_token(a)
                if not a_last:
                    continue

                matched = set()

                if a_firstword:
                    matched = contributor_full_lookup.get((a_last, a_firstword), set())
                elif a_fi:
                    matched = contributor_initial_lookup.get((a_last, a_fi), set())

                if matched:
                    list_hits.extend(sorted(matched))
                    paper_hits.append(a.strip())

            list_hits = dedupe_keep_order(list_hits)
            paper_hits = dedupe_keep_order(paper_hits)

            return ("; ".join(list_hits), "; ".join(paper_hits))

        if "Authors" in df_Contributor.columns:
            author_matches = df_Contributor["Authors"].fillna("").astype(str).apply(find_PI_staff_in_authors)
            df_Contributor["PI_staff_names_in_authors_list"] = author_matches.apply(lambda x: x[0])
            df_Contributor["PI_staff_names_in_authors_paper"] = author_matches.apply(lambda x: x[1])
            df_Contributor["PI_staff_in_authors"] = df_Contributor["PI_staff_names_in_authors_list"].str.len() > 0
        else:
            df_Contributor["PI_staff_names_in_authors_list"] = ""
            df_Contributor["PI_staff_names_in_authors_paper"] = ""
            df_Contributor["PI_staff_in_authors"] = False

        contributor_ack_patterns = {}
        for full in Contributor_names:
            full_pat, initial_pats = build_ack_patterns(full, max_words=4)
            contributor_ack_patterns[full] = {
                "full": re.compile(full_pat, re.IGNORECASE) if full_pat else None,
                "initials": [re.compile(p, re.IGNORECASE) for p in initial_pats],
            }

        def find_PI_staff_in_ack(text: str):
            if not isinstance(text, str) or not text.strip():
                return ("", "")

            list_hits = []
            paper_hits = []

            for full in Contributor_names:
                pats = contributor_ack_patterns.get(full, {})
                full_pat = pats.get("full")
                initial_pats = pats.get("initials", [])

                found = False

                if full_pat:
                    m = full_pat.search(text)
                    if m:
                        list_hits.append(full)
                        paper_hits.append(m.group(0))
                        found = True

                if not found:
                    for p in initial_pats:
                        m = p.search(text)
                        if m:
                            list_hits.append(full)
                            paper_hits.append(m.group(0))
                            break

            list_hits = dedupe_keep_order(list_hits)
            paper_hits = dedupe_keep_order(paper_hits)

            return ("; ".join(list_hits), "; ".join(paper_hits))

        fund_cols_candidates = ["FundingText", "FundingAgencies", "GrantNumbers"]
        fund_cols = [c for c in fund_cols_candidates if c in df_Contributor.columns]
        ack_cols = [c for c in df_Contributor.columns if re.search(r"ack", c, re.IGNORECASE)]

        ack_fund_cols = []
        for c in fund_cols + ack_cols:
            if c not in ack_fund_cols:
                ack_fund_cols.append(c)

        if ack_fund_cols:
            ack_fund_search_text = df_Contributor[ack_fund_cols].fillna("").astype(str).agg(" ".join, axis=1).apply(normalize_whitespace)
        else:
            ack_fund_search_text = pd.Series("", index=df_Contributor.index)

        ack_matches = ack_fund_search_text.apply(find_PI_staff_in_ack)
        df_Contributor["PI_staff_names_in_ack_list"] = ack_matches.apply(lambda x: x[0])
        df_Contributor["PI_staff_names_in_ack_paper"] = ack_matches.apply(lambda x: x[1])
        df_Contributor["PI_staff_in_ack"] = df_Contributor["PI_staff_names_in_ack_list"].str.len() > 0

        if out_Contributor_checked is None:
            out_Contributor_checked = "Contributor_names_checked.csv"

        Path(out_Contributor_checked).parent.mkdir(parents=True, exist_ok=True)

        keep_mask = df_Contributor["PI_staff_in_ack"] | df_Contributor["PI_staff_in_authors"]

        output_cols = list(df_Contributor.columns)
        preferred_cols = [
            "PI_staff_in_ack",
            "PI_staff_names_in_ack_paper",
            "PI_staff_names_in_ack_list",
            "PI_staff_in_authors",
            "PI_staff_names_in_authors_paper",
            "PI_staff_names_in_authors_list",
        ]

        other_cols = [c for c in output_cols if c not in preferred_cols]
        final_cols = other_cols + preferred_cols

        Contributor_checked = df_Contributor.loc[keep_mask, final_cols].copy()
        Contributor_checked.to_csv(out_Contributor_checked, index=False)
        Contributor_checked_path = out_Contributor_checked

        if out_merged and str(out_merged).strip():
            kw_part = keyword_filtered.copy() if keyword_filtered is not None else pd.DataFrame()
            name_part = Contributor_checked.copy() if Contributor_checked is not None else pd.DataFrame()

            if not kw_part.empty:
                kw_part["matched_by_keyword"] = True
                kw_part["matched_by_name"] = False

            if not name_part.empty:
                name_part["matched_by_keyword"] = False
                name_part["matched_by_name"] = True

            if not kw_part.empty and not name_part.empty:
                if "UT" in kw_part.columns and "UT" in name_part.columns:
                    kw_base = kw_part.drop_duplicates(subset=["UT"], keep="first")
                    name_base = name_part.drop_duplicates(subset=["UT"], keep="first")

                    kw_ids = set(kw_base["UT"].astype(str))
                    name_ids = set(name_base["UT"].astype(str))

                    merged = kw_base.set_index("UT").combine_first(name_base.set_index("UT")).reset_index()
                    merged["matched_by_keyword"] = merged["UT"].astype(str).isin(kw_ids)
                    merged["matched_by_name"] = merged["UT"].astype(str).isin(name_ids)
                else:
                    merged = pd.concat([kw_part, name_part], ignore_index=True, sort=False)
                    dedupe_cols = [c for c in ["DOI", "Title"] if c in merged.columns]
                    if dedupe_cols:
                        merged = merged.drop_duplicates(subset=dedupe_cols, keep="first")
                    else:
                        merged = merged.drop_duplicates()

            elif not kw_part.empty:
                merged = kw_part.copy()

            elif not name_part.empty:
                merged = name_part.copy()

            else:
                merged = pd.DataFrame()

            Path(out_merged).parent.mkdir(parents=True, exist_ok=True)
            merged.to_csv(out_merged, index=False)
            merged_path = out_merged

            print(f"MERGED kept: {len(merged)}")

        print(f"PI/staff matches in ack: {int(df_Contributor['PI_staff_in_ack'].sum())}")
        print(f"PI/staff matches in authors: {int(df_Contributor['PI_staff_in_authors'].sum())}")

    else:
        if out_merged and str(out_merged).strip() and keyword_filtered is not None:
            merged = keyword_filtered.copy()
            merged["matched_by_keyword"] = True
            merged["matched_by_name"] = False
            Path(out_merged).parent.mkdir(parents=True, exist_ok=True)
            merged.to_csv(out_merged, index=False)
            merged_path = out_merged
            print(f"MERGED kept: {len(merged)}")

        print("PI/staff name check skipped")

    return {
        "out_filtered": keyword_filtered_path,
        "out_Contributor_checked": Contributor_checked_path,
        "out_merged": merged_path,
        "counts": {
            "include_only": int(len(include_only)),
            "excluded_main": int(df["exclude_match"].sum()) if "exclude_match" in df.columns else 0,
            "excluded_category": int(df["exclude_category_match"].sum()) if "exclude_category_match" in df.columns else 0,
            "keyword_kept": int(len(keyword_filtered)) if keyword_filtered is not None else 0,
            "name_kept": int(len(Contributor_checked)) if Contributor_checked is not None else 0,
        }
    }


def build_argparser():
    ap = argparse.ArgumentParser(description="Filter WoS CSV using keyword.yml (+ optional Contributor funding/authors/acknowledgment name check).")
    ap.add_argument("--wos", required=True, help="Input WoS summary CSV (comma-separated)")
    ap.add_argument("--keywords", default="", help="Optional path to keyword.yml")
    ap.add_argument("--no-keyword-filter", action="store_true", help="Disable keyword include filtering")
    ap.add_argument("--out", required=True, help="Filtered CSV output path")
    ap.add_argument("--Contributor", default="", help='Optional Contributor list CSV (separator=";") with column Name')
    ap.add_argument("--out-Contributor-checked", default="", help="Optional Contributor checked output CSV path")
    ap.add_argument("--out-merged", default="", help="Optional merged output CSV path")
    ap.add_argument("--fuzzy-threshold", type=int, default=95, help="Fuzzy threshold for include terms, recommended 92-95")
    return ap


def main():
    args = build_argparser().parse_args()

    Contributor = args.Contributor.strip() if args.Contributor else None
    out_Contributor_checked = args.out_Contributor_checked.strip() if args.out_Contributor_checked else None
    keywords = args.keywords.strip() if args.keywords else None
    out_merged = args.out_merged.strip() if args.out_merged else None

    run_filter(
        wos_csv=args.wos,
        keyword_file=keywords,
        out_filtered=args.out,
        Contributor_csv=Contributor,
        out_Contributor_checked=out_Contributor_checked,
        out_merged=out_merged,
        do_keyword_filter=not args.no_keyword_filter,
        use_fuzzy=True,
        fuzzy_threshold=args.fuzzy_threshold,
    )


if __name__ == "__main__":
    main()
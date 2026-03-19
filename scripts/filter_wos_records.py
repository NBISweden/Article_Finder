import argparse
import re
from pathlib import Path

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


def include_match(text: str, normal_include_regex: re.Pattern, score_regex: re.Pattern) -> bool:

    if normal_include_regex.pattern == r"a^" and score_regex.pattern == r"a^":
        return True

    if not isinstance(text, str):
        return False
    return bool(score_regex.search(text) or normal_include_regex.search(text))


def extract_include_phrase(text: str, normal_include_regex: re.Pattern, score_regex: re.Pattern):
    if not isinstance(text, str):
        return ""
    m = score_regex.search(text)
    if m:
        return m.group(0)
    m2 = normal_include_regex.search(text)
    return m2.group(0) if m2 else ""


def extract_include_sentence(text: str, normal_include_regex: re.Pattern, score_regex: re.Pattern):
    if not isinstance(text, str):
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    for s in sentences:
        if score_regex.search(s) or normal_include_regex.search(s):
            return s.strip()
    return ""

def norm_simple(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace(".", " ")
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


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
    keyword_file: str,
    out_filtered: str,
    Contributor_csv: str | None = None,
    out_Contributor_checked: str | None = None,
):

    include_terms, exclude_terms, exclude_terms_category, include_terms_category, NORMAL_INCLUDE_REGEX, SCORE_REGEX, EXCLUDE_REGEX, EXCLUDE_CATEGORY_REGEX, INCLUDE_CATEGORY_REGEX = load_keywords(keyword_file)

    df = pd.read_csv(wos_csv, sep=",")
    df = df.dropna(axis=1, how="all")
    df = df.loc[:, (df != "").any(axis=0)]

    category_cols = [c for c in df.columns if re.search(r"categor", c, re.IGNORECASE)]

    text_cols = [
        c for c in df.columns
        if re.search(r"title|abstract|fund|keyword|ack", c, re.IGNORECASE)
        and c not in category_cols
    ]

    df["fulltext"] = df[text_cols].fillna("").astype(str).agg(" ".join, axis=1)

    if category_cols:
        df["category_text"] = df[category_cols].fillna("").astype(str).agg(" ".join, axis=1)
    else:
        df["category_text"] = ""

    df["include_match"] = df["fulltext"].apply(lambda t: include_match(t, NORMAL_INCLUDE_REGEX, SCORE_REGEX))
    df["exclude_match"] = df["fulltext"].str.contains(EXCLUDE_REGEX, na=False)
    df["exclude_category_match"] = df["category_text"].str.contains(EXCLUDE_CATEGORY_REGEX, na=False)
    df["include_category_match"] = df["category_text"].str.contains(INCLUDE_CATEGORY_REGEX, na=False)

    df["matched_term"] = ""
    df["matched_sentence"] = ""

    inc_mask = df["include_match"]
    df.loc[inc_mask, "matched_term"] = df.loc[inc_mask, "fulltext"].apply(
        lambda t: extract_include_phrase(t, NORMAL_INCLUDE_REGEX, SCORE_REGEX)
    )
    df.loc[inc_mask, "matched_sentence"] = df.loc[inc_mask, "fulltext"].apply(
        lambda t: extract_include_sentence(t, NORMAL_INCLUDE_REGEX, SCORE_REGEX)
    )

    include_only = df[df["include_match"]]

    if include_terms_category:
        keep_cat = df["include_category_match"]
    else:
        keep_cat = True

    filtered = df[df["include_match"] & keep_cat & ~df["exclude_match"] & ~df["exclude_category_match"]]

    df_exclude = df[~df["exclude_match"] & ~df["exclude_category_match"]].copy()

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
    print(f"FINAL kept: {len(filtered)}")

    Path(out_filtered).parent.mkdir(parents=True, exist_ok=True)
    filtered.to_csv(out_filtered, index=False)

    Contributor_checked_path = None

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

        if Contributor_lastnames:
            LASTNAME_REGEX = re.compile(
                r"(?<!\w)(?:" + "|".join(map(re.escape, Contributor_lastnames)) + r")(?!\w)",
                re.IGNORECASE
            )
        else:
            LASTNAME_REGEX = re.compile(r"a^")

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
            ack_fund_search_text = df_Contributor[ack_fund_cols].fillna("").astype(str).agg(" ".join, axis=1)
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

        print(f"PI/staff matches in ack: {int(df_Contributor['PI_staff_in_ack'].sum())}")
        print(f"PI/staff matches in authors: {int(df_Contributor['PI_staff_in_authors'].sum())}")
        
    else:
        print("PI/staff name check skipped")

    return {
        "out_filtered": out_filtered,
        "out_Contributor_checked": Contributor_checked_path,
        "counts": {
            "include_only": int(len(include_only)),
            "excluded_main": int(df["exclude_match"].sum()),
            "excluded_category": int(df["exclude_category_match"].sum()),
            "final_kept": int(len(filtered)),
        }
    }


def build_argparser():
    ap = argparse.ArgumentParser(description="Filter WoS CSV using keyword.yml (+ optional Contributor funding/authors/acknowledgment name check).")
    ap.add_argument("--wos", required=True, help="Input WoS summary CSV (comma-separated)")
    ap.add_argument("--keywords", required=True, help="Path to keyword.yml")
    ap.add_argument("--out", required=True, help="Filtered CSV output path")
    ap.add_argument("--Contributor", default="", help='Optional Contributor list CSV (separator=";") with column Name')
    ap.add_argument("--out-Contributor-checked", default="", help="Optional Contributor checked output CSV path")
    return ap


def main():
    args = build_argparser().parse_args()

    Contributor = args.Contributor.strip() if args.Contributor else None
    out_Contributor_checked = args.out_Contributor_checked.strip() if args.out_Contributor_checked else None

    run_filter(
        wos_csv=args.wos,
        keyword_file=args.keywords,
        out_filtered=args.out,
        Contributor_csv=Contributor,
        out_Contributor_checked=out_Contributor_checked,
    )


if __name__ == "__main__":
    main()
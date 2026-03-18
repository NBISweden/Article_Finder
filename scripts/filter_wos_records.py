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
    #print(f"\nSaved keyword_filtered file:\n- {out_filtered}\n")

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

        Contributor_lastnames = sorted(set(get_lastname(n) for n in Contributor_names if get_lastname(n)), key=len, reverse=True)

        if Contributor_lastnames:
            LASTNAME_REGEX = re.compile(
                r"(?<!\w)(?:" + "|".join(map(re.escape, Contributor_lastnames)) + r")(?!\w)",
                re.IGNORECASE
            )
        else:
            LASTNAME_REGEX = re.compile(r"a^")

        def name_to_pattern(name: str) -> str:
            parts = re.split(r"\s+", name.strip())
            return r"(?<!\w)" + r"[\s\-]+".join(map(re.escape, parts)) + r"(?!\w)"

        if Contributor_names:
            Contributor_REGEX = re.compile("|".join(name_to_pattern(n) for n in Contributor_names), re.IGNORECASE)
        else:
            Contributor_REGEX = re.compile(r"a^")

        def find_Contributor_names_full(text: str):
            if not isinstance(text, str) or not text.strip():
                return ""
            hits = Contributor_REGEX.findall(text)
            hits = dedupe_keep_order([h.strip() for h in hits])
            return "; ".join(hits)

        fund_cols_candidates = ["FundingText", "FundingAgencies", "GrantNumbers"]
        fund_cols = [c for c in fund_cols_candidates if c in df_Contributor.columns]

        if fund_cols:
            df_Contributor["funding_search_text"] = df_Contributor[fund_cols].fillna("").astype(str).agg(" ".join, axis=1)
        else:
            df_Contributor["funding_search_text"] = ""

        df_Contributor["Contributor_names_in_funding"] = ""
        cand_fund = df_Contributor["funding_search_text"].str.contains(LASTNAME_REGEX, na=False)
        df_Contributor.loc[cand_fund, "Contributor_names_in_funding"] = df_Contributor.loc[cand_fund, "funding_search_text"].apply(find_Contributor_names_full)
        df_Contributor["Contributor_in_funding"] = df_Contributor["Contributor_names_in_funding"].str.len() > 0

        if out_Contributor_checked is None:
            out_Contributor_checked = "Contributor_names_checked.csv"

        Path(out_Contributor_checked).parent.mkdir(parents=True, exist_ok=True)
        Contributor_checked = df_Contributor[df_Contributor["Contributor_in_funding"]]
        Contributor_checked.to_csv(out_Contributor_checked, index=False)
        Contributor_checked_path = out_Contributor_checked

        #print(f"Saved Contributor_checked file:\n- {out_Contributor_checked}\n")
    else:
        print("Contributor check skipped")

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
    ap = argparse.ArgumentParser(description="Filter WoS CSV using keyword.yml (+ optional Contributor funding-name check).")
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
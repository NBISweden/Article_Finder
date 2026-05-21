import argparse
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process


DOI_RE = re.compile(r"\b10\.\d{4,9}/[-._;()/:A-Z0-9]+\b", re.I)
SUPPORTED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".tsv"}

META_COLS = {"manual_source_file", "manual_excel_row"}

ROLE_PHRASES = {
    "doi": [
        "doi",
        "doi link",
        "doi url",
        "digital object identifier",
        "persistent link",
        "url",
        "link",
    ],
    "title": [
        "title",
        "titel",
        "article title",
        "paper title",
        "publication title",
        "manuscript title",
        "research output title",
        "name of publication",
        "paper name",
        "work title",
    ],
    "year": [
        "year",
        "publication year",
        "published year",
        "publication date",
        "date",
        "publikationsår",
    ],
}

MIN_SCORE = {
    "doi": 0.20,
    "title": 0.35,
    "year": 0.35,
}


def clean_text(x: Any) -> str:
    if x is None or pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).replace("\u00a0", " ")).strip()


def normalize_title(x: Any) -> str:
    x = unicodedata.normalize("NFKD", clean_text(x).lower())
    x = "".join(ch for ch in x if not unicodedata.combining(ch))
    x = re.sub(r"[^a-z0-9]+", " ", x)
    return re.sub(r"\s+", " ", x).strip()


ROLE_PHRASES_NORM = {
    role: [normalize_title(x) for x in phrases]
    for role, phrases in ROLE_PHRASES.items()
}


def normalize_doi(x: Any) -> str:
    x = clean_text(x)
    if not x:
        return ""

    x = re.sub(r"^https?://(dx\.)?doi\.org/", "", x, flags=re.I)
    x = re.sub(r"^doi:\s*", "", x, flags=re.I)
    x = x.strip("\"'“”’` ")
    x = x.rstrip(" .;,)\t\r\n]}>")
    return x.lower().strip()


def extract_doi_from_text(x: Any) -> str:
    m = DOI_RE.search(clean_text(x))
    return normalize_doi(m.group(0)) if m else ""


def normalize_or_extract_doi(x: Any) -> str:
    return extract_doi_from_text(x) or normalize_doi(x)


def is_valid_doi(x: Any) -> bool:
    doi = normalize_or_extract_doi(x)
    return doi.startswith("10.") and "/" in doi


def extract_year(x: Any) -> Optional[int]:
    m = re.search(r"(19|20)\d{2}", clean_text(x))
    return int(m.group(0)) if m else None


def read_table_file(path: Path) -> pd.DataFrame:
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix not in SUPPORTED_EXTENSIONS:
        raise ValueError(f"Unsupported file type: {path}")

    if suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(path, sheet_name=0, dtype=str)
    elif suffix == ".tsv":
        df = pd.read_csv(path, dtype=str, sep="\t", encoding="utf-8-sig")
    else:
        try:
            df = pd.read_csv(path, dtype=str, sep=None, engine="python", encoding="utf-8-sig")
        except UnicodeDecodeError:
            df = pd.read_csv(path, dtype=str, sep=None, engine="python", encoding="latin1")

    df.dropna(how="all").fillna("")
    df.columns = [clean_text(c) for c in df.columns]

    return df

def sample_values(s: pd.Series, max_n: int = 200) -> List[str]:
    vals = s.dropna().map(clean_text)
    vals = vals[vals != ""]
    return vals.head(max_n).tolist()


def heading_score(col: str, role: str) -> float:
    col_norm = normalize_title(col)
    candidates = ROLE_PHRASES_NORM.get(role, [])

    if not col_norm or not candidates:
        return 0.0

    best = max(fuzz.token_set_ratio(col_norm, c) for c in candidates)

    if best >= 95:
        return 1.0
    if best >= 85:
        return 0.85
    if best >= 75:
        return 0.65
    if best >= 65:
        return 0.45

    return 0.0


def data_score(s: pd.Series, role: str) -> float:
    vals = sample_values(s)
    if not vals:
        return 0.0

    if role == "doi":
        return sum(1 for v in vals if is_valid_doi(v)) / len(vals)

    if role == "year":
        return sum(
            1 for v in vals
            if extract_year(v) is not None and len(clean_text(v)) <= 40
        ) / len(vals)

    if role == "title":
        hits = 0
        unique = set()

        for v in vals:
            text = clean_text(v)
            norm = normalize_title(text)

            if not norm:
                continue
            if extract_doi_from_text(text):
                continue
            if extract_year(text) is not None and len(text) <= 40:
                continue

            words = norm.split()

            if 4 <= len(words) <= 60 and 20 <= len(text) <= 500:
                hits += 1
                unique.add(norm)

        return min(1.0, 0.75 * (hits / len(vals)) + 0.25 * (len(unique) / max(hits, 1)))

    return 0.0


def infer_columns(df: pd.DataFrame, role: str) -> List[str]:
    scored = []

    for col in df.columns:
        if col in META_COLS:
            continue

        hs = heading_score(col, role)
        ds = data_score(df[col], role)
        score = max(hs, ds)

        if hs > 0 and ds > 0:
            score = min(1.0, score + 0.15)

        if score >= MIN_SCORE[role]:
            scored.append((score, col))

    scored.sort(reverse=True)
    return [col for score, col in scored]


def choose_first(row: pd.Series, cols: List[str]) -> str:
    for col in cols:
        val = clean_text(row.get(col, ""))
        if val:
            return val
    return ""


def choose_doi(row: pd.Series, doi_cols: List[str], all_cols: List[str]) -> str:
    for col in doi_cols:
        doi = normalize_or_extract_doi(row.get(col, ""))
        if is_valid_doi(doi):
            return doi

    for col in all_cols:
        doi = extract_doi_from_text(row.get(col, ""))
        if doi:
            return doi

    return ""


def read_manual_files(
    manual_dir: Optional[str],
    manual_files: Optional[List[str]],
) -> pd.DataFrame:
    files: List[Path] = []

    if manual_dir:
        p = Path(manual_dir)
        for ext in SUPPORTED_EXTENSIONS:
            files.extend(sorted(p.glob(f"*{ext}")))

    if manual_files:
        files.extend(Path(f) for f in manual_files)

    files = [
        f for f in files
        if f.exists()
        and not f.name.startswith("~$")
        and f.suffix.lower() in SUPPORTED_EXTENSIONS
    ]

    if not files:
        raise FileNotFoundError("No manual table files found.")

    frames = []

    for path in files:
        df = read_table_file(path)
        if df.empty:
            continue

        df["manual_source_file"] = path.name
        df["manual_excel_row"] = df.index + 2
        frames.append(df)

    if not frames:
        raise ValueError("Manual files were found, but no usable rows were read.")

    return pd.concat(frames, ignore_index=True, sort=False).fillna("")


def prepare_manual(manual: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    original_cols = [c for c in manual.columns if c not in META_COLS]

    detected = {
        "doi": infer_columns(manual, "doi"),
        "title": infer_columns(manual, "title"),
        "year": infer_columns(manual, "year"),
    }

    bad_title_headings = {
        "author", "authors", "forfattare",
        "journal", "publication", "publikation",
        "source", "tidskrift", "volume", "issue", "pages"
    }

    detected["title"] = [
        c for c in detected["title"]
        if normalize_title(c) not in bad_title_headings
    ]

    bad_year_headings = {
        "publication", "publikation", "journal", "tidskrift",
        "title", "titel", "author", "authors", "forfattare"
    }

    detected["year"] = [
        c for c in detected["year"]
        if normalize_title(c) not in bad_year_headings
    ]


    print("\nDetected manual columns:")
    print(f"  DOI:   {detected['doi']}")
    print(f"  Title: {detected['title']}")
    print(f"  Year:  {detected['year']}")

    manual["manual_doi_raw"] = manual.apply(
        lambda row: choose_doi(row, detected["doi"], original_cols),
        axis=1,
    )
    manual["manual_doi_norm"] = manual["manual_doi_raw"].apply(normalize_or_extract_doi)

    manual["manual_title"] = manual.apply(
        lambda row: choose_first(row, detected["title"]),
        axis=1,
    )
    manual["manual_title_norm"] = manual["manual_title"].apply(normalize_title)

    manual["manual_year"] = manual.apply(
        lambda row: choose_first(row, detected["year"]),
        axis=1,
    )
    manual["manual_year_int"] = manual["manual_year"].apply(extract_year)

    if not manual["manual_doi_norm"].any() and not manual["manual_title_norm"].any():
        raise ValueError(
            "Could not detect usable DOI or title values from the manual file. "
            f"Available columns: {list(manual.columns)}"
        )

    return manual, original_cols


def existing_col(df: pd.DataFrame, name: str) -> Optional[str]:
    return name if name in df.columns else None


def prepare_wos(wos_file: str) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
    path = Path(wos_file)

    if not path.exists():
        raise FileNotFoundError(f"WoS filtered file not found: {path}")

    wos = read_table_file(path)

    cols = {
        "ut": existing_col(wos, "UT"),
        "title": existing_col(wos, "Title"),
        "journal": existing_col(wos, "Journal"),
        "year": existing_col(wos, "Year"),
        "doi": existing_col(wos, "DOI"),
        "authors": existing_col(wos, "Authors"),
    }

    if not cols["title"] and not cols["doi"]:
        raise ValueError(
            "Filtered WoS file must contain at least 'Title' or 'DOI'. "
            f"Available columns: {list(wos.columns)}"
        )

    wos["__doi_norm"] = wos[cols["doi"]].apply(normalize_or_extract_doi) if cols["doi"] else ""
    wos["__title_norm"] = wos[cols["title"]].apply(normalize_title) if cols["title"] else ""
    wos["__year_int"] = wos[cols["year"]].apply(extract_year) if cols["year"] else None

    return wos, cols


def build_index(df: pd.DataFrame, col: str) -> Dict[str, List[int]]:
    index: Dict[str, List[int]] = {}

    for idx, value in df[col].items():
        value = clean_text(value)
        if value:
            index.setdefault(value, []).append(idx)

    return index


def year_ok(manual_year: Optional[int], wos_year: Optional[int], window: int) -> bool:
    if manual_year is None or wos_year is None:
        return True
    if pd.isna(manual_year) or pd.isna(wos_year):
        return True

    return manual_year <= wos_year <= manual_year + window


def best_by_year(
    indices: List[int],
    wos: pd.DataFrame,
    manual_year: Optional[int],
    window: int,
) -> int:
    compatible = [
        idx for idx in indices
        if year_ok(manual_year, wos.loc[idx, "__year_int"], window)
    ]

    return compatible[0] if compatible else indices[0]


def find_title_match(
    title_norm: str,
    manual_year: Optional[int],
    wos: pd.DataFrame,
    title_index: Dict[str, List[int]],
    threshold: int,
    window: int,
):
    if not title_norm:
        return None, "", 0

    if title_norm in title_index:
        idx = best_by_year(title_index[title_norm], wos, manual_year, window)
        return idx, "title_exact", 100

    candidates = wos[wos["__title_norm"].astype(str).str.len() > 0]

    if manual_year is not None:
        year_filtered = candidates[
            candidates["__year_int"].apply(lambda y: year_ok(manual_year, y, window))
        ]
        if not year_filtered.empty:
            candidates = year_filtered

    choices = {
        int(idx): title
        for idx, title in candidates["__title_norm"].items()
        if title
    }

    if not choices:
        return None, "", 0

    best = process.extractOne(title_norm, choices, scorer=fuzz.token_set_ratio)

    if best is None:
        return None, "", 0

    _, score, idx = best

    if score >= threshold:
        return int(idx), "title_fuzzy", int(score)

    return None, "", int(score)


def get_value(row: Optional[pd.Series], col: Optional[str]) -> str:
    if row is None or not col or col not in row.index:
        return ""

    return clean_text(row[col])


def run_check(
    wos_filtered_csv: str,
    out_dir: str,
    manual_dir: Optional[str] = None,
    manual_files: Optional[List[str]] = None,
    title_threshold: int = 90,
    year_window: int = 1,
):
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    manual = read_manual_files(manual_dir, manual_files)
    manual, original_cols = prepare_manual(manual)

    wos, wos_cols = prepare_wos(wos_filtered_csv)

    doi_index = build_index(wos, "__doi_norm")
    title_index = build_index(wos, "__title_norm")

    labels = {
        "doi_exact": "DOI",
        "title_exact": "Title exact",
        "title_fuzzy": "Title fuzzy",
    }

    results = []

    for _, row in manual.iterrows():
        manual_doi = clean_text(row.get("manual_doi_norm", ""))
        manual_title = clean_text(row.get("manual_title_norm", ""))
        manual_year = row.get("manual_year_int", None)

        matched_idx = None
        method = ""
        score = 0

        if manual_doi and manual_doi in doi_index:
            matched_idx = best_by_year(doi_index[manual_doi], wos, manual_year, year_window)
            method = "doi_exact"
            score = 100

        if matched_idx is None:
            matched_idx, method, score = find_title_match(
                manual_title,
                manual_year,
                wos,
                title_index,
                title_threshold,
                year_window,
            )

        matched = matched_idx is not None
        wos_row = wos.loc[matched_idx] if matched else None

        out = {col: clean_text(row.get(col, "")) for col in original_cols}
        out["matched_in_wos"] = matched
        out["matched_by"] = labels.get(method, "Not matched")
        out["match_score"] = score if matched else 0
        out["wos_match_title"] = get_value(wos_row, wos_cols["title"]) if matched else ""
        out["wos_match_doi"] = get_value(wos_row, wos_cols["doi"]) if matched else ""
        out["wos_match_year"] = get_value(wos_row, wos_cols["year"]) if matched else ""

        results.append(out)

    result_df = pd.DataFrame(results)

    compare_out = out_path / "manual_vs_filtered_wos_comparison.csv"
    result_df.to_csv(compare_out, index=False, encoding="utf-8-sig")

    print("\nDone.")
    print(f"Manual rows checked: {len(result_df)}")
    print(f"Available in filtered WoS: {int(result_df['matched_in_wos'].sum())}")
    print(f"Not available in filtered WoS: {int((~result_df['matched_in_wos']).sum())}")
    print()
    print(f"Saved comparison file: {compare_out}")


def build_argparser():
    ap = argparse.ArgumentParser(
        description="Check manual publication lists against filtered WoS results."
    )

    ap.add_argument("--wos-filtered", required=True)
    ap.add_argument("--manual-dir", default="")
    ap.add_argument("--manual-files", nargs="*", default=None)
    ap.add_argument("--out-dir", default="manual_vs_filtered_wos_check")
    ap.add_argument("--title-threshold", type=int, default=90)
    ap.add_argument("--year-window", type=int, default=1)

    return ap


def main():
    args = build_argparser().parse_args()

    manual_dir = args.manual_dir.strip() if args.manual_dir else None
    manual_files = args.manual_files if args.manual_files else None

    if not manual_dir and not manual_files:
        raise ValueError("Provide either --manual-dir or --manual-files.")

    run_check(
        wos_filtered_csv=args.wos_filtered,
        manual_dir=manual_dir,
        manual_files=manual_files,
        out_dir=args.out_dir,
        title_threshold=args.title_threshold,
        year_window=args.year_window,
    )


if __name__ == "__main__":
    main()
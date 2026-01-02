import pandas as pd
import re
import yaml


# SETTINGS


WOS = r"wos_results.csv"        # Fetched WoS file
PI  = r"Pi_list.csv"    # PI list file (with column "Name")
Keyword_File = r"keyword.yml"
Results  = "filtered_results.csv"
PI_Checked = "pi_names_checked.csv"

# 1) Load YAML keyword

with open(Keyword_File, "r", encoding="utf-8") as f:
    kw = yaml.safe_load(f)

include_terms = kw.get("include_terms", [])
exclude_terms = kw.get("exclude_terms", [])
exclude_terms_category = kw.get("exclude_terms_category", []) 

if exclude_terms_category:
    EXCLUDE_CATEGORY_REGEX = re.compile("|".join(map(re.escape, exclude_terms_category)), re.IGNORECASE)
else:
    EXCLUDE_CATEGORY_REGEX = re.compile(r"a^")  # matches nothing


# Special handling for SCoRe vs score 
normal_terms = [t for t in include_terms if t.lower() != "score" and t != "SCoRe"]

if normal_terms:
    NORMAL_INCLUDE_REGEX = re.compile(
        r"(?<!\w)(" + "|".join(map(re.escape, normal_terms)) + r")(?!\w)",
        re.IGNORECASE
    )
else:
    NORMAL_INCLUDE_REGEX = re.compile(r"a^")  

SCORE_REGEX = re.compile(r"(?<!\w)SCoRe(?!\w)")  # case-sensitive

if exclude_terms:
    EXCLUDE_REGEX = re.compile("|".join(map(re.escape, exclude_terms)), re.IGNORECASE)
else:
    EXCLUDE_REGEX = re.compile(r"a^")  

def include_match(text: str) -> bool:
    if not isinstance(text, str):
        return False
    return bool(SCORE_REGEX.search(text) or NORMAL_INCLUDE_REGEX.search(text))

def extract_include_phrase(text):
    if not isinstance(text, str):
        return ""
    m = SCORE_REGEX.search(text)
    if m:
        return m.group(0)
    m2 = NORMAL_INCLUDE_REGEX.search(text)
    return m2.group(0) if m2 else ""

def extract_include_sentence(text):
    if not isinstance(text, str):
        return ""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    for s in sentences:
        if SCORE_REGEX.search(s) or NORMAL_INCLUDE_REGEX.search(s):
            return s.strip()
    return ""

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

# 2) Load WoS data

df = pd.read_csv(WOS, sep=",")
df = df.dropna(axis=1, how="all")
df = df.loc[:, (df != "").any(axis=0)]


# 3) Build searchable text for include/exclude

category_cols = [c for c in df.columns if re.search(r"categor", c, re.IGNORECASE)]

text_cols = [
    c for c in df.columns
    if re.search(r"title|abstract|fund|keyword|ack", c, re.IGNORECASE)
    and c not in category_cols
]

df["fulltext"] = df[text_cols].fillna("").astype(str).agg(" ".join, axis=1)

# Separate category-only text
if category_cols:
    df["category_text"] = df[category_cols].fillna("").astype(str).agg(" ".join, axis=1)
else:
    df["category_text"] = ""

df["include_match"] = df["fulltext"].apply(include_match)
df["exclude_match"] = df["fulltext"].str.contains(EXCLUDE_REGEX, na=False)

df["exclude_category_match"] = df["category_text"].str.contains(EXCLUDE_CATEGORY_REGEX, na=False)

df["matched_term"] = ""
df["matched_sentence"] = ""

inc_mask = df["include_match"]
df.loc[inc_mask, "matched_term"] = df.loc[inc_mask, "fulltext"].apply(extract_include_phrase)
df.loc[inc_mask, "matched_sentence"] = df.loc[inc_mask, "fulltext"].apply(extract_include_sentence)

include_only = df[df["include_match"]]

filtered = df[df["include_match"] & ~df["exclude_match"] & ~df["exclude_category_match"]]

print(f"Total INCLUDE matches: {len(include_only)}")
print(f"Excluded due to EXCLUSION terms (main text): {df['exclude_match'].sum()}")
print(f"Excluded due to CATEGORY exclusions: {df['exclude_category_match'].sum()}")
print(f"FINAL kept: {len(filtered)}")



# 4) Load PI names

pi_df = pd.read_csv(PI, sep=";")
if "Name" not in pi_df.columns:
    raise ValueError('PI file must contain a column named "Name".')

pi_names = (
    pi_df["Name"]
    .dropna()
    .astype(str)
    .str.strip()
)
pi_names = [n for n in pi_names if n]
pi_names = sorted(set(pi_names), key=len, reverse=True)


# 5) Build PI regex 

# Stage 1: last-name filter
def get_lastname(name: str) -> str:
    parts = re.split(r"\s+", name.strip())
    return parts[-1] if parts else ""

pi_lastnames = sorted(set(get_lastname(n) for n in pi_names if get_lastname(n)), key=len, reverse=True)

if pi_lastnames:
    LASTNAME_REGEX = re.compile(
        r"(?<!\w)(?:" + "|".join(map(re.escape, pi_lastnames)) + r")(?!\w)",
        re.IGNORECASE
    )

else:
    LASTNAME_REGEX = re.compile(r"a^")  # matches nothing

# Stage 2: full-name extraction
def name_to_pattern(name: str) -> str:
    parts = re.split(r"\s+", name.strip())
    return r"(?<!\w)" + r"[\s\-]+".join(map(re.escape, parts)) + r"(?!\w)"

if pi_names:
    PI_REGEX = re.compile("|".join(name_to_pattern(n) for n in pi_names), re.IGNORECASE)
else:
    PI_REGEX = re.compile(r"a^")

def find_pi_names_full(text: str):
    if not isinstance(text, str) or not text.strip():
        return ""
    hits = PI_REGEX.findall(text)
    hits = dedupe_keep_order([h.strip() for h in hits])
    return "; ".join(hits)

# 6) Build funding/ack search texts
fund_cols_candidates = ["FundingText", "FundingAgencies", "GrantNumbers"]
fund_cols = [c for c in fund_cols_candidates if c in df.columns]

if fund_cols:
    df["funding_search_text"] = df[fund_cols].fillna("").astype(str).agg(" ".join, axis=1)
else:
    df["funding_search_text"] = ""



df["pi_names_in_funding"] = ""


cand_fund = df["funding_search_text"].str.contains(LASTNAME_REGEX, na=False)


df.loc[cand_fund, "pi_names_in_funding"] = df.loc[cand_fund, "funding_search_text"].apply(find_pi_names_full)


df["pi_in_funding"] = df["pi_names_in_funding"].str.len() > 0


# 8) Save results

filtered = df[df["include_match"] & ~df["exclude_match"] & ~df["exclude_category_match"]]
filtered.to_csv(Results, index=False)


PI_checked = df[df["pi_in_funding"] ]
PI_checked.to_csv(PI_Checked, index=False)

print(f"\nSaved:\n- {Results}\n- {PI_Checked}\n")

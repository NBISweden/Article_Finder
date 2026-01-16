# Article Finder (WoS fetch + keyword/category filtering)

This repository provides two scripts:

- **Script 1 (filtering):** `src/filter_wos_records_category.py`  
- **Script 2 (fetching from WoS API):** `src/fetch_wos_data.py`

It uses **Pixi** for reproducible environments and works on **Windows, macOS, and Linux**.
---

## System requirements

- **Operating system:** Windows / macOS / Linux
- **Software:** Git + Pixi
- **Python:** Managed by Pixi (no manual venv needed)

> If you have `pixi.toml` + `pixi.lock`, you generally do **not** need to create a separate virtual environment. Pixi manages an isolated environment for you.

---

## Installation

```bash
git clone https://github.com/NBISweden/Article_Finder.git
cd Article_Finder
pixi install
```

## Quick start (run the included test dataset)

The filtering script expects these filenames in the repo root:

- `wos_results.csv`
- `keyword.yml`
- `Pi_list.csv` (optional)



## Script 1: Filter WoS records 

### When to use
Use `src/filter_wos_records_category.py` if you already have a WoS export (CSV) and want to:
- keep records matching include keywords,
- exclude records matching exclusion keywords,
- optionally exclude records based on WoS category columns only,
- optionally detect PI names in the funding text.

If you don’t have a WoS export, use src/fetch_wos_data.py (Script 2) to fetch data from the WoS API or export the data manually from the WoS website.

### Run
The script expects these files in the **current working directory** (repo root):
- `wos_results.csv` (WoS export)
- `keyword.yml` (keyword rules)
- `Pi_list.csv` (optional PI list)

## Test data (included)

This repository includes small test inputs so you can clone the repo and run it immediately:

- `data/wos_results.csv` — small WoS-like CSV for fast testing
- `data/Pi_list.csv` — small PI list (optional input)
- `configs/keyword.yml` — example keyword rules (include/exclude/category)

Run:
```bash
pixi run python src/filter_wos_records_category.py
```

## Keyword file (`keyword.yml`)

The YAML configuration can contain three lists:

```yaml
include_terms:
exclude_terms:
exclude_terms_category:
```

### Meaning

#### `include_terms`
A record is considered relevant if any include term matches the searchable text.

#### `exclude_terms`
Records are removed if any exclusion term matches the same searchable text.

#### `exclude_terms_category`
WoS category columns often contain broad/general labels.  
This list is searched only in category columns (columns whose name contains `categor`) to avoid removing data you want due to broad category wording.

---

### Special case: `SCoRe` (case-sensitive)
Some terms are ambiguous abbreviations (e.g., `score` vs `SCoRe`).  
If `SCoRe` is included in `include_terms`, it is matched case-sensitively (exactly `SCoRe`) so it is not mixed with generic uses of “score”.

## PI-name check (optional)

Sometimes authors acknowledge a PI instead of the organisation/infrastructure.  
If `Pi_list.csv` is provided, the script also checks funding-related fields for PI names and writes an additional output file.

PI detection uses a two-stage approach:

1. **last-name screening** (faster)
2. **full-name extraction** (for final reporting)

## Inputs and formats

### `wos_results.csv`

Expected as a comma-separated CSV (`,`).

The script searches columns whose names contain (case-insensitive):
- `title`
- `abstract`
- `fund`
- `keyword`
- `ack`

Category columns are detected automatically as columns containing:
- `categor`

### `Pi_list.csv` (optional)


Must contain a column named `Name`.  
Additional columns are allowed.

## Outputs

Running the filter script produces:

### `filtered_results.csv`
Final filtered records. Includes extra columns:
- `matched_term` (first matched include term)
- `matched_sentence` (sentence containing the match)

### `pi_names_checked.csv` (optional)
Created only if PI file exists and is valid.  
Subset of records where one or more PI names were detected in funding-related text.

## Script 2: Fetch WoS data (Clarivate WoS API)

### When to use
Use `src/fetch_wos_data.py` if you want to fetch WoS records directly from the WoS API.

### API key
You need an API key from Clarivate:  
https://developer.clarivate.com/apis/wos

Tip: Request an **Expanded API key** 
## API key setup (`.env`)

### Why `.env` is used
The API key must not be hard-coded. The script reads it from an environment variable:

- `WOS_API_KEY`

### Create a `.env` file
Create a file called `.env` in the directory where you run the script:

```text
WOS_API_KEY=YOUR_KEY_HERE
```
## Main settings (important parameters)

### `USR_QUERY`
WoS search query string used by the API.

In the original use case, WoS did not reliably filter by inclusion/exclusion directly, so a broad query (e.g., Sweden + year) was fetched and later filtered locally using regex/keywords.

You can set any query you want.

### `PAGE_SIZE`
Number of records per API call. Max is `100`.

### `MAX_RECORDS`
`None` means “fetch all records found”. You can set a limit if needed.

### `SAVE_DEBUG_FIRST_PAGE`
Saves the first response page for debugging/inspecting schema.

### `SLEEP_BETWEEN_CALLS`
Small sleep between calls to reduce rate limiting risk.
## How fetching works 

### Seed query
The script first sends a seed query (`count=0`, `optionView="SR"`) to determine:
- total number of matching records
- query ID (if returned)

### Fetch loop
Then it fetches full records using:
- `optionView="FR"`
- `firstRecord` set to page start index
- `count` set to `PAGE_SIZE` (or smaller on the last page)

### Rate limiting + retry logic
The script retries automatically when:
- **HTTP 429 (rate limit):** exponential backoff (sleep and retry up to `max_tries`)
- **HTTP 5xx (server errors):** short incremental waits and retries

For other **4xx** errors, it prints URL + response head and raises.

---

## What fields are extracted
For each record, the script extracts:
- `UT` (unique identifier)
- Title, Journal, Year
- DOI (from identifiers; fallback regex search)
- Authors, author emails
- Abstract
- Funding text, agencies, grant numbers
- Author keywords, Keywords Plus
- WoS categories (traditional and extended)








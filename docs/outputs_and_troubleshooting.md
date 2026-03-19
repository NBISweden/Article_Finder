# Outputs and Troubleshooting

This page describes the files produced by Article Finder and the most common problems users may face.

---
## Results and interactive table output

After a successful run, the interface displays a results section.

This section shows:

- the completed run mode
- the paths of the generated output files
- interactive preview tables for the available output files

### Result summary

At the top of the results section, the interface shows a short summary including paths.
---

### Interactive preview tables

The generated files are shown as interactive Marimo tables.

Depending on the workflow, the interface may show the following tabs:

- **Full Result**
- **Filtered**
- **Contributor**

These tables are intended for quick inspection of the outputs inside the interface.

### Table behavior

The preview tables:

- are interactive
- use pagination
- load only part of the file for preview

In the current code, the interface loads up to:

```text
500 rows
```

from each file for preview.

This means:

- the preview is useful for checking whether the output looks correct
- the preview is **not necessarily the full file**
- the complete output is still saved on disk in the run directory

---
## Output files

### Query fetch outputs

#### `wos_results.csv`

Flattened Web of Science metadata for the fetched records.

The output includes fields such as:

- `UT`
- `Title`
- `Journal`
- `Year`
- `DOI`
- `Authors`
- `AuthorEmails`
- `Abstract`
- `FundingText`
- `FundingAgencies`
- `GrantNumbers`
- `AuthorKeywords`
- `KeywordsPlus`
- `WoSCategoriesTraditional`
- `WoSCategoriesExtended`

#### `records_full.jsonl`

Full raw WoS records written in JSON Lines format.

This is useful if you want to inspect the original WoS record structure.

#### `debug_first_page.json`

Saved first API response page for debugging or schema inspection.

This is useful if the API structure changes or if records are not being extracted as expected.

---

### DOI fetch outputs

#### `wos_results_by_doi.csv`

The original DOI input table merged with retrieved WoS metadata.

The merged output can include:

- the original input columns
- WoS metadata columns
- `Found`
- `DOI_from_record`

`Found` indicates whether a matching WoS record was retrieved.

#### `missing_dois.txt`

Normalized DOIs that could not be matched.

#### `missing_rows_with_input_columns.csv`

Original input rows that were not matched.

This is useful when investigating why some DOI rows failed.

#### `raw_pages.jsonl`

Saved raw API page responses from the DOI workflow.

This is useful for debugging retrieval and matching.

---

### Filter outputs

#### `filtered_results.csv`

Records kept after filtering.

This file includes the original metadata columns together with filtering-related columns such as:

- `fulltext`
- `category_text`
- `include_match`
- `exclude_match`
- `exclude_category_match`
- `include_category_match`
- `matched_term`
- `matched_sentence`


#### `Contributor_names_checked.csv`

This file contains the subset of records where at least one PI/staff name was detected.

The output can include the following columns:

- `PI_staff_in_ack`
- `PI_staff_names_in_ack_paper`
- `PI_staff_names_in_ack_list`
- `PI_staff_in_authors`
- `PI_staff_names_in_authors_paper`
- `PI_staff_names_in_authors_list`

### Description of each column

#### `PI_staff_in_ack`

A boolean-style field indicating whether at least one PI/staff name was found in acknowledgment.

---

#### `PI_staff_names_in_ack_paper`

Shows the exact name form that was detected in the paper text.

This column is useful when you want to see **what exact text in the paper triggered the match**.

---

#### `PI_staff_names_in_ack_list`

Shows the standardized PI/staff name from your uploaded PI/staff name list that matched the paper text.

This is the name as it appears in your input CSV, not necessarily as written in the paper.

This column is useful when you want to know **which person from your name list was matched**.

It is the same for author columns as well.

---

### Pipeline outputs

When you run through the Marimo interface or pipeline module, outputs are usually written under:

```text
runs/<mode>_<hash>/
```

Examples:

```text
runs/fetch_query_<hash>/
runs/fetch_doi_<hash>/
runs/filter_<hash>/
```

Each run directory typically contains:

- output files
- `manifest.json`
- `pipeline.log`

#### `manifest.json`

Contains:

- run ID
- timestamp
- configuration
- artifact paths

#### `pipeline.log`

Contains structured event records and output lines from the underlying scripts.

---

## Common errors and how to fix them

### 1. Missing API key

#### Symptom

A fetch run stops before contacting the WoS API.

In the current Marimo-based setup, the interface may show:

```text
Missing API Key. Please read the Secrets Management guide in the README
```

#### Fix

Set the API key in one of the supported ways.

The current credential helper checks:

1. Python keyring
   - service: `wos_api`
   - account: `default`

2. Environment variable
   - `WOS_API_KEY`

Example `.env` file:

```text
WOS_API_KEY=your_key_here
```

---

### 2. HTTP 429 rate limit

#### Symptom

The fetch step reports a 429 error.

#### Cause

Too many requests were sent in a short time.

#### Fix

Increase **Sleep between calls** in the interface.

Suggested values:

- `0.5`
- `1.0`

This is the main advanced option to adjust for rate-limit problems.

---

### 3. HTTP 5xx server error

#### Symptom

The fetch step reports a 5xx error.

#### Cause

Temporary WoS API server problem.

#### Fix

Wait and rerun later.

The scripts already retry many temporary failures automatically.

You can also keep the default page size and increase sleep slightly if the connection is unstable.

---

### 4. Invalid date format

#### Symptom

The interface reports a date-format error.

#### Fix

In the interface, use:

```text
DD-MM-YYYY
```

Example:

```text
01-01-2025
```

---

### 5. Upload a DOI file

#### Symptom

DOI fetch mode fails because no DOI file was provided.

#### Fix

Upload a supported DOI file:

- `.txt`
- `.csv`
- `.tsv`
- `.xlsx`
- `.xls`

For table-based DOI files, make sure the file contains a DOI column.

---

### 6. No DOI column found

#### Symptom

DOI mode fails because the input file does not contain a recognized DOI column.

#### Fix

Rename the DOI column to one of:

- `DOI`
- `doi`
- `Doi`
- `doi `

For plain text files, you can instead provide one DOI per line.

---

### 7. Upload Keywords YAML

#### Symptom

Filter mode reports that the keyword file is missing.

#### Fix

Upload a valid YAML file containing at least one non-empty rule category from:

- `include_terms`
- `exclude_terms`
- `include_terms_category`
- `exclude_terms_category`

---

### 8. `keyword.yml has no rules`

#### Symptom

The filter script fails because the YAML file is present but all rule categories are empty.

#### Fix

Add rules to at least one of the four YAML categories.

No single category is mandatory, but at least one must contain rules.

---

### 9. Upload Contributor CSV

#### Symptom

PI/staff name checking is enabled, but no CSV file was uploaded.

#### Fix

Upload the PI/staff name list CSV file before running Filter mode.

---

### 10. Contributor file must contain a column named `Name`

#### Symptom

PI/staff checking fails.

#### Fix

Make sure the PI/staff CSV contains a column named:

```text
Name
```
---

### 11. `sleep_between_calls must be 0 or greater`

#### Symptom

A fetch workflow fails before running.

#### Cause

The sleep value was set to a negative number.

#### Fix

Use `0` or a positive value.

Recommended default:

```text
0.25
```

---

### 12. `max_records must be greater than 0 when provided`

#### Symptom

Query fetch fails before running.

#### Cause

A non-positive `max_records` value was passed directly to the fetch function.

#### Fix

In the interface, use:

- `0` for no limit
- a positive integer for a limit

Examples:

- `10`
- `50`
- `100`

---

### 13. No records returned in query mode

#### Symptom

The fetch step completes but returns zero records.

#### Possible causes

- the WoS query is too narrow
- the date range is too restrictive
- no matching records exist

#### Fix

- test with a broader query
- reduce restrictions
- use a small `Max records` value while testing

---

### 14. Many DOIs are missing in DOI mode

#### Symptom

The DOI fetch step completes, but many DOIs appear in `missing_dois.txt`.

#### Possible causes

- some DOIs are not indexed in WoS
- some DOI values are malformed
- some DOI rows cannot be matched even after normalization and fallback

#### Fix

Inspect:

- `missing_dois.txt`
- `missing_rows_with_input_columns.csv`

Also verify the DOI source file formatting.

---

### 15. No records extracted on this page

#### Symptom

The query fetch script reports that no records were extracted from an API page.

#### Fix

Inspect:

- `debug_first_page.json`

This usually indicates that the returned API structure is not what the parser expected.

For DOI mode, raw API responses are saved to:

- `raw_pages.jsonl`

---

### 16. Filtering returns no records

#### Symptom

The filtering step runs, but `filtered_results.csv` is empty or nearly empty.

#### Possible causes

- include rules are too strict
- exclusion rules are too broad
- the wrong WoS metadata file was used
- category-based rules are excluding too much

#### Fix

- review the YAML rules
- simplify the include rules
- temporarily reduce exclusions
- inspect the input WoS metadata before filtering

---
## See also

- [User Guide](user_guide.md)
- [Configuration](configuration.md)
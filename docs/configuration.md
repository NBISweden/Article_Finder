# Configuration

This page describes the files and settings that Article Finder expects.

## 1. Keyword YAML file

Filtering requires a YAML file describing the rule set.

Supported file types in the interface:

- `.yml`
- `.yaml`

Expected categories in the YAML file:

```yaml
include_terms:
exclude_terms:
include_terms_category:
exclude_terms_category:
```

### Important rule

At least **one** of these categories must contain rules.

The code will fail if all four are empty.

### What each category does

#### `include_terms`

These terms are matched in the main searchable text.

A record is considered relevant if it matches these terms.

#### `exclude_terms`

These terms are also matched in the main searchable text.

If a record matches these terms, it is excluded.

#### `include_terms_category`

These terms are matched only in category columns.

If this category is used, category matching becomes an additional keep condition.

#### `exclude_terms_category`

These terms are matched only in category columns.

If a record matches these terms in the category fields, it is excluded.

---

## 2. DOI input file

DOI mode accepts these file types:

- `.csv`
- `.tsv`
- `.txt`
- `.xlsx`
- `.xls`

### DOI column name

For table-based DOI input files, the file must contain a DOI column.

It recognizes these column names:

- `DOI`
- `doi`
- `Doi`
- `doi `

For plain text files, the file can contain one DOI per line.
---

## 3. PI/staff name list CSV file

Optional PI/staff name checking expects a **CSV** file with a required column:

```text
Name
```

This file is used when the PI/staff name check is enabled.

### What it is used for

The current code uses the PI/staff name list to search for names in:

- the `Authors` field
- acknowledgment text

---

## 4. Advanced settings used during fetch

The fetch workflows use these configuration settings.

### `page_size`

Controls how many records are requested per API call.

Default:

```text
100
```

### `sleep`

Controls the delay between API requests.

Default:

```text
0.25
```

### `max_records`

Used only in **Fetch by WoS query** mode.

In the interface:

- `0` means no limit

### `use_cache`

Controls whether previous outputs are reused for the same pipeline configuration.

In the current pipeline, caching is implemented for:

- `fetch_query`
- `fetch_doi`

## See also

- [User Guide](user_guide.md)
- [Outputs and Troubleshooting](outputs_and_troubleshooting.md)
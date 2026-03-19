# User Guide

This guide explains how to use Article Finder through the **Marimo user interface**.

---

## Before you start

A **Web of Science API key** is required for the fetch workflows.

Before starting the interface, make sure the API key has been configured.

See:

- [API key setup](api_key_setup.md)

---

## Start the interface

From the repository directory, run:

```bash
pixi run ui
```
After the application starts, Marimo opens in your browser.

---

## Interface layout

The interface has two main tabs:

- **Fetch**
- **Filter**

The typical workflow is:

1. Fetch publication metadata
2. Filter the metadata
---

## Fetch tab

The Fetch tab retrieves metadata from the Web of Science API.

It supports two modes.

### 1. Fetch by WoS query

Use this mode when you want to retrieve publications directly from Web of Science using a search query.

Example:

```text
CU=(Sweden)
```

Recommended use in this project:

- fetch broadly, for example all Sweden publications for a year
- then use **Filter** mode to identify the relevant subset

This usually gives better control and more accurate final selection than trying to make the WoS query very narrow from the beginning.

Inputs in query mode:

- **WoS usrQuery**
- **Start Date (DD-MM-YYYY)**
- **End Date (DD-MM-YYYY)**
- **Max records (0 = no limit)**

### 2. Fetch by DOI list

Use this mode when you already have a list of DOIs and want to retrieve Web of Science metadata for each DOI.

This is useful when:

- you already have a DOI list from a previous year
- you want to enrich an existing publication list with WoS metadata

Supported DOI upload types in the UI:

- `.txt`
- `.csv`
- `.tsv`
- `.xlsx`
- `.xls`

For table-based DOI files, the DOI column must be named one of:

- `DOI`
- `doi`
- `Doi`
- `doi `

For plain text files, the file can contain one DOI per line.

---

## Filter tab

The Filter tab applies local filtering rules to a WoS metadata file.

Required inputs:

- WoS metadata file
- keyword YAML file

Optional inputs:

- PI/staff name list CSV file

---

## What each input means

### WoS metadata file

This is the metadata file that will be filtered.

### Keyword YAML file

This file defines the filtering rules.

It controls which records are kept or excluded.

Supported file types in the UI:

- `.yml`
- `.yaml`

### PI/staff name list CSV file

This file is used only if PI/staff name checking is enabled.

It must contain a column called:

```text
Name
```
---

## Advanced settings

The advanced panel contains settings that mainly affect fetch workflows.

### Page size

Controls how many records are requested per API call during fetch operations.

Default:

```text
100
```
Keep this value unless you are debugging or testing.

### Sleep between calls

Controls the delay between API requests.

Default:

```text
0.25
```
Increase this if you encounter rate-limit or fetch stability problems.

Suggested values to try:

- `0.5`
- `1.0`

### Max records

Useful when testing a new WoS query before running a large fetch.

In the interface, `0` means no limit.

### Use cache

Reuses previous outputs for the same configuration.

In the current pipeline, caching is implemented for the fetch workflows.

Keep this enabled for normal use.

---

## What happens when you click Run

When you click **Run Fetch** or **Run Filter**:

1. the interface reads the selected mode
2. uploaded files are copied into the run upload area
3. a pipeline configuration is created
4. the corresponding workflow is executed
5. output files are written into a run directory
6. results are displayed in the interface

---

## Results and interactive tables

After a successful run, the interface shows:

- the run mode
- the paths of important output files
- preview tables for generated files

Depending on the workflow, result tabs may include:

- **Full Result**
- **Filtered**
- **Contributor**

The preview tables are interactive Marimo tables and are intended for quick inspection of the generated data.

Typical use:

- inspect whether the fetch worked as expected
- check whether filtering kept the right records
- review PI/staff matches

For large files, the interface previews only part of the data. In the current code, the preview loads up to **500 rows** per file.

---

## See also

- [Configuration](configuration.md)
- [Outputs and Troubleshooting](outputs_and_troubleshooting.md)
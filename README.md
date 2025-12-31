
# Web of Science (WoS) Fetch + Keyword/PI Filtering

This repository contains scripts to:
1. **Fetch WoS records via Clarivate Web of Science API** (Full Record view), then save a clean summary plus a raw **JSONL** dump.
2. **Filter the fetched records** using:
   - include/exclude keywords from a **YAML** file, and
   - PI name detection in funding/acknowledgement fields using a **PI list CSV**.

---
## What it does

### 1) Fetch WoS data
- Runs a WoS API query 
- Saves a clean table for downstream scanning
- Optionally saves debug/raw JSON for inspection

### 2) Scan fetched data
- Loads a WoS export file 
- Builds a searchable text from relevant columns 
- Finds rows that match:
  - **include_terms** (YAML)
  - NOT matching **exclude_terms** (YAML)
- Also checks PI names in funding/ack-related fields and outputs a separate file


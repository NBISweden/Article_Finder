# Article Finder

Article Finder helps identify publications that received support from PI or staff or used infrastructure.

It can:

- fetch metadata from the Web of Science API using a query
- fetch Web of Science metadata for an existing DOI list
- filter metadata using configurable keyword and category rules
- optionally detect PI and staff names in both authors and acknowledgment

The recommended entry point is the **Marimo user interface**.

---

## Quick start

Clone the repository and install the environment:

```bash
git clone https://github.com/NBISweden/Article_Finder.git
cd Article_Finder
pixi install
```

Start the Marimo interface:

```bash
pixi run ui
```
## Recommended workflow

Article Finder supports two fetch workflows:

1. **Fetch by WoS query**
2. **Fetch by DOI list**

For the most accurate results in this project, the recommended workflow is usually:

1. Fetch broadly, for example all Sweden publications for a given year
2. Apply local filtering in **Filter** mode using the keyword rules

This is generally preferable to relying on a very narrow WoS query alone, because local filtering gives more control and usually more accurate final results.

If you already have a DOI list from a previous year, you can instead upload the DOI file and fetch Web of Science metadata for each DOI.

## Documentation

- [User Guide](docs/user_guide.md)
- [API key setup](docs/api_key_setup.md)
- [Configuration](docs/configuration.md)
- [Outputs and Troubleshooting](docs/outputs_and_troubleshooting.md)
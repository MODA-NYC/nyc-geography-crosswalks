# NYC Geographic Crosswalks

This repository provides Python tools (scripts and notebooks) to generate comprehensive geographic crosswalk tables for various New York City (NYC) administrative and spatial boundaries. It aims to provide up-to-date intersection data derived directly from official sources.

## Acknowledgement & Evolution

This repository significantly builds upon the concepts and data aggregation methods originally implemented in the [BetaNYC NYC Boundaries Map repository](https://github.com/BetaNYC/nyc-boundaries). The core goal remains the same: understanding overlaps between NYC's complex administrative and spatial boundaries.

This project refactors and extends the original BetaNYC data processing approach by:
1.  **Migrating to Python:** Translating the data aggregation logic from Node.js (`shpjs`) to a pure Python framework using GeoPandas and related libraries.
2.  **Focusing on Up-to-Date Sources:** Implementing a process (`generate_all_bounds.py`) designed to fetch the latest available versions* of boundary files directly from official city data portals (NYC Open Data, DCP, EDC).
3.  **Pre-Calculating Crosswalks:** Shifting the focus from primarily supporting a real-time map backend to efficiently generating comprehensive, pre-calculated crosswalk flat files (both wide and long formats) suitable for analysis, distribution, and potentially powering other applications via files or an API.

Essentially, this repository takes the foundational data definitions and overlap concept from the BetaNYC project and adapts it into a Python-based pipeline optimized for generating robust, versioned crosswalk datasets.

---

## Key Features & Motivation

Understanding how different administrative and statistical boundaries overlap in NYC is crucial for city agencies, researchers, community boards, and the public. Building on the [original work by BetaNYC](https://github.com/BetaNYC/nyc-boundaries), this repository addresses this need by:

1.  **Consolidating Source Data:** A Python script (`generate_all_bounds.py`) downloads the latest available versions* of core NYC geographic boundaries from official sources (NYC Open Data, DCP, EDC) and aggregates them into a single, standardized GeoJSON file (`all_boundaries.geojson`). (This refactors the original Node.js approach into Python).
2.  **Generating Pre-Calculated Crosswalks:** Jupyter notebooks leverage the consolidated GeoJSON to perform spatial analysis (intersections with negative buffering) and generate detailed crosswalk tables in two formats:
    *   **Wide Format:** One CSV per primary geography, showing overlapping features from all other geographies in separate columns (ideal for quick lookups).
    *   **Long Format:** One CSV per primary geography, detailing every significant pairwise overlap with precise intersection area and percentage calculations (ideal for detailed analysis).
3.  **Providing User Tools:** Includes an interactive notebook (`Selector`) for generating custom crosswalks on the fly.
4.  **Enhancing Performance for Consumers:** The pre-calculated crosswalks can significantly speed up applications that need to display boundary overlaps, compared to performing real-time spatial queries.
5.  **Transparency:** Aims to provide clear metadata about the source data vintages used in each generated crosswalk set.

*   \* **Note on Versions:** Currently, the specific version links for source data (e.g., URLs containing `_25a` for data from NYC Planning's 2025 Cycle A update) are defined within the `generate_all_bounds.py` script. Future enhancements may automate the detection of the absolute latest versions.*

---

## Methodology Notes

- **ZIP Codes are MODZCTAs**: The `zipcode` layer uses DOHMH MODZCTA polygons (`modzcta`). MODZCTAs approximate USPS ZIP boundaries and may be multipart. Results may differ from USPS definitions. We document this explicitly to set expectations for users.

- **Geometry validity**: After combining all inputs, we check for invalid geometries and apply `buffer(0)` to attempt a non-destructive fix. This can slightly adjust slivers/holes but improves robustness of spatial operations.

- **Intersection de-noising (buffer) and thresholds**: Crosswalk calculations use a small negative buffer applied only during intersection to reduce line-touching artifacts. Candidate inclusion is not gated by this buffer. After intersection, we apply a minimum intersection-area threshold and an epsilon to suppress numerical noise. Defaults are documented in `scripts/build_crosswalks.py` and captured in `crosswalks_meta.json` per run.

- **Multipart primaries**: Primary features are first dissolved by `nameCol` so multipart geometries (e.g., some MODZCTAs) are treated as a single unit. This prevents duplicate rows for a given primary–other pair and yields correct percentages against the total primary area.

- **Per-run outputs and metadata**: Each run is saved under `outputs/<UTC timestamp>/` and includes:
  - `all_boundaries.geojson` and `run_meta.json` (source URLs, resolved cycles, config, git SHA)
  - `longform/` and `wide/` crosswalk CSVs and `crosswalks_meta.json` (thresholds and IDs used)
  These folders are ignored by git; publish as Release assets for distribution.

- **IBZ source access**: The EDC IBZ URL may be protected by a web challenge. The script attempts download and, on failure, falls back to a locally provided `data/external/ibz.zip`. For reproducibility, include this file when creating a Release for a given vintage.

---

## Repository Contents

*   **`scripts/generate_all_bounds.py`**: Python script to download source boundary data and create the base `all_boundaries_YYYYMMDD.geojson` file. **Run this first.** Includes geometry validity fixing.
*   **`NYC_Geographies_Generate_All_Wide_Crosswalks.ipynb`**: Thin wrapper that calls `scripts/build_crosswalks.py` for reproducible runs. Requires the output from `scripts/generate_all_bounds.py`.
*   **`NYC_Geographies_Generate_All_Long_Crosswalks.ipynb`**: Thin wrapper that calls `scripts/build_crosswalks.py` for reproducible runs. Requires the output from `scripts/generate_all_bounds.py`.
*   **`NYC_Geographies_Crosswalk_Selector.ipynb`**: Interactive Jupyter Notebook (best used in Google Colab) to generate *custom* wide or long-format crosswalks for user-selected primary and target geographies. Requires the output from `generate_all_bounds.py`.
*   **`.gitignore`**: Excludes virtual environments and potentially large generated data files from Git.
*   **`README.md`**: This file.

---

## Geography IDs and Abbreviations

The following table maps each geography to the short ID used throughout the code (see `generate_all_bounds.py`). Use these IDs when selecting or referencing geographies in scripts and notebooks.

| Geography | ID |
| --- | --- |
| Community Districts | `cd` |
| Police Precincts | `pp` |
| Sanitation Districts | `dsny` |
| Fire Battalions | `fb` |
| School Districts | `sd` |
| Health Center Districts | `hc` |
| City Council Districts | `cc` |
| Congressional Districts | `nycongress` |
| State Assembly Districts | `sa` |
| State Senate Districts | `ss` |
| Business Improvement Districts | `bid` |
| Neighborhood Tabulation Areas | `nta` |
| Zip Codes | `zipcode` |
| Historic Districts | `hd` |
| Industrial Business Zones | `ibz` |

Note: `cc_upcoming` has been retired and is no longer generated or included in outputs.

---

## Getting Started & Workflow

1.  **Clone the Repository:**
    ```bash
    git clone https://github.com/MODA-NYC/nyc-geography-crosswalks.git
    cd nyc-geography-crosswalks
    ```

2.  **Set up Python Environment:** (Recommended) Create and activate a virtual environment:
    ```bash
    python3 -m venv .venv
    source .venv/bin/activate # On Linux/macOS
    # .\venv\Scripts\activate # On Windows
    ```

3.  **Install Dependencies:**
    ```bash
    # Consider creating a requirements.txt file
    pip install geopandas pandas requests tqdm # Add ipywidgets google-colab-dependencies if using Selector in Colab
    ```
    *(Note: GeoPandas installation might require system dependencies like GDAL/GEOS/PROJ if wheels are not available for your system.)*

4.  **Generate Base GeoJSON:** Run the script to download sources and create the master boundary file. This file will be saved locally (default: `data/processed/all_boundaries_YYYYMMDD.geojson`). Specify a vintage label if desired.
    ```bash
    python scripts/generate_all_bounds.py # Add arguments if implemented, e.g., --vintage YYYYMMDD --output-base-dir ./output
    ```
    *(This step requires internet access and may take several minutes.)*

5.  **Generate Crosswalks (Choose one or more):**
    *   **Preferred:** `python scripts/make_run.py --zip-artifacts` (runs bounds + crosswalks; writes zips in the run folder)
    *   **Option A (All Wide):** Open and run the cells sequentially in `NYC_Geographies_Generate_All_Wide_Crosswalks.ipynb`. You will need to configure the input path in Cell 3 to point to the `all_boundaries_*.geojson` file created in Step 4 (e.g., via Google Drive mount if using Colab). Output: Zipped CSVs saved locally in Colab environment.
    *   **Option B (All Long):** Open and run the cells sequentially in `NYC_Geographies_Generate_All_Long_Crosswalks.ipynb`. Configure the input path in Cell 3. Output: Zipped CSVs saved locally in Colab environment.
    *   **Option C (Interactive Selector):** Open and run the cells sequentially in `NYC_Geographies_Crosswalk_Selector.ipynb` (Google Colab recommended). Configure the input path in Cell 3. Use the widgets to generate specific crosswalks on demand. Output: Individual CSV downloads via browser.

---

## Pre-Generated Data (Via GitHub Releases)

For users who do not wish to run the generation process themselves, pre-generated sets of the crosswalk files (corresponding to specific run dates/vintages) are available for download as ZIP archives from the **[Releases Page](https://github.com/MODA-NYC/nyc-geography-crosswalks/releases)** of this repository. Each release typically includes:

*   Wide-format crosswalk CSVs (one per primary geography).
*   Long-format crosswalk CSVs (one per primary geography).
*   A `metadata.json` file detailing the source data versions used for that specific crosswalk vintage.
*   The corresponding `all_boundaries_*.geojson` file used as input.

---

## API Access (Planned)

A future goal is to provide an API endpoint for accessing the latest generated crosswalk data programmatically. This API would return crosswalk results in JSON format and include metadata about the source data vintages. Details will be added here once available. Querying specific historical vintage combinations via the API is not currently planned but may be considered based on user needs (users needing specific historical data can currently download older data sets from the Releases page).

---

## Dependencies Summary

*   Python 3.x
*   GeoPandas & Dependencies (Shapely, Fiona, PyPROJ - often require GDAL, GEOS, PROJ system libraries)
*   Pandas
*   Requests
*   Tqdm (for progress bars)
*   ipywidgets (for `Selector` notebook)
*   google.colab (for notebooks in Colab environment)
*   zipfile, os (Standard Library)

---

## Maintainer

Nathan Storey - [github.com/npstorey](https://github.com/npstorey)

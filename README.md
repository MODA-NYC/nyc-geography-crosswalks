# NYC Geographies Crosswalk Generator

This repository provides Python notebooks that generate comprehensive geographic crosswalk tables for various New York City (NYC) administrative and spatial boundaries, leveraging the [BetaNYC `nyc-boundaries` GeoJSON dataset](https://github.com/BetaNYC/nyc-boundaries).

---

## What's Included?

### 1. Wide-Format Crosswalk Tables (`NYC_Geographies_Generate_All_Wide_Crosswalks.ipynb`)

Generates individual wide-format CSV files, each representing a specific NYC geography type. Each CSV includes rows representing individual geographic features, with columns showing overlaps from other geography types as semicolon-separated identifiers.

- **Example Use Cases:**
  - Quickly identifying geographic intersections (e.g., ZIP codes within a Community District).
  - Urban planning analysis.
  - Administrative boundary reporting.

### 2. Long-Form Crosswalk Tables (`NYC_Geographies_Generate_All_Longform_Crosswalks.ipynb`)

Generates detailed long-form CSV files capturing pairwise intersections between geographic features, including precise intersection area and percentage overlap calculations.

- **Example Use Cases:**
  - Detailed spatial analytics and overlap calculations.
  - GIS and spatial data analysis.
  - Advanced urban planning or research studies.

### 3. Interactive Selector (`NYC_Geographies_Crosswalk_Selector.ipynb`)

An interactive notebook allowing users to quickly create customized crosswalk tables based on their selections of primary and target geographies.

- **Example Use Cases:**
  - Rapid prototyping and exploratory analysis.
  - Interactive spatial data exploration.

---

## How It Works

Each notebook performs the following spatial analysis steps:

- **Data Acquisition**: Downloads latest NYC geographic boundaries from BetaNYC.
- **Spatial Operations**: Applies negative buffering and intersection-area filtering using GeoPandas to ensure only meaningful overlaps are included.
- **Crosswalk Generation**: Exports organized CSV files clearly documenting geographic relationships.

---

## Data Source

The geographic boundaries used in these notebooks are sourced directly from the [BetaNYC NYC Boundaries GeoJSON](https://github.com/BetaNYC/nyc-boundaries).

---

## Dependencies

Ensure you have Python installed with the following packages:

- `geopandas`
- `pandas`
- `requests`
- `shapely`
- `ipywidgets` (interactive notebook)
- `tqdm` (progress bars)

These dependencies can be installed using:

```bash
pip install geopandas pandas requests shapely ipywidgets tqdm


## Getting Started

1. Clone the repository:
git clone [your-repository-url]

2. Open notebooks in Google Colab or your local Jupyter environment.

3. Run notebook cells sequentially to generate crosswalk files.

## Outputs

Generated CSV files are automatically zipped and downloaded:
- all_geographies_wide_crosswalks.zip (Wide format)
- all_geographies_longform_crosswalks.zip (Long-form format)
- Custom CSV files (Interactive Selector notebook)

## Maintainer
Name: Nathan Storey
Contact: https://github.com/npstorey

import geopandas
import pandas as pd
import requests
import io
import logging
import os
from typing import Union, Tuple
import zipfile
import tempfile
import re
import json
from datetime import datetime, timezone
import subprocess

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Config ---
AUTO_DETECT_LATEST = True  # Attempt to auto-detect latest DCP cycle letters (e.g., 25b, 25c)
PREFERRED_CYCLE = None     # Optionally pin a cycle letter like 'c'; overrides auto if provided
EXTERNAL_DATA_DIR = os.path.join('data', 'external')  # Optional local fallbacks for protected sources

# --- Dataset Definitions (Add ALL datasets here eventually) ---
datasets = [
  {
    "id": "cd",
    "datasetName": "Community Districts",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/community-districts/nycd_25a.zip",
    "nameCol": "BoroCD",
    "nameAlt": None
  },
  {
    "id": "pp",
    "datasetName": "Police Precincts",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/police-precincts/nypp_25a.zip",
    "nameCol": "Precinct",
    "nameAlt": None
  },
  {
    "id": "dsny",
    "datasetName": "Sanitation Districts",
    "url": "https://data.cityofnewyork.us/api/geospatial/i6mn-amj2?method=export&format=Shapefile",
    "nameCol": "district",
    "nameAlt": "districtco"
  },
  {
    "id": "fb",
    "datasetName": "Fire Battalions",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/fire-battalions/nyfb_25a.zip",
    "nameCol": "FireBN",
    "nameAlt": None
  },
  {
    "id": "sd",
    "datasetName": "School Districts",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/school/nysd_25a.zip",
    "nameCol": "SchoolDist",
    "nameAlt": None
  },
  {
    "id": "hc",
    "datasetName": "Health Center Districts",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/health-center/nyhc_25a.zip",
    "nameCol": "HCentDist",
    "nameAlt": None
  },
  {
    "id": "cc",
    "datasetName": "City Council Districts",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/city-council/nycc_25a.zip",
    "nameCol": "CounDist",
    "nameAlt": None
  },
  {
    "id": "nycongress",
    "datasetName": "Congressional Districts",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/congressional/nycg_25a.zip",
    "nameCol": "CongDist",
    "nameAlt": None
  },
  {
    "id": "sa",
    "datasetName": "State Assembly Districts",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/state-assembly/nyad_25a.zip",
    "nameCol": "AssemDist",
    "nameAlt": None
  },
  {
    "id": "ss",
    "datasetName": "State Senate Districts",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/state-senate/nyss_25a.zip",
    "nameCol": "StSenDist",
    "nameAlt": None
  },
  {
    "id": "bid",
    "datasetName": "Business Improvement District",
    "url": "https://data.cityofnewyork.us/resource/7jdm-inj8.geojson",
    "nameCol": "f_all_bids",
    "nameAlt": None
  },
  {
    "id": "nta",
    "datasetName": "Neighborhood Tabulation Areas",
    "url": "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/neighborhood-tabulation-areas/nynta2020_25a.zip",
    "nameCol": "NTAName",
    "nameAlt": "NTA2020"
  },
  {
    "id": "zipcode",
    "datasetName": "Zip Codes",
    "url": "https://data.cityofnewyork.us/api/geospatial/pri4-ifjk?method=export&format=Shapefile",
    "nameCol": "modzcta",
    "nameAlt": None
  },
  {
    "id": "hd",
    "datasetName": "Historic Districts",
    "url": "https://data.cityofnewyork.us/resource/skyk-mpzq.geojson",
    "nameCol": "area_name",
    "nameAlt": None
  },
  {
    "id": "ibz",
    "datasetName": "Industrial Business Zones",
    "url": "https://edc.nyc/sites/default/files/2020-10/IBZ%20Shapefiles.zip",
    "nameCol": "NAME",
    "nameAlt": None
  }
]
def _resolve_latest_dcp_cycle(url: str) -> Tuple[str, dict]:
    """Resolve the latest available DCP cycle by testing higher letters via HTTP HEAD.

    Returns a tuple of (resolved_url, meta_dict)
    meta_dict contains: cycle_source, cycle_resolved, auto_detected, probes
    """
    meta = {
        "cycle_source": None,
        "cycle_resolved": None,
        "auto_detected": False,
        "probes": []
    }

    # Match final _NNx.zip (e.g., _25a.zip) pattern at end of URL
    m = re.search(r"_(\d{2})([a-z])\.zip$", url)
    if not m:
        return url, meta  # Not a standard DCP cycle URL; leave as-is

    cycle_num = m.group(1)
    cycle_letter = m.group(2)
    meta["cycle_source"] = f"{cycle_num}{cycle_letter}"

    # If caller pins a cycle, use it directly
    if PREFERRED_CYCLE and re.fullmatch(r"[a-z]", PREFERRED_CYCLE):
        preferred = PREFERRED_CYCLE.lower()
        candidate = re.sub(r"_(\d{2})([a-z])\.zip$", f"_{cycle_num}{preferred}.zip", url)
        meta["probes"].append({"url": candidate, "type": "preferred"})
        try:
            resp = requests.head(candidate, allow_redirects=True, timeout=10)
            if resp.status_code == 200:
                meta["cycle_resolved"] = f"{cycle_num}{preferred}"
                meta["auto_detected"] = False
                return candidate, meta
        except requests.exceptions.RequestException:
            pass  # fall back to autodetect if enabled

    if not AUTO_DETECT_LATEST:
        meta["cycle_resolved"] = meta["cycle_source"]
        return url, meta

    # Probe ascending letters from the current to 'z', taking highest that exists
    best_url = url
    best_letter = cycle_letter
    for letter_ord in range(ord(cycle_letter) + 1, ord('z') + 1):
        letter = chr(letter_ord)
        candidate = re.sub(r"_(\d{2})([a-z])\.zip$", f"_{cycle_num}{letter}.zip", url)
        meta["probes"].append({"url": candidate, "type": "autodetect"})
        try:
            resp = requests.head(candidate, allow_redirects=True, timeout=10)
            if resp.status_code == 200:
                best_url = candidate
                best_letter = letter
            else:
                # Non-200 likely means not yet published; keep last best
                pass
        except requests.exceptions.RequestException:
            # Network or timeout — ignore and keep last best
            pass

    meta["cycle_resolved"] = f"{cycle_num}{best_letter}"
    meta["auto_detected"] = best_letter != cycle_letter
    return best_url, meta


def process_dataset(dataset_info: dict) -> Union[Tuple[geopandas.GeoDataFrame, dict], Tuple[None, dict]]:
    """Downloads, reads, and standardizes a single dataset.

    Returns (gdf, meta) where meta captures URL resolution and status.
    """
    dataset_id = dataset_info['id']
    url = dataset_info['url']
    name_col_key = dataset_info['nameCol']
    name_alt_key = dataset_info.get('nameAlt') # Use .get for safety

    logging.info(f"Processing dataset: {dataset_id} ({dataset_info['datasetName']}) from {url}")

    gdf = None # Initialize gdf
    meta = {
        "id": dataset_id,
        "original_url": url,
        "resolved_url": url,
        "cycle": None,
        "auto_detected": False,
        "probes": [],
        "status": "pending",
        "error": None
    }

    # Resolve potential newer DCP cycle
    resolved_url = url
    cycle_meta = {}
    if url.endswith('.zip') and 'dcp' in url and '/bytes/' in url:
        try:
            resolved_url, cycle_meta = _resolve_latest_dcp_cycle(url)
            if resolved_url != url:
                logging.info(f"Resolved newer cycle for {dataset_id}: {url} -> {resolved_url} ({cycle_meta.get('cycle_resolved')})")
            else:
                logging.info(f"Using original cycle for {dataset_id}: {cycle_meta.get('cycle_source')}")
        except Exception as e:
            logging.warning(f"Cycle resolution failed for {dataset_id}. Proceeding with original URL. Error: {e}")
            resolved_url = url
    meta["resolved_url"] = resolved_url
    if cycle_meta:
        meta["cycle"] = cycle_meta.get("cycle_resolved") or cycle_meta.get("cycle_source")
        meta["auto_detected"] = cycle_meta.get("auto_detected", False)
        meta["probes"] = cycle_meta.get("probes", [])

    # --- Read GeoJSON directly if URL ends with .geojson ---
    if url.lower().endswith('.geojson'):
        logging.info(f"Attempting to read GeoJSON directly from URL: {url}")
        try:
            gdf = geopandas.read_file(url)
            logging.info(f"Successfully read GeoJSON for {dataset_id} directly from URL.")
            # Proceed directly to Reproject step
        except Exception as e:
            logging.error(f"Failed to read GeoJSON directly from {url}. Error: {e}")
            meta["status"] = "geojson_read_error"
            meta["error"] = str(e)
            return None, meta # Exit if direct GeoJSON read fails
    else:
        # --- Download and Process Zip (existing logic) ---
        logging.info(f"URL does not end with .geojson, attempting zip download and processing for {dataset_id}")
        # --- Download ---
        try:
            response = requests.get(resolved_url, stream=True, timeout=60) # Added timeout
            response.raise_for_status()
            zip_content = io.BytesIO(response.content)
            logging.info(f"Downloaded zip for {dataset_id} successfully.")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to download zip for {dataset_id} from {resolved_url}. Error: {e}")
            # --- Local fallback for protected or blocked sources ---
            fallback_zip_path = None
            try:
                os.makedirs(EXTERNAL_DATA_DIR, exist_ok=True)
            except Exception:
                pass
            # Heuristic: look for a pre-downloaded file named after dataset id
            # e.g., data/external/ibz.zip or a directory data/external/ibz/
            candidate_files = [
                os.path.join(EXTERNAL_DATA_DIR, f"{dataset_id}.zip"),
                os.path.join(EXTERNAL_DATA_DIR, f"{dataset_id}.ZIP"),
            ]
            for cpath in candidate_files:
                if os.path.isfile(cpath):
                    fallback_zip_path = cpath
                    break
            if fallback_zip_path:
                logging.warning(f"Using local fallback for {dataset_id}: {fallback_zip_path}")
                try:
                    with open(fallback_zip_path, 'rb') as lf:
                        zip_content = io.BytesIO(lf.read())
                    # proceed as if downloaded
                except Exception as le:
                    logging.error(f"Failed reading local fallback for {dataset_id}: {le}")
                    meta["status"] = "download_error"
                    meta["error"] = str(e)
                    return None, meta
            else:
                meta["status"] = "download_error"
                meta["error"] = str(e)
                return None, meta

        # --- Read Shapefile from Zip ---  (MODIFIED LOGIC)
        try:
            zip_content.seek(0)  # Ensure buffer is at the start

            with tempfile.TemporaryDirectory() as temp_dir:
                logging.info(f"Extracting zip contents for {dataset_id} to temporary directory: {temp_dir}")
                shapefile_path_in_zip = None
                extracted_shp_path = None

                try:
                    with zipfile.ZipFile(zip_content, 'r') as zip_ref:
                        # Find the .shp file within the zip, potentially in a subdirectory
                        for member in zip_ref.namelist():
                            if member.lower().endswith('.shp'):
                                shapefile_path_in_zip = member
                                logging.info(f"Found shapefile inside zip: {shapefile_path_in_zip}")
                                break  # Assume first .shp file found is the correct one

                        if not shapefile_path_in_zip:
                            logging.error(f"No .shp file found inside the zip archive for {dataset_id}.")
                            meta["status"] = "no_shp_in_zip"
                            meta["error"] = "No .shp file in zip"
                            return None, meta

                        # Extract necessary files (.shp, .dbf, .shx, potentially .prj)
                        shp_basename = os.path.splitext(shapefile_path_in_zip)[0]
                        required_extensions = ['.shp', '.dbf', '.shx', '.prj'] # .prj is often needed for CRS
                        extracted_files_count = 0
                        for member in zip_ref.namelist():
                            # Handle potential subdirectories: compare basenames relative to their directory
                            member_dir = os.path.dirname(member)
                            member_basename_rel = os.path.splitext(os.path.basename(member))[0]
                            shp_basename_rel = os.path.splitext(os.path.basename(shapefile_path_in_zip))[0]
                            member_ext = os.path.splitext(member)[1]

                            # Check if file is in the same (potentially root) directory and has the same base name
                            if os.path.dirname(shapefile_path_in_zip) == member_dir and \
                               member_basename_rel == shp_basename_rel and \
                               member_ext.lower() in required_extensions:

                                # Extract preserving filename
                                zip_ref.extract(member, path=temp_dir)
                                logging.debug(f"Extracted {member} to {temp_dir}")
                                extracted_files_count += 1
                                if member.lower().endswith('.shp'):
                                    # Construct the full path to the extracted .shp file
                                    extracted_shp_path = os.path.join(temp_dir, member)

                        if not extracted_shp_path:
                            logging.error(f"Shapefile was found ({shapefile_path_in_zip}) but could not determine extracted path in {temp_dir}.")
                            meta["status"] = "extracted_shp_missing"
                            meta["error"] = "Extracted .shp path not determined"
                            return None, meta

                        if extracted_files_count < 3: # Need at least .shp, .shx, .dbf
                            logging.warning(f"Extracted only {extracted_files_count} files for {shp_basename}. Reading might fail.")

                        logging.info(f"Attempting to read extracted shapefile: {extracted_shp_path}")
                        gdf = geopandas.read_file(extracted_shp_path)
                        logging.info(f"Successfully read {dataset_id} from extracted shapefile.")

                except zipfile.BadZipFile:
                    logging.error(f"Invalid or corrupted zip file for {dataset_id}.")
                    meta["status"] = "bad_zip"
                    meta["error"] = "BadZipFile"
                    return None, meta
                except FileNotFoundError:
                    logging.error(f"Extracted shapefile path not found after extraction: {extracted_shp_path}")
                    meta["status"] = "extract_path_missing"
                    meta["error"] = "Extracted path missing"
                    return None, meta
                except Exception as extract_read_e:
                    # Catch potential geopandas read errors here too
                    logging.error(f"Error during zip extraction or reading extracted file for {dataset_id}. Error: {extract_read_e}")
                    meta["status"] = "extract_or_read_error"
                    meta["error"] = str(extract_read_e)
                    return None, meta
            # End of with tempfile.TemporaryDirectory() - temp_dir is automatically cleaned up here

        except Exception as e:
            # This outer exception is less likely now, but kept as a failsafe
            logging.error(f"Unexpected error during shapefile processing setup for {dataset_id}. Error: {e}")
            meta["status"] = "unexpected_shapefile_error"
            meta["error"] = str(e)
            return None, meta

        # If gdf is still None after the zip processing block, handle error
        if gdf is None:
            logging.error(f"Failed to obtain GeoDataFrame for {dataset_id} via zip processing.")
            meta["status"] = "no_gdf_post_zip"
            meta["error"] = "gdf is None after zip processing"
            return None, meta
    # --- END of if/else for GeoJSON vs Zip ---

    # --- Reproject (Runs for both GeoJSON and Shapefile reads) ---
    if gdf is None: # Should ideally not happen if logic above is correct, but safety check
        logging.error(f"GDF is None before reprojection for {dataset_id}. Cannot proceed.")
        meta["status"] = "no_gdf_pre_reproject"
        meta["error"] = "gdf None before reprojection"
        return None, meta

    try:
        if gdf.crs is None:
            # Try to infer CRS if possible, otherwise assume 4326 if read from GeoJSON, or 2263 if likely from shapefile
            default_crs = 'EPSG:4326' if url.lower().endswith('.geojson') else 'EPSG:2263'
            logging.warning(f"CRS is missing for {dataset_id}. Assuming {default_crs} and attempting reproject to EPSG:4326.")
            try:
                 gdf = gdf.set_crs(default_crs, allow_override=True).to_crs('EPSG:4326')
                 logging.info(f"Assumed {default_crs} and reprojected {dataset_id} to EPSG:4326.")
            except Exception as crs_e:
                 logging.error(f"Failed to assume and reproject CRS for {dataset_id}. Error: {crs_e}. Setting to None.")
                 meta["status"] = "crs_reproject_error"
                 meta["error"] = str(crs_e)
                 return None, meta # Cannot proceed reliably without CRS
        elif gdf.crs != 'EPSG:4326':
            logging.info(f"Reprojecting {dataset_id} from {gdf.crs} to EPSG:4326.")
            gdf = gdf.to_crs('EPSG:4326')
        else:
            logging.info(f"{dataset_id} is already in EPSG:4326.")
    except Exception as e:
        logging.error(f"Error during reprojection for {dataset_id}. Error: {e}")
        meta["status"] = "reproject_error"
        meta["error"] = str(e)
        return None, meta

    # --- Standardize Properties ---
    try:
        # Check if 'geometry' column exists, needed for GeoDataFrame
        if 'geometry' not in gdf.columns:
            # Sometimes GeoJSON might use a different name, like 'the_geom'
            geom_col_found = None
            potential_geom_cols = ['the_geom', 'geom', 'shape'] # Add others if needed
            for col in potential_geom_cols:
                if col in gdf.columns:
                    logging.warning(f"Geometry column named '{col}' found for {dataset_id}, renaming to 'geometry'.")
                    gdf = gdf.rename(columns={col: 'geometry'})
                    gdf = gdf.set_geometry('geometry') # Ensure it's the active geometry column
                    geom_col_found = True
                    break
            if not geom_col_found:
                logging.error(f"'geometry' column (or recognized alternative) not found in {dataset_id} after reading. Columns: {gdf.columns.tolist()}")
                meta["status"] = "geometry_column_missing"
                meta["error"] = "geometry column not found"
                return None, meta

        processed_gdf = geopandas.GeoDataFrame(geometry=gdf.geometry, crs='EPSG:4326')
        processed_gdf['id'] = dataset_id

        # Add nameCol
        if name_col_key not in gdf.columns:
            logging.warning(f"nameCol '{name_col_key}' not found in {dataset_id}. Columns: {gdf.columns.tolist()}. Setting column to None.")
            processed_gdf['nameCol'] = None
        else:
            processed_gdf['nameCol'] = gdf[name_col_key].fillna('').astype(str)

        # Add nameAlt (conditionally)
        final_columns = ['id', 'nameCol', 'geometry']
        if name_alt_key and name_alt_key in gdf.columns:
            logging.info(f"Found nameAlt column '{name_alt_key}' for {dataset_id}.")
            processed_gdf['nameAlt'] = gdf[name_alt_key].fillna('').astype(str)
            final_columns.insert(2, 'nameAlt') # Insert 'nameAlt' before 'geometry'

        # Filter to essential columns
        processed_gdf = processed_gdf[final_columns]
        logging.info(f"Standardized properties for {dataset_id}.")

    except KeyError as e:
         logging.error(f"KeyError during property standardization for {dataset_id}. Missing column: {e}. Available: {gdf.columns.tolist()}")
         meta["status"] = "standardize_key_error"
         meta["error"] = str(e)
         return None, meta
    except Exception as e:
         logging.error(f"Unexpected error during property standardization for {dataset_id}. Error: {e}")
         meta["status"] = "standardize_unexpected_error"
         meta["error"] = str(e)
         return None, meta

    meta["status"] = "ok"
    return processed_gdf, meta

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("Starting dataset processing...")

    # --- Create output directory if it doesn't exist ---
    output_dir = "data/processed"
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Ensured output directory exists: {output_dir}")

    all_processed_gdfs = []
    failed_datasets = []
    run_meta = {
        "run_id": datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M%S_UTC"),
        "git_sha": None,
        "config": {
            "auto_detect_latest": AUTO_DETECT_LATEST,
            "preferred_cycle": PREFERRED_CYCLE,
            "target_crs": "EPSG:4326"
        },
        "datasets": []
    }

    # Try to capture current git SHA
    try:
        sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
        run_meta["git_sha"] = sha
    except Exception:
        run_meta["git_sha"] = None

    # --- Process ALL datasets ---
    for dataset_info in datasets:
        logging.info("\n--- Processing dataset: %s ---", dataset_info['id'])
        result_gdf, meta = process_dataset(dataset_info)
        run_meta["datasets"].append({
            "id": meta.get("id"),
            "original_url": meta.get("original_url"),
            "resolved_url": meta.get("resolved_url"),
            "cycle": meta.get("cycle"),
            "auto_detected": meta.get("auto_detected"),
            "status": meta.get("status"),
            "error": meta.get("error"),
        })

        if result_gdf is not None:
            logging.info(f"Successfully processed {dataset_info['id']}.")
            print(f"\n--- Preview for {dataset_info['id']} ---")
            print(result_gdf.head())
            all_processed_gdfs.append(result_gdf)

             # --- Save individual GeoJSON ---
            output_path = os.path.join(output_dir, f"{dataset_info['id']}.geojson")
            try:
                result_gdf.to_file(output_path, driver='GeoJSON')
                logging.info(f"Saved {dataset_info['id']} to {output_path}")
            except Exception as e:
                logging.error(f"Failed to save {dataset_info['id']} to GeoJSON. Error: {e}")
                failed_datasets.append(f"{dataset_info['id']} (Save Error: {e})")

        else:
            logging.error(f"--- Processing failed for {dataset_info['id']} ---")
            failed_datasets.append(dataset_info['id'] + " (Processing Error)")


    # --- Combine all processed datasets ---
    if all_processed_gdfs:
        logging.info("\n--- Combining all successfully processed datasets ---")
        combined_gdf = pd.concat(all_processed_gdfs, ignore_index=True)
        combined_gdf = geopandas.GeoDataFrame(combined_gdf, crs='EPSG:4326') # Ensure it's still a GeoDataFrame

        logging.info("Combined GeoDataFrame Info:")
        print(combined_gdf.info())
        print("\nCombined GeoDataFrame Head:")
        print(combined_gdf.head())
        print("\nCombined GeoDataFrame Tail:")
        print(combined_gdf.tail())

        # --- Fix Invalid Geometries ---
        logging.info("Checking for and fixing invalid geometries...")
        invalid_before = combined_gdf[~combined_gdf.geometry.is_valid]
        if not invalid_before.empty:
            logging.warning(f"Found {len(invalid_before)} invalid geometries before fix. Applying .buffer(0).")
            # Apply buffer(0) - this often fixes minor validity issues
            try:
                combined_gdf['geometry'] = combined_gdf.buffer(0)
                # Check validity again after buffering
                invalid_after = combined_gdf[~combined_gdf.geometry.is_valid]
                if not invalid_after.empty:
                     logging.warning(f"Found {len(invalid_after)} invalid geometries remaining after applying .buffer(0). Manual review may be needed.")
                else:
                     logging.info("All geometries appear valid after applying .buffer(0).")
            except Exception as buffer_err:
                logging.error(f"Error applying .buffer(0) to fix geometries: {buffer_err}. Proceeding without fix.")
        else:
            logging.info("No invalid geometries found.")
        # --- End Fix Invalid Geometries ---

        # --- Save Combined GeoJSON ---
        # Create timestamped run directory for outputs and metadata
        run_dir = os.path.join("outputs", run_meta["run_id"]) 
        os.makedirs(run_dir, exist_ok=True)

        combined_output_path = os.path.join(run_dir, "all_boundaries.geojson")
        try:
            combined_gdf.to_file(combined_output_path, driver='GeoJSON')
            logging.info(f"Saved combined boundaries to {combined_output_path}")
        except Exception as e:
            logging.error(f"Failed to save combined boundaries GeoJSON. Error: {e}")
            failed_datasets.append(f"all_boundaries.geojson (Save Error: {e})")

        # Also save/overwrite a convenience copy under data/processed for notebook users
        latest_copy_path = os.path.join("data", "processed", "all_boundaries.geojson")
        try:
            combined_gdf.to_file(latest_copy_path, driver='GeoJSON')
            logging.info(f"Saved convenience copy to {latest_copy_path}")
        except Exception as e:
            logging.error(f"Failed to save convenience copy of all_boundaries.geojson. Error: {e}")

        # Write run metadata
        meta_path = os.path.join(run_dir, "run_meta.json")
        try:
            with open(meta_path, "w") as f:
                json.dump(run_meta, f, indent=2)
            logging.info(f"Saved run metadata to {meta_path}")
        except Exception as e:
            logging.error(f"Failed to write run_meta.json. Error: {e}")
    else:
        logging.warning("No datasets were processed successfully. Cannot create combined file.")


    # --- Summary ---
    logging.info("\n--- Processing Summary ---")
    total_datasets = len(datasets)
    successful_count = len(all_processed_gdfs)
    failed_count = len(failed_datasets)
    logging.info(f"Total datasets defined: {total_datasets}")
    logging.info(f"Successfully processed and saved: {successful_count}")
    logging.info(f"Failed datasets ({failed_count}): {', '.join(failed_datasets) if failed_datasets else 'None'}")

    logging.info("Finished dataset processing.")
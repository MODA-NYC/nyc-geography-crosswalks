import argparse
import json
import logging
import os
from typing import Dict, Iterable, List, Optional, Set, Tuple

import geopandas as gpd
import pandas as pd
from shapely.geometry.base import BaseGeometry


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def dissolve_by_name(geodf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Dissolve features by 'nameCol' keeping the first non-geo columns.

    Returns a GeoDataFrame with columns: id, nameCol, geometry (dissolved)
    """
    if geodf.empty:
        return geodf.copy()

    # geopandas >= 0.10 supports aggfunc in dissolve
    dissolved = geodf.dissolve(by='nameCol', as_index=False, aggfunc='first')
    # Ensure id is preserved (same across this subset)
    if 'id' in geodf.columns and 'id' not in dissolved.columns:
        dissolved['id'] = geodf['id'].iloc[0]
    return dissolved[['id', 'nameCol', 'geometry']]


def union_by_name(geodf: gpd.GeoDataFrame) -> List[Tuple[str, BaseGeometry]]:
    """Return list of (nameCol, unioned geometry) for the given subset."""
    if geodf.empty:
        return []
    grouped = []
    for name, group in geodf.groupby('nameCol'):
        # Use union_all if available (shapely >= 2.0 via geodataframe), else unary_union
        try:
            union_geom = group.geometry.union_all()
        except AttributeError:
            union_geom = group.geometry.unary_union
        grouped.append((str(name), union_geom))
    return grouped


def build_longform_for_primary(
    all_gdf_2263: gpd.GeoDataFrame,
    primary_id: str,
    other_ids: Iterable[str],
    buffer_feet: float,
    min_intersection_area_final: float,
    epsilon: float,
    max_primaries: Optional[int] = None,
) -> pd.DataFrame:
    """Create long-form crosswalk for a single primary geography type.

    - Dissolve primary by nameCol to avoid multi-part duplicates
    - Use buffer only in intersection op if buffer_feet != 0 (de-noising), not for candidate inclusion
    - Apply post-intersection min area threshold
    """
    primary_src = all_gdf_2263[all_gdf_2263['id'] == primary_id].copy()
    if primary_src.empty:
        logging.warning("No features for primary id=%s", primary_id)
        return pd.DataFrame()

    primary_dissolved = dissolve_by_name(primary_src)
    if max_primaries is not None and max_primaries > 0:
        primary_dissolved = primary_dissolved.head(max_primaries)

    # Spatial index for candidate preselection by bounds (not filtered by buffer area)
    sindex = all_gdf_2263.sindex

    records: List[Dict] = []
    for _, prow in primary_dissolved.iterrows():
        p_name = str(prow['nameCol'])
        p_geom_orig = prow.geometry
        if p_geom_orig is None or p_geom_orig.is_empty:
            continue
        p_area = float(p_geom_orig.area)
        if p_area <= epsilon:
            continue
        # De-noise geometry during intersection only (optional)
        p_geom_for_intersection = p_geom_orig.buffer(buffer_feet) if buffer_feet else p_geom_orig

        # Bounds candidate preselection
        candidate_idx = list(sindex.intersection(p_geom_orig.bounds))
        candidate_gdf = all_gdf_2263.iloc[candidate_idx]

        for other_id in other_ids:
            if other_id == primary_id:
                continue
            subset = candidate_gdf[candidate_gdf['id'] == other_id].copy()
            if subset.empty:
                continue

            for t_name, t_union in union_by_name(subset):
                if t_union is None or getattr(t_union, 'is_empty', False):
                    continue
                inter_geom = p_geom_for_intersection.intersection(t_union)
                inter_area = float(inter_geom.area) if not inter_geom.is_empty else 0.0
                if inter_area <= max(min_intersection_area_final, epsilon):
                    continue
                perc = (inter_area / p_area) * 100.0
                records.append({
                    'Primary Geography ID': primary_id,
                    'Primary Geography NameCol': p_name,
                    'Other Geography ID': other_id,
                    'Other Geography NameCol': str(t_name),
                    'Primary Area (sq ft)': p_area,
                    'Intersection Area (sq ft)': inter_area,
                    'Percentage Overlap': perc,
                })

    if not records:
        return pd.DataFrame()
    df = pd.DataFrame(records)
    df = df.sort_values(by=["Primary Geography NameCol", "Other Geography ID", "Percentage Overlap"], ascending=[True, True, False])
    return df


def build_wide_for_primary(
    all_gdf_2263: gpd.GeoDataFrame,
    primary_id: str,
    other_ids: Iterable[str],
    buffer_feet: float,
    min_intersection_area_final: float,
    epsilon: float,
    max_primaries: Optional[int] = None,
) -> pd.DataFrame:
    """Create wide-format crosswalk for a single primary geography type using dissolved primaries and post-intersection thresholds."""
    primary_src = all_gdf_2263[all_gdf_2263['id'] == primary_id].copy()
    if primary_src.empty:
        return pd.DataFrame()

    primary_dissolved = dissolve_by_name(primary_src)
    if max_primaries is not None and max_primaries > 0:
        primary_dissolved = primary_dissolved.head(max_primaries)

    sindex = all_gdf_2263.sindex

    rows: List[Dict] = []
    for _, prow in primary_dissolved.iterrows():
        p_name = str(prow['nameCol'])
        p_geom_orig = prow.geometry
        if p_geom_orig is None or p_geom_orig.is_empty:
            continue
        p_geom_for_intersection = p_geom_orig.buffer(buffer_feet) if buffer_feet else p_geom_orig

        candidate_idx = list(sindex.intersection(p_geom_orig.bounds))
        candidate_gdf = all_gdf_2263.iloc[candidate_idx]

        record: Dict[str, str] = {primary_id: p_name}
        for other_id in other_ids:
            if other_id == primary_id:
                continue
            subset = candidate_gdf[candidate_gdf['id'] == other_id].copy()
            keep_names: List[str] = []
            for t_name, t_union in union_by_name(subset):
                inter_geom = p_geom_for_intersection.intersection(t_union)
                inter_area = float(inter_geom.area) if not inter_geom.is_empty else 0.0
                if inter_area > max(min_intersection_area_final, epsilon):
                    keep_names.append(str(t_name))
            record[other_id] = ";".join(sorted(set(keep_names))) if keep_names else ""

        rows.append(record)

    if not rows:
        return pd.DataFrame()
    # Order columns: primary first, then sorted others
    cols = [primary_id] + sorted([g for g in other_ids if g != primary_id])
    df = pd.DataFrame(rows)
    df = df[cols]
    return df


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build crosswalk CSVs (longform and wide) from unified boundaries.")
    p.add_argument("--boundaries", required=True, help="Path to all_boundaries.geojson produced by generate_all_bounds.py")
    p.add_argument("--run-dir", required=True, help="Output run directory, e.g., outputs/<run-id>/")
    p.add_argument("--buffer-feet", type=float, default=-50.0, help="Negative buffer applied only in intersection calculations")
    p.add_argument("--min-area-final", type=float, default=100.0, help="Minimum intersection area (sq ft) to include a pair")
    p.add_argument("--epsilon", type=float, default=1e-6, help="Tiny epsilon area to suppress numeric noise")
    p.add_argument("--exclude-ids", nargs='*', default=['cc_upcoming'], help="Geography IDs to exclude entirely")
    p.add_argument("--primary-only", nargs='*', default=None, help="If provided, build only for these primary IDs")
    p.add_argument("--targets", nargs='*', default=None, help="If provided, limit target geography IDs to this set")
    p.add_argument("--max-primaries", type=int, default=None, help="Limit number of primary features (for smoke tests)")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    logging.info("Loading boundaries from %s", args.boundaries)
    gdf = gpd.read_file(args.boundaries)
    if gdf.crs is None or gdf.crs.to_string().upper() != 'EPSG:2263':
        logging.info("Reprojecting to EPSG:2263 for area computations...")
        gdf = gdf.to_crs(epsg=2263)

    # Determine geography IDs
    all_ids: List[str] = sorted(list({str(v) for v in gdf['id'].unique()}))
    ids: List[str] = [gid for gid in all_ids if gid not in set(args.exclude_ids)]

    if args.primary_only:
        primary_ids = [gid for gid in args.primary_only if gid in ids]
    else:
        primary_ids = ids.copy()

    if args.targets:
        target_ids = [gid for gid in args.targets if gid in ids]
    else:
        target_ids = ids.copy()

    long_dir = os.path.join(args.run_dir, 'longform')
    wide_dir = os.path.join(args.run_dir, 'wide')
    ensure_dir(long_dir)
    ensure_dir(wide_dir)

    meta = {
        'buffer_feet': args.buffer_feet,
        'min_intersection_area_final': args.min_area_final,
        'epsilon': args.epsilon,
        'exclude_ids': args.exclude_ids,
        'primary_ids': primary_ids,
        'target_ids': target_ids,
    }

    # Build per primary
    for primary_id in primary_ids:
        logging.info("Building longform for primary=%s", primary_id)
        lf = build_longform_for_primary(
            all_gdf_2263=gdf,
            primary_id=primary_id,
            other_ids=target_ids,
            buffer_feet=args.buffer_feet,
            min_intersection_area_final=args.min_area_final,
            epsilon=args.epsilon,
            max_primaries=args.max_primaries,
        )
        if not lf.empty:
            lf_path = os.path.join(long_dir, f"longform_{primary_id}_crosswalk.csv")
            lf.to_csv(lf_path, index=False)
            logging.info("Saved longform: %s (%d rows)", lf_path, len(lf))
        else:
            logging.info("No longform rows produced for primary=%s", primary_id)

        logging.info("Building wide for primary=%s", primary_id)
        wf = build_wide_for_primary(
            all_gdf_2263=gdf,
            primary_id=primary_id,
            other_ids=target_ids,
            buffer_feet=args.buffer_feet,
            min_intersection_area_final=args.min_area_final,
            epsilon=args.epsilon,
            max_primaries=args.max_primaries,
        )
        if not wf.empty:
            wf_path = os.path.join(wide_dir, f"wide_{primary_id}_crosswalk.csv")
            wf.to_csv(wf_path, index=False)
            logging.info("Saved wide: %s (%d rows)", wf_path, len(wf))
        else:
            logging.info("No wide rows produced for primary=%s", primary_id)

    # Write a small meta file alongside run_meta.json
    xmeta_path = os.path.join(args.run_dir, 'crosswalks_meta.json')
    with open(xmeta_path, 'w') as f:
        json.dump(meta, f, indent=2)
    logging.info("Saved crosswalks meta to %s", xmeta_path)


if __name__ == '__main__':
    main()



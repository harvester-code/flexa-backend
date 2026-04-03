"""
Passenger Timeline Builder

Transforms simulation-pax.parquet data into a compact per-passenger
timeline format optimized for 3D animation playback on the frontend.
"""

from typing import Any, Dict, List, Optional

import pandas as pd
from loguru import logger


def _get_process_list(df: pd.DataFrame) -> List[str]:
    return [
        col.replace("_on_pred", "")
        for col in df.columns
        if col.endswith("_on_pred")
    ]


def _auto_generate_zone_positions(
    pax_df: pd.DataFrame,
    process_list: List[str],
) -> Dict[str, Dict[str, float]]:
    """Generate automatic zone positions when metadata has no zoneAreas.

    Lays out steps as rows (top to bottom) and zones within each step
    as columns (left to right). Zone widths are proportional to their
    facility count so dense zones (e.g. check-in with 20 counters) are wider.
    """
    step_zones: Dict[str, List[str]] = {}
    zone_fac_count: Dict[str, int] = {}

    for step_idx, proc in enumerate(process_list):
        zone_col = f"{proc}_zone"
        fac_col = f"{proc}_facility"
        if zone_col in pax_df.columns:
            zones = sorted(pax_df[zone_col].dropna().unique().tolist())
            step_zones[proc] = zones if zones else ["default"]
        else:
            step_zones[proc] = ["default"]

        if zone_col in pax_df.columns and fac_col in pax_df.columns:
            for zn in step_zones[proc]:
                key = f"{step_idx}:{zn}"
                mask = pax_df[zone_col] == zn
                n_fac = pax_df.loc[mask, fac_col].dropna().nunique()
                zone_fac_count[key] = max(n_fac, 1)
        else:
            for zn in step_zones[proc]:
                zone_fac_count[f"{step_idx}:{zn}"] = 1

    n_steps = len(process_list)
    if n_steps == 0:
        return {}

    centers: Dict[str, Dict[str, float]] = {}
    margin = 0.05
    usable = 1.0 - 2 * margin
    gap = 0.01

    for step_idx, proc in enumerate(process_list):
        zones = step_zones[proc]
        n_zones = len(zones)
        if n_zones == 0:
            continue
        row_y = margin + (step_idx + 0.5) / n_steps * usable

        keys = [f"{step_idx}:{zn}" for zn in zones]
        weights = [zone_fac_count.get(k, 1) for k in keys]
        total_weight = sum(weights)
        total_gap = gap * max(n_zones - 1, 0)
        available = usable - total_gap

        cursor_x = margin
        for z_idx, zone_name in enumerate(zones):
            w_frac = weights[z_idx] / total_weight if total_weight > 0 else 1.0 / n_zones
            zone_w = available * w_frac
            zone_h = usable / n_steps * 0.6
            col_x = cursor_x + zone_w / 2

            centers[f"{step_idx}:{zone_name}"] = {
                "x": round(col_x, 4),
                "y": round(row_y, 4),
                "w": round(zone_w, 4),
                "h": round(zone_h, 4),
            }
            cursor_x += zone_w + gap

    logger.info(
        f"Auto-generated positions for {len(centers)} zones across {n_steps} steps "
        f"(facility-weighted widths)"
    )
    return centers


def _extract_travel_minutes(
    process_flow: Optional[List[dict]],
    process_list: List[str],
) -> Dict[str, float]:
    """Build a map from process name to travel_time_minutes from config."""
    travel = {p: 0.0 for p in process_list}
    if not process_flow:
        return travel

    for step_cfg in process_flow:
        name = step_cfg.get("name", "")
        if name in travel:
            travel[name] = float(step_cfg.get("travel_time_minutes", 0))
    return travel


def build_passenger_timelines(
    pax_df: pd.DataFrame,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the compact passenger-timeline payload.

    Returns a dict ready to be serialised as JSON with keys:
        base_time, duration_seconds, steps, zones, passengers
    """
    process_list = _get_process_list(pax_df)
    if not process_list:
        logger.warning("No process columns found in parquet – returning empty timeline")
        return {
            "base_time": None,
            "duration_seconds": 0,
            "steps": [],
            "zones": {},
            "passengers": [],
        }

    logger.info(f"Building timeline for {len(pax_df):,} passengers, processes={process_list}")

    process_flow = None
    if metadata:
        process_flow = metadata.get("process_flow")
        if not isinstance(process_flow, list):
            process_flow = None

    travel_map = _extract_travel_minutes(process_flow, process_list)
    zone_centers = _auto_generate_zone_positions(pax_df, process_list)

    has_show_up = "show_up_time" in pax_df.columns

    global_min = pd.Timestamp.max
    global_max = pd.Timestamp.min

    time_cols = []
    if has_show_up:
        time_cols.append("show_up_time")
    for proc in process_list:
        for suffix in ("_on_pred", "_done_time"):
            col = f"{proc}{suffix}"
            if col in pax_df.columns:
                time_cols.append(col)

    for col in time_cols:
        series = pd.to_datetime(pax_df[col], errors="coerce").dropna()
        if len(series) > 0:
            col_min = series.min()
            col_max = series.max()
            if col_min < global_min:
                global_min = col_min
            if col_max > global_max:
                global_max = col_max

    if global_min >= global_max:
        global_min = pd.Timestamp("2025-01-01")
        global_max = global_min + pd.Timedelta(hours=24)

    base_time = global_min.floor("h")
    duration_seconds = int((global_max - base_time).total_seconds()) + 60

    steps_out = []
    for proc in process_list:
        steps_out.append({
            "name": proc,
            "travel_minutes": travel_map.get(proc, 0),
        })

    # Build zone_facilities mapping: "step_idx:zone_name" -> sorted facility IDs
    zone_facilities: Dict[str, List[str]] = {}
    for step_idx, proc in enumerate(process_list):
        zone_col = f"{proc}_zone"
        fac_col = f"{proc}_facility"
        if zone_col in pax_df.columns and fac_col in pax_df.columns:
            grouped = (
                pax_df[[zone_col, fac_col]]
                .dropna()
                .drop_duplicates()
                .groupby(zone_col)[fac_col]
                .apply(lambda x: sorted(x.astype(str).unique().tolist()))
            )
            for zone_name, fac_list in grouped.items():
                zone_facilities[f"{step_idx}:{zone_name}"] = [
                    f"{step_idx}:{fid}" for fid in fac_list
                ]

    logger.info(f"Collected facilities for {len(zone_facilities)} zones")

    passengers_out: List[Any] = []

    for _, row in pax_df.iterrows():
        # show_up_offset: seconds from base_time when passenger enters airport
        show_up_off = -1
        if has_show_up:
            su = row.get("show_up_time")
            if pd.notna(su):
                show_up_off = int((pd.Timestamp(su) - base_time).total_seconds())

        pax_events: List[Any] = []

        for step_idx, proc in enumerate(process_list):
            on_pred_col = f"{proc}_on_pred"
            start_col = f"{proc}_start_time"
            done_col = f"{proc}_done_time"
            zone_col = f"{proc}_zone"
            fac_col = f"{proc}_facility"
            status_col = f"{proc}_status"

            status = row.get(status_col)
            if status in (None, "skipped", "failed") or pd.isna(status):
                pax_events.append(None)
                continue

            on_pred = row.get(on_pred_col)
            start_time = row.get(start_col)
            done_time = row.get(done_col)
            zone = row.get(zone_col) if zone_col in row.index else None
            facility = row.get(fac_col) if fac_col in row.index else None

            if pd.isna(on_pred) or pd.isna(done_time):
                pax_events.append(None)
                continue

            on_pred_ts = pd.Timestamp(on_pred)
            start_ts = pd.Timestamp(start_time) if pd.notna(start_time) else on_pred_ts
            done_ts = pd.Timestamp(done_time)

            on_off = int((on_pred_ts - base_time).total_seconds())
            st_off = int((start_ts - base_time).total_seconds())
            dn_off = int((done_ts - base_time).total_seconds())

            raw_zone = str(zone) if zone and not pd.isna(zone) else ""
            zone_str = f"{step_idx}:{raw_zone}" if raw_zone else ""
            raw_fac = str(facility) if facility and not pd.isna(facility) else ""
            fac_str = f"{step_idx}:{raw_fac}" if raw_fac else ""
            pax_events.append([on_off, st_off, dn_off, zone_str, fac_str])

        passengers_out.append([show_up_off, pax_events])

    logger.info(
        f"Timeline built: {len(passengers_out):,} passengers, "
        f"base_time={base_time}, duration={duration_seconds}s"
    )

    return {
        "base_time": base_time.isoformat(),
        "duration_seconds": duration_seconds,
        "steps": steps_out,
        "zones": zone_centers,
        "zone_facilities": zone_facilities,
        "passengers": passengers_out,
    }

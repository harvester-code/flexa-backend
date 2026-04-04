"""
Passenger Timeline Builder

Transforms simulation-pax.parquet data into a compact per-passenger
timeline format optimized for 3D animation playback on the frontend.
"""

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from loguru import logger


def _get_process_list(df: pd.DataFrame) -> List[str]:
    return [
        col.replace("_on_pred", "")
        for col in df.columns
        if col.endswith("_on_pred")
    ]


def _compute_zone_max_queue(
    pax_df: pd.DataFrame,
    process_list: List[str],
) -> Dict[str, int]:
    """Compute the peak concurrent queue size per zone.

    Uses a numpy event-sweep for each facility.  The raw values may be
    very large (thousands) for bottleneck or block-waiting scenarios;
    callers should apply damping (e.g. log2) before using these for
    layout sizing.
    """
    zone_max: Dict[str, int] = {}

    for step_idx, proc in enumerate(process_list):
        on_pred_col = f"{proc}_on_pred"
        start_col = f"{proc}_start_time"
        zone_col = f"{proc}_zone"
        fac_col = f"{proc}_facility"

        needed = [on_pred_col, start_col, fac_col]
        if not all(c in pax_df.columns for c in needed):
            continue

        has_zone = zone_col in pax_df.columns
        cols = needed + ([zone_col] if has_zone else [])
        sub = pax_df[cols].dropna()

        on_pred = pd.to_datetime(sub[on_pred_col], errors="coerce")
        start = pd.to_datetime(sub[start_col], errors="coerce")
        mask = on_pred.notna() & start.notna() & (on_pred < start)
        if not mask.any():
            continue

        # Normalize to datetime64[ns] before int64 conversion.
        # Parquet may store on_pred as datetime64[us] and start_time as
        # datetime64[ns]; raw .astype("int64") would then give values in
        # different units (µs vs ns), making all arrivals sort before all
        # departures in the event sweep and producing wildly inflated peaks.
        on_vals = on_pred[mask].values.astype("datetime64[ns]").astype("int64")
        st_vals = start[mask].values.astype("datetime64[ns]").astype("int64")
        fac_vals = sub.loc[mask, fac_col].values
        zone_vals = sub.loc[mask, zone_col].values if has_zone else np.full(mask.sum(), "default")

        for fac_id in np.unique(fac_vals):
            fm = fac_vals == fac_id
            arrivals = on_vals[fm]
            departures = st_vals[fm]

            times = np.concatenate([arrivals, departures])
            deltas = np.concatenate([np.ones(len(arrivals)), -np.ones(len(departures))])
            order = np.argsort(times, kind="mergesort")
            max_q = int(np.maximum.accumulate(deltas[order].cumsum()).max())

            key = f"{step_idx}:{zone_vals[fm][0]}"
            zone_max[key] = max(zone_max.get(key, 0), max_q)

    return zone_max


def _auto_generate_zone_positions(
    pax_df: pd.DataFrame,
    process_list: List[str],
    zone_max_queue: Optional[Dict[str, int]] = None,
) -> Dict[str, Dict[str, float]]:
    """Generate automatic zone positions when metadata has no zoneAreas.

    Lays out steps as rows (top to bottom) and zones within each step
    as columns (left to right). Zone widths are proportional to their
    facility count so dense zones (e.g. check-in with 20 counters) are wider.
    Step heights are proportional to their max queue depth so steps with
    long queues get more vertical space.
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

    zmq = zone_max_queue or {}

    # Per-step height weights based on max queue depth.
    # log2 damping compresses extreme ratios so that a step with
    # queue 3000 is only ~1.5x taller than one with queue 100.
    MIN_WEIGHT = 5.0
    step_weights: List[float] = []
    for step_idx, proc in enumerate(process_list):
        step_max = 0
        for zn in step_zones[proc]:
            step_max = max(step_max, zmq.get(f"{step_idx}:{zn}", 0))
        est_cols = 3
        queue_rows = max(step_max / est_cols, 0)
        dampened = float(np.log2(queue_rows + 2)) * 10
        step_weights.append(max(dampened, MIN_WEIGHT))

    total_weight_y = sum(step_weights)

    centers: Dict[str, Dict[str, float]] = {}
    margin = 0.05
    usable = 1.0 - 2 * margin
    gap = 0.01

    cursor_y = margin
    for step_idx, proc in enumerate(process_list):
        zones = step_zones[proc]
        n_zones = len(zones)
        if n_zones == 0:
            continue

        step_h_frac = usable * step_weights[step_idx] / total_weight_y
        row_y = cursor_y + step_h_frac / 2
        cursor_y += step_h_frac

        keys = [f"{step_idx}:{zn}" for zn in zones]
        weights = [zone_fac_count.get(k, 1) for k in keys]
        total_weight = sum(weights)
        total_gap = gap * max(n_zones - 1, 0)
        available = usable - total_gap

        cursor_x = margin
        for z_idx, zone_name in enumerate(zones):
            w_frac = weights[z_idx] / total_weight if total_weight > 0 else 1.0 / n_zones
            zone_w = available * w_frac
            zone_h = step_h_frac * 0.85
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
        f"(facility-weighted widths, queue-weighted heights)"
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


def _extract_facility_schedules(
    process_flow: Optional[List[dict]],
    base_time: "pd.Timestamp",
) -> Dict[str, List[List]]:
    """Extract per-facility operating schedules as offsets from base_time.

    Returns ``{composite_fac_id: [[start_sec, end_sec, activate], ...]}``
    where composite_fac_id uses the ``step_idx:fac_id`` format.
    """
    schedules: Dict[str, List[List]] = {}
    if not process_flow:
        return schedules

    for step_idx, step_cfg in enumerate(process_flow):
        zones = step_cfg.get("zones")
        if not zones or not isinstance(zones, dict):
            continue
        for _zone_name, zone_data in zones.items():
            if not isinstance(zone_data, dict):
                continue
            facilities = zone_data.get("facilities", [])
            for fac in facilities:
                fac_id = fac.get("id", "")
                if not fac_id:
                    continue
                composite_id = f"{step_idx}:{fac_id}"
                time_blocks = fac.get("operating_schedule", {}).get("time_blocks", [])
                blocks_out: List[List] = []
                for block in time_blocks:
                    period = block.get("period", "")
                    activate = block.get("activate", True)
                    if len(period) > 19 and period[19] == "-":
                        start_str = period[:19]
                        end_str = period[20:]
                        try:
                            s_dt = pd.to_datetime(start_str)
                            e_dt = pd.to_datetime(end_str)
                            s_off = int((s_dt - base_time).total_seconds())
                            e_off = int((e_dt - base_time).total_seconds())
                            blocks_out.append([s_off, e_off, activate])
                        except Exception:
                            continue
                if blocks_out:
                    schedules[composite_id] = blocks_out
    return schedules


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
    zone_max_queue = _compute_zone_max_queue(pax_df, process_list)
    zone_centers = _auto_generate_zone_positions(pax_df, process_list, zone_max_queue)

    if zone_max_queue:
        logger.info(f"Max queue depths: {zone_max_queue}")

    # Will be populated after base_time is computed
    facility_schedules: Dict[str, List[List]] = {}

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

    facility_schedules = _extract_facility_schedules(process_flow, base_time)
    if facility_schedules:
        logger.info(f"Extracted operating schedules for {len(facility_schedules)} facilities")

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

    # ── Vectorised passenger building (replaces iterrows) ──────────
    n_pax = len(pax_df)
    n_steps = len(process_list)

    # Pre-compute show_up offsets
    if has_show_up:
        su_ts = pd.to_datetime(pax_df["show_up_time"], errors="coerce")
        su_offsets = np.where(
            su_ts.notna(),
            ((su_ts - base_time).dt.total_seconds()).values,
            -1,
        ).astype(np.int64)
    else:
        su_offsets = np.full(n_pax, -1, dtype=np.int64)

    # Pre-compute arrays for each step (all vectorised)
    step_valid: List[np.ndarray] = []
    step_on: List[np.ndarray] = []
    step_st: List[np.ndarray] = []
    step_dn: List[np.ndarray] = []
    step_zone: List[np.ndarray] = []
    step_fac: List[np.ndarray] = []

    _empty_str = np.full(n_pax, "", dtype=object)

    for step_idx, proc in enumerate(process_list):
        status_col = f"{proc}_status"
        on_pred_col = f"{proc}_on_pred"
        start_col = f"{proc}_start_time"
        done_col = f"{proc}_done_time"
        zone_col = f"{proc}_zone"
        fac_col = f"{proc}_facility"

        # Valid mask
        if status_col in pax_df.columns:
            s = pax_df[status_col]
            valid = (~s.isin(["skipped", "failed"])) & s.notna()
        else:
            valid = pd.Series(False, index=pax_df.index)

        on_pred = (
            pd.to_datetime(pax_df[on_pred_col], errors="coerce")
            if on_pred_col in pax_df.columns
            else pd.Series(pd.NaT, index=pax_df.index)
        )
        done_time = (
            pd.to_datetime(pax_df[done_col], errors="coerce")
            if done_col in pax_df.columns
            else pd.Series(pd.NaT, index=pax_df.index)
        )
        start_time = (
            pd.to_datetime(pax_df[start_col], errors="coerce")
            if start_col in pax_df.columns
            else pd.Series(pd.NaT, index=pax_df.index)
        )

        valid = valid & on_pred.notna() & done_time.notna()
        vmask = valid.values

        on_sec = ((on_pred - base_time).dt.total_seconds()).fillna(0).values.astype(np.int64)
        start_filled = start_time.fillna(on_pred)
        st_sec = ((start_filled - base_time).dt.total_seconds()).fillna(0).values.astype(np.int64)
        dn_sec = ((done_time - base_time).dt.total_seconds()).fillna(0).values.astype(np.int64)

        prefix = f"{step_idx}:"
        if zone_col in pax_df.columns:
            z_raw = pax_df[zone_col].fillna("").astype(str).values
            z_strs = np.array(
                [prefix + v if v and v != "nan" else "" for v in z_raw],
                dtype=object,
            )
        else:
            z_strs = _empty_str

        if fac_col in pax_df.columns:
            f_raw = pax_df[fac_col].fillna("").astype(str).values
            f_strs = np.array(
                [prefix + v if v and v != "nan" else "" for v in f_raw],
                dtype=object,
            )
        else:
            f_strs = _empty_str

        step_valid.append(vmask)
        step_on.append(on_sec)
        step_st.append(st_sec)
        step_dn.append(dn_sec)
        step_zone.append(z_strs)
        step_fac.append(f_strs)

    # Assemble output (simple array indexing – no pandas per-row)
    passengers_out: List[Any] = [None] * n_pax
    for i in range(n_pax):
        events = [None] * n_steps
        for s in range(n_steps):
            if step_valid[s][i]:
                events[s] = [
                    int(step_on[s][i]),
                    int(step_st[s][i]),
                    int(step_dn[s][i]),
                    step_zone[s][i],
                    step_fac[s][i],
                ]
        passengers_out[i] = [int(su_offsets[i]), events]

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
        "facility_schedules": facility_schedules,
        "zone_max_queue": zone_max_queue,
        "passengers": passengers_out,
    }

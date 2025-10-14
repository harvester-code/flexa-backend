from __future__ import annotations

from typing import Dict, List, Tuple, Optional

import pandas as pd

from app.routes.new_home.domain.chart import (
    FacilityChart,
    FacilityChartSummary,
    TimeSeriesData,
)


def _validate_columns(df: pd.DataFrame, columns: List[str]) -> None:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise ValueError(f"Required columns missing: {missing}")


def _get_step_config(process_flow: List[dict], step_name: str) -> dict:
    for step in process_flow:
        if step.get("name") == step_name:
            return step
    raise ValueError(f"Step '{step_name}' not found in process flow")


def _get_facility_config(step_config: dict, facility_id: str) -> Tuple[str, str, dict]:
    for zone_id, zone in step_config.get("zones", {}).items():
        for facility in zone.get("facilities", []):
            if facility.get("id") == facility_id:
                zone_name = zone.get("name") or zone_id
                return zone_id, zone_name, facility
    raise ValueError(f"Facility '{facility_id}' not found in step '{step_config.get('name')}'")


def _prepare_time_range(
    facility_df: pd.DataFrame,
    time_columns: Tuple[str, str, str],
    interval_minutes: int,
) -> pd.DatetimeIndex:
    timestamps: List[pd.Series] = []
    for col in time_columns:
        series = pd.to_datetime(facility_df[col], errors="coerce")
        series = series.dropna()
        if not series.empty:
            timestamps.append(series)
    if not timestamps:
        now = pd.Timestamp.utcnow().floor(f"{interval_minutes}min")
        return pd.date_range(now, now, freq=f"{interval_minutes}min")

    min_ts = min(series.min() for series in timestamps).floor(f"{interval_minutes}min")
    max_ts = max(series.max() for series in timestamps).ceil(f"{interval_minutes}min")
    return pd.date_range(min_ts, max_ts, freq=f"{interval_minutes}min")


def _calculate_slot_counts(
    facility_df: pd.DataFrame,
    datetime_col: str,
    time_range: pd.DatetimeIndex,
    interval_minutes: int,
) -> Tuple[List[int], List[pd.Series]]:
    dt_series = pd.to_datetime(facility_df[datetime_col], errors="coerce")
    counts: List[int] = []
    masks: List[pd.Series] = []
    for start in time_range:
        end = start + pd.Timedelta(minutes=interval_minutes)
        mask = (dt_series >= start) & (dt_series < end)
        masks.append(mask)
        counts.append(int(mask.sum()))
    return counts, masks


def _calculate_series_by_group(
    facility_df: pd.DataFrame,
    masks: List[pd.Series],
    group_col: str,
    labels: pd.Index,
) -> Dict[str, List[int]]:
    grouped: Dict[str, List[int]] = {}
    for slot_idx, mask in enumerate(masks):
        slot_df = facility_df.loc[mask]
        if slot_df.empty:
            continue
        for value, count in slot_df[group_col].value_counts().items():
            grouped.setdefault(value, [0] * len(labels))
            grouped[value][slot_idx] = int(count)
    return grouped


def _calculate_capacity_for_slot(
    facility_config: dict,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> float:
    slot_capacity = 0.0
    for block in facility_config.get("operating_schedule", {}).get("time_blocks", []):
        if not block.get("activate", True):
            continue

        period = block.get("period", "")
        if len(period) > 19 and period[19] == "-":
            start_str = period[:19]
            end_str = period[20:]
        else:
            try:
                start_str, end_str = period.split(" - ")
            except ValueError:
                continue

        block_start = pd.to_datetime(start_str.strip(), errors="coerce")
        block_end = pd.to_datetime(end_str.strip(), errors="coerce")
        if pd.isna(block_start) or pd.isna(block_end):
            continue

        if start >= block_end or end <= block_start:
            continue

        overlap_start = max(start, block_start)
        overlap_end = min(end, block_end)
        overlap_minutes = max((overlap_end - overlap_start).total_seconds() / 60.0, 0)
        if overlap_minutes == 0:
            continue

        process_time_seconds = block.get("process_time_seconds")
        if not process_time_seconds:
            continue

        capacity_per_hour = 3600.0 / process_time_seconds
        slot_capacity += (overlap_minutes / 60.0) * capacity_per_hour
    return slot_capacity


def _calculate_capacity_series(
    facility_config: dict,
    time_range: pd.DatetimeIndex,
    interval_minutes: int,
) -> List[float]:
    capacities: List[float] = []
    for start in time_range:
        end = start + pd.Timedelta(minutes=interval_minutes)
        capacities.append(_calculate_capacity_for_slot(facility_config, start, end))
    return capacities


def _build_facility_info_text(facility_config: dict) -> str:
    parts: List[str] = []
    for block in facility_config.get("operating_schedule", {}).get("time_blocks", []):
        if not block.get("activate", True):
            continue
        period = block.get("period", "")
        process_time = block.get("process_time_seconds")
        if "-" in period and len(period) > 19:
            start_label = period[11:16]
            end_label = period[31:36]
            window = f"{start_label}-{end_label}"
        else:
            window = period
        capacity_per_hour = int(3600 / process_time) if process_time else 0
        conditions = block.get("passenger_conditions", [])
        condition_texts: List[str] = []
        for condition in conditions:
            field = condition.get("field")
            values = condition.get("values", [])
            if field == "operating_carrier_iata":
                condition_texts.append("/".join(values))
            elif field == "profile":
                condition_texts.append("/".join(values))
        suffix = f" ({' '.join(condition_texts)})" if condition_texts else ""
        parts.append(f"{window} 운영, {capacity_per_hour}명/시간{suffix}")
    return " | ".join(parts) if parts else "시설 운영 정보 없음"


def build_facility_chart(
    pax_df: pd.DataFrame,
    process_flow: List[dict],
    step_name: str,
    facility_id: str,
    interval_minutes: int = 60,
) -> FacilityChart:
    facility_col = f"{step_name}_facility"
    start_time_col = f"{step_name}_start_time"
    done_time_col = f"{step_name}_done_time"
    on_pred_col = f"{step_name}_on_pred"

    required_columns = [
        facility_col,
        start_time_col,
        done_time_col,
        on_pred_col,
        "operating_carrier_name",
    ]
    _validate_columns(pax_df, required_columns)

    has_carrier_code = "operating_carrier_iata" in pax_df.columns

    facility_df = pax_df[pax_df[facility_col] == facility_id].copy()
    if facility_df.empty:
        raise ValueError(f"No passengers processed by facility '{facility_id}' in step '{step_name}'")

    name_series = facility_df["operating_carrier_name"].fillna("")
    name_series = name_series.astype(str).str.strip()

    if has_carrier_code:
        code_series = facility_df["operating_carrier_iata"].fillna("")
    else:
        code_series = None

    facility_df["airline_label"] = (
        name_series
        if code_series is None
        else name_series.where(name_series != "", code_series)
    )
    facility_df.loc[facility_df["airline_label"].isin(["", "nan", "None"]), "airline_label"] = "Unknown"

    time_range = _prepare_time_range(
        facility_df,
        (on_pred_col, start_time_col, done_time_col),
        interval_minutes,
    )

    inflow_totals, inflow_masks = _calculate_slot_counts(
        facility_df,
        on_pred_col,
        time_range,
        interval_minutes,
    )
    outflow_totals, outflow_masks = _calculate_slot_counts(
        facility_df,
        start_time_col,
        time_range,
        interval_minutes,
    )

    inflow_by_airline = _calculate_series_by_group(
        facility_df,
        inflow_masks,
        "airline_label",
        time_range,
    )
    outflow_by_airline = _calculate_series_by_group(
        facility_df,
        outflow_masks,
        "airline_label",
        time_range,
    )

    all_airlines = sorted(set(inflow_by_airline.keys()) | set(outflow_by_airline.keys()))
    inflow_series = [
        TimeSeriesData(label=f"{airline} 유입", values=inflow_by_airline.get(airline, [0] * len(time_range)))
        for airline in all_airlines
    ]
    outflow_series = [
        TimeSeriesData(label=f"{airline} 유출", values=outflow_by_airline.get(airline, [0] * len(time_range)))
        for airline in all_airlines
    ]

    step_config = _get_step_config(process_flow, step_name)
    zone_id, zone_name, facility_config = _get_facility_config(step_config, facility_id)
    capacity = _calculate_capacity_series(
        facility_config,
        time_range,
        interval_minutes,
    )

    bottleneck_times = [
        time_range[idx].floor('s').isoformat()
        for idx, (inflow, cap) in enumerate(zip(inflow_totals, capacity))
        if inflow > cap and cap > 0
    ]

    summary = FacilityChartSummary(
        total_inflow=int(sum(inflow_totals)),
        total_outflow=int(sum(outflow_totals)),
        max_capacity=float(max(capacity) if capacity else 0.0),
        average_capacity=float(sum(capacity) / len(capacity) if capacity else 0.0),
        bottleneck_times=bottleneck_times,
    )

    return FacilityChart(
        step=step_name,
        facility_id=facility_id,
        zone_id=zone_id,
        zone_name=zone_name,
        interval_minutes=interval_minutes,
        time_range=[ts.floor('s').isoformat() for ts in time_range],
        capacity=[float(val) for val in capacity],
        inflow_series=inflow_series,
        outflow_series=outflow_series,
        total_inflow=[int(val) for val in inflow_totals],
        total_outflow=[int(val) for val in outflow_totals],
        facility_info=_build_facility_info_text(facility_config),
        summary=summary,
    )

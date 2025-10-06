from __future__ import annotations

import re
from typing import Dict, List, Literal

import pandas as pd

from dependency_injector.wiring import inject

from app.routes.new_home.application.core.facility_chart import build_facility_chart
from app.routes.new_home.infra.repository import NewHomeRepository


class NewHomeService:

    @inject
    def __init__(self, repository: NewHomeRepository):
        self.repository = repository

    async def _load_process_flow(self, scenario_id: str) -> List[dict]:
        metadata = await self.repository.load_metadata(scenario_id)
        if not metadata:
            raise ValueError("시나리오 메타데이터를 찾을 수 없습니다.")

        process_flow = metadata.get("process_flow")
        if not process_flow:
            raise ValueError("메타데이터에 process_flow 정보가 없습니다.")
        if not isinstance(process_flow, list):
            raise ValueError("process_flow 데이터 형식이 올바르지 않습니다.")
        return process_flow

    async def get_facility_chart(
        self,
        scenario_id: str,
        step_name: str,
        facility_id: str,
        interval_minutes: int = 60,
    ) -> Dict:
        pax_df = await self.repository.load_passenger_dataframe(scenario_id)
        if pax_df is None or pax_df.empty:
            raise ValueError("시뮬레이션 승객 데이터를 불러올 수 없습니다.")

        process_flow = await self._load_process_flow(scenario_id)

        chart = build_facility_chart(
            pax_df=pax_df,
            process_flow=process_flow,
            step_name=step_name,
            facility_id=facility_id,
            interval_minutes=interval_minutes,
        )
        return chart.to_dict()

    async def get_all_facility_charts(
        self,
        scenario_id: str,
        interval_minutes: int = 60,
    ) -> Dict[str, List[Dict]]:
        pax_df = await self.repository.load_passenger_dataframe(scenario_id)
        if pax_df is None or pax_df.empty:
            raise ValueError("시뮬레이션 승객 데이터를 불러올 수 없습니다.")

        process_flow = await self._load_process_flow(scenario_id)

        charts_by_step: List[Dict] = []
        for step in process_flow:
            step_name = step.get("name")
            if not step_name:
                continue

            facility_col = f"{step_name}_facility"
            if facility_col not in pax_df.columns:
                continue

            processed_df = pax_df[pax_df[facility_col].notna()]
            if processed_df.empty:
                continue

            facility_ids = _sort_facilities(processed_df[facility_col].dropna().unique().tolist())

            facility_charts: List[Dict] = []
            for facility_id in facility_ids:
                try:
                    chart = build_facility_chart(
                        pax_df=pax_df,
                        process_flow=process_flow,
                        step_name=step_name,
                        facility_id=facility_id,
                        interval_minutes=interval_minutes,
                    )
                except ValueError:
                    continue
                facility_charts.append(chart.to_dict())

            if facility_charts:
                charts_by_step.append(
                    {
                        "step": step_name,
                        "facilityCharts": facility_charts,
                    }
                )

        return {"steps": charts_by_step}

    async def get_passenger_summary(
        self,
        scenario_id: str,
        top_n: int = 10,
    ) -> Dict[str, Dict[str, List[Dict[str, object]]]]:
        pax_df = await self.repository.load_passenger_dataframe(scenario_id)
        if pax_df is None or pax_df.empty:
            raise ValueError("시뮬레이션 승객 데이터를 불러올 수 없습니다.")

        pax_df = pax_df.copy()

        total_passengers = int(len(pax_df))

        flight_dates = (
            pax_df["flight_date"].dropna().astype(str).sort_values().unique().tolist()
        )

        show_up_times = pd.to_datetime(pax_df["show_up_time"], errors="coerce")
        show_up_start = show_up_times.min()
        show_up_end = show_up_times.max()

        summary = {
            "totals": {
                "passengers": total_passengers,
                "flightDates": flight_dates,
                "showUpWindow": {
                    "start": show_up_start.isoformat() if pd.notna(show_up_start) else None,
                    "end": show_up_end.isoformat() if pd.notna(show_up_end) else None,
                },
            },
            "dimensions": {
                "carrier": self._summarize_carriers(pax_df, top_n),
                "city": self._summarize_arrivals(pax_df, level="city", top_n=top_n),
                "country": self._summarize_arrivals(pax_df, level="country", top_n=top_n),
            },
        }

        return summary

    def _summarize_carriers(self, pax_df: pd.DataFrame, top_n: int) -> List[Dict[str, object]]:
        carrier_group = (
            pax_df.groupby("operating_carrier_name")
            .agg(
                passengers=("operating_carrier_name", "size"),
                flights=("flight_number", "nunique"),
                destinations=("arrival_airport_iata", "nunique"),
            )
            .reset_index()
        )

        destination_breakdown = (
            pax_df.groupby(
                [
                    "operating_carrier_name",
                    "arrival_airport_iata",
                    "arrival_city",
                    "arrival_country",
                ]
            )
            .size()
            .reset_index(name="passengers")
        )

        top_destinations = (
            destination_breakdown.sort_values("passengers", ascending=False)
            .groupby("operating_carrier_name")
            .first()
            .reset_index()
        )

        carrier_group = carrier_group.merge(
            top_destinations,
            on="operating_carrier_name",
            how="left",
            suffixes=("", "_top"),
        )

        carrier_group["top_destination_share"] = (
            carrier_group["passengers_top"] / carrier_group["passengers"]
        ).fillna(0)

        carrier_group = carrier_group.sort_values("passengers", ascending=False).head(top_n)

        records: List[Dict[str, object]] = []
        for _, row in carrier_group.iterrows():
            records.append(
                {
                    "label": row.get("operating_carrier_name"),
                    "passengers": int(row.get("passengers", 0)),
                    "flights": int(row.get("flights", 0)),
                    "destinations": int(row.get("destinations", 0)),
                    "topDestination": {
                        "airport": row.get("arrival_airport_iata"),
                        "city": row.get("arrival_city"),
                        "country": row.get("arrival_country"),
                        "passengers": int(row.get("passengers_top", 0) or 0),
                        "share": round(float(row.get("top_destination_share", 0) or 0), 4),
                    },
                }
            )

        return records

    def _summarize_arrivals(
        self,
        pax_df: pd.DataFrame,
        level: Literal["city", "country"],
        top_n: int,
    ) -> List[Dict[str, object]]:
        if level == "city":
            group_cols = ["arrival_country", "arrival_city"]

            def label_func(row: pd.Series) -> Dict[str, object]:
                return {"label": row.get("arrival_city"), "country": row.get("arrival_country")}

        else:
            group_cols = ["arrival_country"]

            def label_func(row: pd.Series) -> Dict[str, object]:
                value = row.get("arrival_country")
                return {"label": value, "country": value}

        grouped = (
            pax_df.groupby(group_cols)
            .agg(
                passengers=("arrival_airport_iata", "size"),
                airports=("arrival_airport_iata", "nunique"),
            )
            .reset_index()
            .sort_values("passengers", ascending=False)
            .head(top_n)
        )

        records: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            base = label_func(row)
            base.update(
                {
                    "passengers": int(row.get("passengers", 0)),
                    "airports": int(row.get("airports", 0)),
                }
            )
            records.append(base)

        return records


def _sort_facilities(facilities: List[str]) -> List[str]:
    def sort_key(value: str):
        match = re.match(r"([A-Za-z_]+)(?:_(\d+))?", value)
        if match:
            prefix = match.group(1)
            number = match.group(2)
            return prefix, int(number) if number else -1
        return value, -1

    return sorted(facilities, key=sort_key)

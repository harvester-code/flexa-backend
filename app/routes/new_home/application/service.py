from __future__ import annotations

import re
from typing import Dict, List, Literal

import pandas as pd

from dependency_injector.wiring import inject

from app.routes.new_home.application.core.facility_chart import build_facility_chart
from app.routes.new_home.domain.aircraft_reference import get_aircraft_class, get_aircraft_metadata
from app.routes.new_home.infra.repository import NewHomeRepository


DEFAULT_INTERVAL_MINUTES = 60
DEFAULT_TOP_N = 10


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

    async def _load_passenger_dataframe(self, scenario_id: str) -> pd.DataFrame:
        pax_df = await self.repository.load_passenger_dataframe(scenario_id)
        if pax_df is None or pax_df.empty:
            raise ValueError("시뮬레이션 승객 데이터를 불러올 수 없습니다.")
        return pax_df

    def _build_facility_charts(
        self,
        pax_df: pd.DataFrame,
        process_flow: List[dict],
        interval_minutes: int,
    ) -> Dict[str, List[Dict]]:
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

    def _build_passenger_summary(self, pax_df: pd.DataFrame, top_n: int) -> Dict[str, Dict[str, List[Dict[str, object]]]]:
        working_df = pax_df.copy()

        total_passengers = int(len(working_df))

        flight_dates = (
            working_df["flight_date"].dropna().astype(str).sort_values().unique().tolist()
        )

        show_up_times = pd.to_datetime(working_df["show_up_time"], errors="coerce")
        show_up_start = show_up_times.min()
        show_up_end = show_up_times.max()

        return {
            "totals": {
                "passengers": total_passengers,
                "flightDates": flight_dates,
                "showUpWindow": {
                    "start": show_up_start.floor('s').isoformat() if pd.notna(show_up_start) else None,
                    "end": show_up_end.floor('s').isoformat() if pd.notna(show_up_end) else None,
                },
            },
            "dimensions": {
                "carrier": self._summarize_carriers(working_df, top_n),
                "city": self._summarize_arrivals(working_df, level="city", top_n=top_n),
                "country": self._summarize_arrivals(working_df, level="country", top_n=top_n),
            },
        }

    def _build_flight_summary(self, pax_df: pd.DataFrame, top_n: int) -> Dict[str, object]:
        flights_df = pax_df.dropna(subset=["flight_number"]).copy()
        if flights_df.empty:
            return {
                "totals": {
                    "flights": 0,
                    "passengers": 0,
                    "carriers": 0,
                    "dateRange": [],
                    "firstDeparture": None,
                    "lastDeparture": None,
                },
                "hours": [],
                "carriers": [],
                "flights": [],
            }

        flights_df["scheduled_departure_local"] = pd.to_datetime(
            flights_df["scheduled_departure_local"], errors="coerce"
        )
        flights_df["scheduled_arrival_local"] = pd.to_datetime(
            flights_df["scheduled_arrival_local"], errors="coerce"
        )

        group_cols = [
            "flight_number",
            "operating_carrier_name",
            "operating_carrier_iata",
            "scheduled_departure_local",
            "scheduled_arrival_local",
            "departure_airport_iata",
            "departure_airport_icao",
            "departure_city",
            "departure_country",
            "arrival_airport_iata",
            "arrival_airport_icao",
            "arrival_city",
            "arrival_country",
            "aircraft_type_iata",
            "aircraft_type_icao",
            "flight_date",
        ]

        flights_group = (
            flights_df.groupby(group_cols, dropna=False)
            .agg(
                passengers=("flight_number", "size"),
                total_seats=("total_seats", "max"),
                first_class_seats=("first_class_seat_count", "max"),
                business_class_seats=("business_class_seat_count", "max"),
                premium_economy_seats=("premium_economy_class_seat_count", "max"),
                economy_seats=("economy_class_seat_count", "max"),
            )
            .reset_index()
        )

        flights_group["hour"] = flights_group["scheduled_departure_local"].dt.hour
        flights_group["aircraft_class"] = flights_group["aircraft_type_iata"].apply(get_aircraft_class)

        total_flights = int(len(flights_group))
        total_passengers = int(flights_group["passengers"].sum())
        carriers_count = int(flights_group["operating_carrier_name"].nunique())

        flight_dates = (
            flights_group["flight_date"].dropna().astype(str).sort_values().unique().tolist()
        )

        first_departure = flights_group["scheduled_departure_local"].min()
        last_departure = flights_group["scheduled_departure_local"].max()

        # Hourly summary
        hour_counts = (
            flights_group.dropna(subset=["hour"])
            .groupby("hour")["flight_number"]
            .count()
            .reindex(range(24), fill_value=0)
        )

        hour_carriers = (
            flights_group.dropna(subset=["hour"])
            .groupby(["hour", "operating_carrier_name"])["flight_number"]
            .count()
            .reset_index()
        )

        hours_summary: List[Dict[str, object]] = []
        for hour in range(24):
            carrier_rows = hour_carriers[hour_carriers["hour"] == hour]
            carriers = [
                {
                    "label": (row["operating_carrier_name"] or "Unknown Carrier"),
                    "flights": int(row["flight_number"]),
                }
                for _, row in carrier_rows.sort_values("flight_number", ascending=False).head(top_n).iterrows()
            ]
            hours_summary.append(
                {
                    "hour": hour,
                    "label": f"{hour:02d}:00",
                    "flights": int(hour_counts.loc[hour]) if hour in hour_counts.index else 0,
                    "carriers": carriers,
                }
            )

        def _format_class_label(code: str) -> str:
            if not code or code == "Unknown":
                return "Unknown Class"
            return f"Class {code}"

        class_distribution_rows = (
            flights_group.groupby("aircraft_class", dropna=False)["flight_number"]
            .count()
            .reset_index(name="flights")
        )

        class_distribution: List[Dict[str, object]] = []
        for _, class_row in class_distribution_rows.sort_values("flights", ascending=False).iterrows():
            class_code = class_row.get("aircraft_class") or "Unknown"
            flights = int(class_row.get("flights", 0) or 0)
            ratio = float(flights / total_flights) if total_flights else 0.0
            class_distribution.append(
                {
                    "class": class_code,
                    "label": _format_class_label(class_code),
                    "flights": flights,
                    "ratio": round(ratio, 4),
                }
            )

        # Carrier summary
        carrier_group = (
            flights_group.groupby("operating_carrier_name", dropna=False)
            .agg(
                flights=("flight_number", "count"),
                passengers=("passengers", "sum"),
            )
            .reset_index()
        )

        carrier_dest = (
            flights_group.groupby(
                [
                    "operating_carrier_name",
                    "arrival_airport_iata",
                    "arrival_city",
                    "arrival_country",
                ],
                dropna=False,
            )
            .agg(flights=("flight_number", "count"), passengers=("passengers", "sum"))
            .reset_index()
        )

        carrier_aircraft = (
            flights_group.groupby(
                ["operating_carrier_name", "aircraft_type_iata"], dropna=False
            )["flight_number"]
            .count()
            .reset_index(name="flights")
        )

        carriers_summary: List[Dict[str, object]] = []
        for _, row in carrier_group.sort_values("flights", ascending=False).iterrows():
            carrier_name = row["operating_carrier_name"] or "Unknown Carrier"

            dest_rows = carrier_dest[carrier_dest["operating_carrier_name"] == row["operating_carrier_name"]]
            top_destination_row = (
                dest_rows.sort_values(["flights", "passengers"], ascending=False).head(1)
            )

            aircraft_rows = carrier_aircraft[
                carrier_aircraft["operating_carrier_name"] == row["operating_carrier_name"]
            ].sort_values("flights", ascending=False)

            top_aircraft: List[Dict[str, object]] = []
            for _, aircraft_row in aircraft_rows.head(3).iterrows():
                code = aircraft_row.get("aircraft_type_iata")
                code_str = str(code) if pd.notna(code) else None
                metadata = get_aircraft_metadata(code_str)
                top_aircraft.append(
                    {
                        "type": metadata.get("name") or metadata.get("code") or "Unknown",
                        "code": metadata.get("code"),
                        "class": metadata.get("class") or "Unknown",
                        "manufacturer": metadata.get("manufacturer"),
                        "flights": int(aircraft_row["flights"]),
                    }
                )

            destination_info = (
                {
                    "airport": top_destination_row.iloc[0]["arrival_airport_iata"],
                    "city": top_destination_row.iloc[0]["arrival_city"],
                    "country": top_destination_row.iloc[0]["arrival_country"],
                    "flights": int(top_destination_row.iloc[0]["flights"]),
                }
                if not top_destination_row.empty
                else {
                    "airport": None,
                    "city": None,
                    "country": None,
                    "flights": 0,
                }
            )

            carriers_summary.append(
                {
                    "label": carrier_name,
                    "flights": int(row["flights"]),
                    "passengers": int(row["passengers"]),
                    "topDestination": destination_info,
                    "topAircraft": top_aircraft,
                }
            )

        carriers_summary = sorted(
            carriers_summary, key=lambda item: item["flights"], reverse=True
        )[:top_n]

        # Flight details
        detail_rows = flights_group.sort_values("scheduled_departure_local")

        flight_details: List[Dict[str, object]] = []
        for _, detail in detail_rows.iterrows():
            departure_dt = detail["scheduled_departure_local"]
            arrival_dt = detail["scheduled_arrival_local"]
            aircraft_code = detail.get("aircraft_type_iata")
            aircraft_code_str = str(aircraft_code) if pd.notna(aircraft_code) else None
            aircraft_metadata = get_aircraft_metadata(aircraft_code_str)

            flight_details.append(
                {
                    "flightNumber": detail.get("flight_number"),
                    "carrier": detail.get("operating_carrier_name") or "Unknown Carrier",
                    "departure": {
                        "airport": detail.get("departure_airport_iata"),
                        "city": detail.get("departure_city"),
                        "country": detail.get("departure_country"),
                        "datetime": departure_dt.floor('s').isoformat() if pd.notna(departure_dt) else None,
                    },
                    "arrival": {
                        "airport": detail.get("arrival_airport_iata"),
                        "city": detail.get("arrival_city"),
                        "country": detail.get("arrival_country"),
                        "datetime": arrival_dt.floor('s').isoformat() if pd.notna(arrival_dt) else None,
                    },
                    "aircraft": aircraft_metadata.get("name") or aircraft_metadata.get("code") or "Unknown",
                    "aircraftCode": aircraft_metadata.get("code"),
                    "aircraftClass": aircraft_metadata.get("class") or "Unknown",
                    "aircraftManufacturer": aircraft_metadata.get("manufacturer"),
                    "passengers": int(detail.get("passengers", 0)),
                    "totalSeats": int(detail.get("total_seats", 0)) if pd.notna(detail.get("total_seats")) else None,
                }
            )

        return {
            "totals": {
                "flights": total_flights,
                "passengers": total_passengers,
                "carriers": carriers_count,
                "dateRange": flight_dates,
                "firstDeparture": first_departure.floor('s').isoformat() if pd.notna(first_departure) else None,
                "lastDeparture": last_departure.floor('s').isoformat() if pd.notna(last_departure) else None,
            },
            "hours": hours_summary,
            "classDistribution": class_distribution,
            "carriers": carriers_summary,
            "flights": flight_details[:500],
        }

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

    async def get_dashboard_summary(self, scenario_id: str) -> Dict[str, object]:
        pax_df = await self._load_passenger_dataframe(scenario_id)
        process_flow = await self._load_process_flow(scenario_id)

        facility_charts = self._build_facility_charts(
            pax_df, process_flow, DEFAULT_INTERVAL_MINUTES
        )
        passenger_summary = self._build_passenger_summary(pax_df, DEFAULT_TOP_N)
        flight_summary = self._build_flight_summary(pax_df, DEFAULT_TOP_N)

        return {
            "facilityCharts": facility_charts,
            "passengerSummary": passenger_summary,
            "flightSummary": flight_summary,
        }

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

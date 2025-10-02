from __future__ import annotations

import re
from typing import Dict, List

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
        interval_minutes: int = 30,
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
        interval_minutes: int = 30,
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


def _sort_facilities(facilities: List[str]) -> List[str]:
    def sort_key(value: str):
        match = re.match(r"([A-Za-z_]+)(?:_(\d+))?", value)
        if match:
            prefix = match.group(1)
            number = match.group(2)
            return prefix, int(number) if number else -1
        return value, -1

    return sorted(facilities, key=sort_key)

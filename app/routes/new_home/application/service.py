from __future__ import annotations

from typing import Dict, List, Optional

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

    async def list_available_facilities(self, scenario_id: str) -> Dict[str, List[str]]:
        process_flow = await self._load_process_flow(scenario_id)
        facilities_by_step: Dict[str, List[str]] = {}
        for step in process_flow:
            step_name = step.get("name")
            facility_ids: List[str] = []
            for zone in step.get("zones", {}).values():
                for facility in zone.get("facilities", []):
                    facility_id = facility.get("id")
                    if facility_id:
                        facility_ids.append(facility_id)
            unique_ids: List[str] = []
            seen = set()
            for facility_id in facility_ids:
                if facility_id not in seen:
                    seen.add(facility_id)
                    unique_ids.append(facility_id)
            facilities_by_step[step_name] = unique_ids
        return facilities_by_step

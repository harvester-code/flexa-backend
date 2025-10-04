from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class TimeSeriesData:
    label: str
    values: List[int]


@dataclass
class FacilityChartSummary:
    total_inflow: int
    total_outflow: int
    max_capacity: float
    average_capacity: float
    bottleneck_times: List[str]


@dataclass
class FacilityChart:
    step: str
    facility_id: str
    zone_id: str
    zone_name: str
    interval_minutes: int
    time_range: List[str]
    capacity: List[float]
    inflow_series: List[TimeSeriesData]
    outflow_series: List[TimeSeriesData]
    total_inflow: List[int]
    total_outflow: List[int]
    facility_info: str
    summary: FacilityChartSummary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step": self.step,
            "facilityId": self.facility_id,
            "zoneId": self.zone_id,
            "zoneName": self.zone_name,
            "intervalMinutes": self.interval_minutes,
            "timeRange": self.time_range,
            "capacity": self.capacity,
            "inflowSeries": [data.__dict__ for data in self.inflow_series],
            "outflowSeries": [data.__dict__ for data in self.outflow_series],
            "totalInflow": self.total_inflow,
            "totalOutflow": self.total_outflow,
            "facilityInfo": self.facility_info,
            "summary": {
                "totalInflow": self.summary.total_inflow,
                "totalOutflow": self.summary.total_outflow,
                "maxCapacity": self.summary.max_capacity,
                "averageCapacity": self.summary.average_capacity,
                "bottleneckTimes": self.summary.bottleneck_times,
            },
        }

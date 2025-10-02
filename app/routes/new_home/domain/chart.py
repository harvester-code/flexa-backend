from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any


@dataclass
class TimeSeriesData:
    label: str
    values: List[int]


@dataclass
class FacilityChartSummary:
    total_demand: int
    total_processed: int
    max_capacity: float
    average_capacity: float
    bottleneck_times: List[str]


@dataclass
class FacilityChart:
    step: str
    facility_id: str
    interval_minutes: int
    time_range: List[str]
    capacity: List[float]
    demand_series: List[TimeSeriesData]
    processing_series: List[TimeSeriesData]
    total_demand: List[int]
    total_processed: List[int]
    facility_info: str
    summary: FacilityChartSummary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "step": self.step,
            "facilityId": self.facility_id,
            "intervalMinutes": self.interval_minutes,
            "timeRange": self.time_range,
            "capacity": self.capacity,
            "demandSeries": [data.__dict__ for data in self.demand_series],
            "processingSeries": [data.__dict__ for data in self.processing_series],
            "totalDemand": self.total_demand,
            "totalProcessed": self.total_processed,
            "facilityInfo": self.facility_info,
            "summary": {
                "totalDemand": self.summary.total_demand,
                "totalProcessed": self.summary.total_processed,
                "maxCapacity": self.summary.max_capacity,
                "averageCapacity": self.summary.average_capacity,
                "bottleneckTimes": self.summary.bottleneck_times,
            },
        }

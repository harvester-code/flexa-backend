from typing import Any, Dict, Optional, List

import numpy as np
import pandas as pd

def _extract_block_period(block: dict) -> Optional[tuple[pd.Timestamp, pd.Timestamp]]:
    period = block.get("period", "")
    if not period:
        return None

    if len(period) > 19 and period[19] == "-":
        start_str = period[:19]
        end_str = period[20:]
    else:
        parts = period.split(" - ")
        if len(parts) != 2:
            return None
        start_str, end_str = parts

    block_start = pd.to_datetime(start_str.strip(), errors="coerce")
    block_end = pd.to_datetime(end_str.strip(), errors="coerce")
    if pd.isna(block_start) or pd.isna(block_end):
        return None
    return block_start, block_end


def _calculate_capacity_for_slot(
    facility_config: dict,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> float:
    slot_capacity = 0.0
    for block in facility_config.get("operating_schedule", {}).get("time_blocks", []):
        if not block.get("activate", True):
            continue

        period_bounds = _extract_block_period(block)
        if not period_bounds:
            continue
        block_start, block_end = period_bounds

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


def _calculate_step_capacity_series_by_zone(
    step_config: dict,
    time_range: pd.DatetimeIndex,
    interval_minutes: int,
) -> Dict[str, List[float]]:
    zone_capacity: Dict[str, List[float]] = {}
    if time_range.empty:
        return zone_capacity

    for zone_name, zone in step_config.get("zones", {}).items():
        total_capacity = [0.0] * len(time_range)
        for facility in zone.get("facilities", []):
            facility_capacity: List[float] = []
            for start in time_range:
                end = start + pd.Timedelta(minutes=interval_minutes)
                facility_capacity.append(
                    _calculate_capacity_for_slot(facility, start, end)
                )
            total_capacity = [curr + add for curr, add in zip(total_capacity, facility_capacity)]
        zone_capacity[zone_name] = total_capacity
    return zone_capacity


class HomeAnalyzer:
    def __init__(
        self,
        pax_df: pd.DataFrame,
        percentile: int | None = None,
        process_flow: Optional[List[dict]] = None,
    ):
        # 1. on, done 값이 없는 경우, 처리 안된 여객이므로 제외하고 시작함
        self.pax_df = pax_df.copy()
        for process in [
            col.replace("_on_pred", "")
            for col in self.pax_df.columns
            if "on_pred" in col
        ]:
            cols_to_check = [f"{process}_on_pred", f"{process}_done_time"]
            self.pax_df = self.pax_df.dropna(subset=cols_to_check)
        # 2. 처리 완료 시간이 예정 출발 시간보다 늦은 경우, 제외하고 시작함
        # last_done_col = f"{process_list[-1]}_done_time"
        # if last_done_col in pax_df.columns and 'scheduled_departure_local' in pax_df.columns:
        #     pax_df = pax_df[pax_df[last_done_col] < pax_df['scheduled_departure_local']]

        self.percentile = percentile
        self.time_unit = "10min"
        self.process_list = self._get_process_list()
        self.process_flow_map = self._build_process_flow_map(process_flow)

    # ===============================
    # 메인 함수들
    # ===============================

    def get_summary(self):
        """요약 데이터 생성"""
        # 기본 데이터 계산
        throughput = int(self.pax_df[f"{self.process_list[-1]}_done_time"].notna().sum())

        waiting_times_df = pd.DataFrame(
            {
                process: self._get_waiting_time(self.pax_df, process)
                for process in self.process_list
            }
        ).dropna()
        waiting_time_data = self._get_pax_experience_data(
            waiting_times_df, "time"
        )

        queue_lengths_df = pd.DataFrame(
            {process: self.pax_df[f"{process}_queue_length"] for process in self.process_list}
        ).dropna()
        queue_data = self._get_pax_experience_data(
            queue_lengths_df, "count"
        )

        # 응답 데이터 구성
        data = {
            "throughput": throughput,
            "waiting_time": waiting_time_data["total"],
            "queue_length": queue_data["total"],
            "pax_experience": {
                "waiting_time": {
                    process: waiting_time_data[process] for process in self.process_list
                },
                "queue_length": {
                    process: queue_data[process] for process in self.process_list
                },
            },
        }
        return data

    def get_alert_issues(self, top_n: int = 8, time_interval: str = "30min"):
        """알림 및 이슈 데이터 생성"""
        result_df = pd.concat(
            [
                self._create_process_dataframe(process, time_interval)
                for process in self.process_list
            ],
            ignore_index=True,
        )

        # 데이터 정렬 및 중복 제거
        result_df = (
            result_df.sort_values("waiting_time", ascending=False)
            .drop_duplicates(subset=["datetime", "process_name"])
            .reset_index(drop=True)
        )

        # 전체 시설 데이터
        data = {
            "all_facilities": [
                self._to_alert_format(row)
                for _, row in result_df.head(top_n).iterrows()
            ]
        }

        # 각 프로세스별 데이터 추가
        for process_name in self.process_list:
            filtered_data = result_df[result_df["process"] == process_name].head(top_n)
            data[process_name] = [
                self._to_alert_format(row) for _, row in filtered_data.iterrows()
            ]
        return data

    def get_flow_chart_data(self, time_unit: str = None):
        """플로우 차트 데이터 생성 - 계층 구조로 변경"""
        time_unit = time_unit or self.time_unit
        time_df = self._create_time_df_index(time_unit)
        data = {"times": time_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()}

        for process in self.process_list:
            # 프로세스 데이터를 분리: zone이 있는 데이터와 None 데이터
            zone_col = f"{process}_zone"

            # None 값 (Skip/Bypass) 처리를 위한 데이터 분리
            all_process_data = self.pax_df.copy()
            has_zone = all_process_data[zone_col].notna()
            no_zone = all_process_data[zone_col].isna()

            facilities = sorted(all_process_data[zone_col].dropna().unique())

            # 계층 구조를 위한 프로세스 정보 생성
            process_info = {
                "process_name": process.replace("_", " ").title(),
                "facilities": [],
                "data": {}
            }

            step_config = self.process_flow_map.get(process) if self.process_flow_map else None
            try:
                interval_minutes = int(pd.Timedelta(time_unit).total_seconds() / 60)
            except ValueError:
                interval_minutes = 0

            if facilities:
                process_data = all_process_data[has_zone].copy()
                waiting_series = self._get_waiting_time(process_data, process)
                process_data[f"{process}_waiting_seconds"] = waiting_series.dt.total_seconds()

                # 시간 플로어링을 복사본에서 계산
                process_data[f"{process}_on_floored"] = process_data[
                    f"{process}_on_pred"
                ].dt.floor(time_unit)
                process_data[f"{process}_done_floored"] = process_data[
                    f"{process}_done_time"
                ].dt.floor(time_unit)

                # 한번에 모든 메트릭 계산
                metrics = {
                    "inflow": process_data.groupby(
                        [f"{process}_on_floored", f"{process}_zone"]
                    ).size(),
                    "outflow": process_data.groupby(
                        [f"{process}_done_floored", f"{process}_zone"]
                    ).size(),
                    "queue_length": process_data.groupby(
                        [f"{process}_on_floored", f"{process}_zone"]
                    )[f"{process}_queue_length"].mean(),
                    "waiting_time": process_data.groupby(
                        [f"{process}_on_floored", f"{process}_zone"]
                    )[f"{process}_waiting_seconds"].mean(),
                }

                # unstack하고 reindex 한번에
                pivoted = {
                    k: v.unstack(fill_value=0).reindex(time_df.index, fill_value=0)
                    for k, v in metrics.items()
                }

                # 결과 구성
                process_facility_data = {}
                aggregated = {
                    k: pd.Series(0, index=time_df.index, dtype=float)
                    for k in metrics.keys()
                }

                zone_capacity_map: Dict[str, List[float]] = {}
                if step_config and interval_minutes > 0:
                    zone_capacity_map = _calculate_step_capacity_series_by_zone(
                        step_config,
                        time_df.index,
                        interval_minutes,
                    )

                for facility_name in facilities:
                    # 원래 facility 이름 보존
                    node_name = facility_name

                    facility_data = {
                        k: pivoted[k].get(facility_name, pd.Series(0, index=time_df.index))
                        for k in metrics.keys()
                    }

                    # 집계
                    for k in facility_data.keys():
                        aggregated[k] += facility_data[k]

                    # facilities 리스트에 추가
                    process_info["facilities"].append(node_name)

                    # 저장 (타입 변환)
                    process_facility_data[node_name] = {
                        k: (
                            facility_data[k].round()
                            if k in ["queue_length", "waiting_time"]
                            else facility_data[k]
                        )
                        .astype(int)
                        .tolist()
                        for k in facility_data.keys()
                    }

                    if node_name in zone_capacity_map:
                        process_facility_data[node_name]["capacity"] = [
                            int(round(value)) for value in zone_capacity_map[node_name]
                        ]

                # None/Skip 데이터 처리 - 프로세스를 건너뛴 승객
                if no_zone.any():
                    skip_count = no_zone.sum()
                    # Skip 노드 추가
                    process_info["facilities"].append("Skip")
                    process_facility_data["Skip"] = {
                        "inflow": [0] * len(time_df),  # Skip은 고정값
                        "outflow": [0] * len(time_df),
                        "queue_length": [0] * len(time_df),
                        "waiting_time": [0] * len(time_df),
                        "skip_count": skip_count  # 건너뛴 총 인원수 정보 추가
                    }

                # all_zones
                facility_count = max(len(facilities), 1)
                all_zones_data = {
                    "inflow": aggregated["inflow"].astype(int).tolist(),
                    "outflow": aggregated["outflow"].astype(int).tolist(),
                    "queue_length": (aggregated["queue_length"] / facility_count)
                    .round()
                    .astype(int)
                    .tolist(),
                    "waiting_time": (aggregated["waiting_time"] / facility_count)
                    .round()
                    .astype(int)
                    .tolist(),
                }

                if zone_capacity_map:
                    aggregate_capacity = [0.0] * len(time_df.index)
                    for capacity_list in zone_capacity_map.values():
                        aggregate_capacity = [curr + add for curr, add in zip(aggregate_capacity, capacity_list)]
                    all_zones_data["capacity"] = [int(round(value)) for value in aggregate_capacity]

                # process_info에 데이터 추가
                process_info["data"] = {"all_zones": all_zones_data, **process_facility_data}
            else:
                # 이 프로세스에 아무도 가지 않은 경우
                process_info["data"] = {}

            data[process] = process_info
        return data

    def get_facility_details(self):
        """시설 세부 정보 생성"""


        data = []
        for process in self.process_list:
            base_fields = ["zone", "facility", "queue_length", "on_pred", "done_time"]
            wait_fields = [
                suffix
                for suffix in ["queue_wait_time"]
                if f"{process}_{suffix}" in self.pax_df.columns
            ]
            cols = [f"{process}_{field}" for field in base_fields] + [f"{process}_{field}" for field in wait_fields]
            cols = [col for col in cols if col in self.pax_df.columns]
            process_df = self.pax_df[cols].copy()

            # Overview 계산
            waiting_time = self._calculate_waiting_time(process_df, process)

            overview = {
                "throughput": len(process_df),
                "queuePax": int(
                    process_df[f"{process}_queue_length"].quantile(1 - self.percentile / 100)
                    if self.percentile is not None
                    else process_df[f"{process}_queue_length"].mean()
                ),
                "waitTime": self._format_waiting_time(
                    waiting_time.quantile(1 - self.percentile / 100)
                    if self.percentile is not None
                    else waiting_time.mean()
                ),
            }

            # Components 계산
            components = []
            for facility in sorted(process_df[f"{process}_zone"].unique()):
                facility_df = process_df[process_df[f"{process}_zone"] == facility]
                waiting_time = self._calculate_waiting_time(facility_df, process)

                components.append(
                    {
                        "title": facility,
                        "throughput": len(facility_df),
                        "queuePax": int(
                        facility_df[f"{process}_queue_length"].quantile(
                            1 - self.percentile / 100
                        )
                            if self.percentile is not None
                            else facility_df[f"{process}_queue_length"].mean()
                        ),
                        "waitTime": self._format_waiting_time(
                            waiting_time.quantile(1 - self.percentile / 100)
                            if self.percentile is not None
                            else waiting_time.mean()
                        ),
                    }
                )

            data.append(
                {"category": process, "overview": overview, "components": components}
            )
        return data


    def get_histogram_data(self):
        """시설별, 그리고 그 안의 구역별 통계 데이터 생성 (all_zones 포함)"""
        # 상수
        WT_BINS = [0, 15, 30, 45, 60, float("inf")]
        WT_LABELS = [
            "00:00-15:00",
            "15:00-30:00",
            "30:00-45:00",
            "45:00-60:00",
            "60:00-",
        ]
        QL_BINS = [0, 50, 100, 150, 200, 250, float("inf")]
        QL_LABELS = ["0-50", "50-100", "100-150", "150-200", "200-250", "250+"]

        data = {}

        for process in self.process_list:
            facilities = sorted(self.pax_df[f"{process}_zone"].dropna().unique())
            wt_collection, ql_collection = [], []
            facility_data = {}

            for facility in facilities:
                df = self.pax_df[self.pax_df[f"{process}_zone"] == facility].copy()

                # 대기시간 분포 (초를 분으로 변환)
                wt_mins = self._get_waiting_time(df, process).dt.total_seconds() / 60
                wt_bins = self._get_distribution(wt_mins, WT_BINS, WT_LABELS)

                # 대기열 분포
                ql_bins = []
                if f"{process}_queue_length" in df.columns and not df[f"{process}_queue_length"].empty:
                    ql_bins = self._get_distribution(
                        df[f"{process}_queue_length"], QL_BINS, QL_LABELS
                    )

                # 데이터 저장
                short_name = facility.split("_")[-1]
                facility_data[short_name] = {
                    "waiting_time": self._create_bins_data(wt_bins, "min", True),
                    "queue_length": self._create_bins_data(ql_bins, "pax", False),
                }

                if wt_bins:
                    wt_collection.append(wt_bins)
                if ql_bins:
                    ql_collection.append(ql_bins)

            # all_zones 생성
            if wt_collection and ql_collection:
                all_zones = {
                    "waiting_time": self._create_bins_data(
                        self._calc_avg_bins(wt_collection), "min", True
                    ),
                    "queue_length": self._create_bins_data(
                        self._calc_avg_bins(ql_collection), "pax", False
                    ),
                }
                data[process] = {"all_zones": all_zones, **facility_data}
            else:
                data[process] = facility_data

        return data

    def get_sankey_diagram_data(self):
        """산키 다이어그램 데이터 생성 - 계층 구조 지원"""
        # zone 기반으로 승객 플로우 생성 (시간 순서로 정렬)
        zone_cols = [
            col for col in self.pax_df.columns
            if col.endswith("_zone")
        ]
        # {process}_done_time 기준으로 시간 순서 정렬
        timed_facilities = []
        for col in zone_cols:
            process_name = col.replace("_zone", "")
            done_time_col = f"{process_name}_done_time"
            if done_time_col in self.pax_df.columns:
                # 평균 완료 시간으로 정렬
                avg_time = self.pax_df[done_time_col].mean()
                timed_facilities.append((avg_time, col))
        
        # 시간 순서대로 정렬 (체크인 → 게이트 순서)
        timed_facilities.sort(key=lambda x: x[0])
        target_columns = [col for _, col in timed_facilities]
        # 시간 정보가 없는 경우 원래 방식 사용
        if not target_columns:
            target_columns = zone_cols

        # 빈 컬럼 리스트 처리
        if not target_columns:
            return {
                "label": [],
                "link": {"source": [], "target": [], "value": []},
            }

        flow_df = self.pax_df.groupby(target_columns).size().reset_index(name="count")

        # 동일한 결과를 보장하기 위해 각 컬럼의 고유값을 정렬
        unique_values = {}
        current_index = 0
        for col in target_columns:
            # 고유값을 정렬하여 일관된 순서 보장
            sorted_unique_vals = sorted(flow_df[col].unique())
            unique_values[col] = {
                val: i + current_index for i, val in enumerate(sorted_unique_vals)
            }
            current_index += len(sorted_unique_vals)

        sources, targets, values = [], [], []
        for i in range(len(target_columns) - 1):
            col1, col2 = target_columns[i], target_columns[i + 1]
            grouped = flow_df.groupby([col1, col2])["count"].sum().reset_index()

            # 일관된 순서를 위해 그룹화된 결과도 정렬
            grouped = grouped.sort_values([col1, col2]).reset_index(drop=True)

            for _, row in grouped.iterrows():
                sources.append(unique_values[col1][row[col1]])
                targets.append(unique_values[col2][row[col2]])
                values.append(int(row["count"]))

        # 라벨도 정렬된 순서로 생성
        labels = []
        for col in target_columns:
            labels.extend(sorted(flow_df[col].unique()))

        # 프로세스 정보 생성 (계층 구조를 위해)
        process_info = {}
        for col in target_columns:
            process_name = col.replace("_zone", "")
            facilities = sorted(flow_df[col].unique())
            process_info[process_name] = {
                "process_name": process_name.replace("_", " ").title(),
                "facilities": facilities
            }

        return {
            "label": labels,
            "link": {"source": sources, "target": targets, "value": values},
            "process_info": process_info  # 계층 구조 정보 추가
        }

    # ===============================
    # 서브 함수들 (헬퍼 메소드)
    # ===============================

    def _get_process_list(self):
        """프로세스 리스트 추출"""
        return [
            col.replace("_on_pred", "")
            for col in self.pax_df.columns
            if "on_pred" in col
        ]

    def _format_waiting_time(self, time_value):
        """대기 시간을 hour, minute, second로 분리하여 딕셔너리로 반환"""
        try:
            # timedelta 객체인 경우
            total_seconds = int(time_value.total_seconds())
        except AttributeError:
            # 정수(초)인 경우
            total_seconds = int(time_value)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return {"hour": hours, "minute": minutes, "second": seconds}

    def _get_pax_experience_data(self, df, data_type):
        """승객 경험 데이터 계산"""
        df["total"] = df.sum(axis=1)
        if self.percentile is None:
            target_value = df["total"].mean()
        else:
            target_value = np.percentile(df["total"], 100 - self.percentile)

        closest_idx = (df["total"] - target_value).abs().idxmin()
        result_row = df.loc[closest_idx]

        result_dict = {}
        for col in df.columns:
            if data_type == "time":
                result_dict[col] = self._format_waiting_time(result_row[col])
            else:
                result_dict[col] = int(result_row[col])

        return result_dict

    def _create_process_dataframe(self, process, time_interval):
        """각 프로세스별 데이터프레임 생성"""
        return pd.DataFrame(
            {
                "process": process,
                "datetime": self.pax_df[f"{process}_on_pred"].dt.floor(time_interval),
                "waiting_time": self._get_waiting_time(self.pax_df, process),
                "queue_length": self.pax_df[f"{process}_queue_length"],
                "process_name": self.pax_df[f"{process}_zone"],
            }
        )

    def _to_alert_format(self, row):
        """행을 알림 형식으로 변환"""
        return {
            "time": row["datetime"].strftime("%H:%M:%S"),
            "waiting_time": self._format_waiting_time(row["waiting_time"]),
            "queue_length": row["queue_length"],
            "node": row["process_name"],
        }

    def _create_time_df_index(self, time_unit):
        """시간별 데이터프레임 생성"""
        last_date = self.pax_df["show_up_time"].dt.date.unique()[-1]
        time_index = pd.date_range(
            start=f"{last_date} 00:00:00", end=f"{last_date} 23:59:59", freq=time_unit
        )
        return pd.DataFrame(index=time_index)

    def _build_process_flow_map(self, process_flow: Optional[List[dict]]) -> Dict[str, dict]:
        if not process_flow:
            return {}
        mapping: Dict[str, dict] = {}
        for step in process_flow:
            name = step.get("name")
            if name:
                mapping[name] = step
        return mapping

    def _get_waiting_time(self, df, process):
        """순수 queue 대기시간 반환"""
        queue_col = f"{process}_queue_wait_time"

        if queue_col in df.columns:
            queue_series = pd.to_timedelta(df[queue_col])
            return queue_series

        return pd.Series(pd.NaT, index=df.index, dtype="timedelta64[ns]")


    def _calculate_waiting_time(self, process_df, process):
        """대기 시간 계산"""
        return self._get_waiting_time(process_df, process)







    def _get_distribution(self, values, bins, labels):
        """값들의 분포를 백분율로 계산"""
        if values.empty:
            return []
        groups = pd.cut(values, bins=bins, labels=labels, right=False)
        counts = groups.value_counts().reindex(labels, fill_value=0)
        total = counts.sum()
        percentages = ((counts / total) * 100).round(0) if total > 0 else counts
        return [
            {"title": label, "value": int(percentages[label]), "unit": "%"}
            for label in labels
        ]

    def _parse_range(self, title, is_time=True):
        """범위 문자열 파싱"""
        if is_time:
            if ":" not in title:
                return [0, None]
            start, rest = title.split("-")
            start_min = int(start.split(":")[0])
            return [start_min, int(rest.split(":")[0]) if rest else None]
        else:
            if "+" in title:
                return [int(title.replace("+", "")), None]
            start, end = title.split("-") if "-" in title else (0, None)
            return [int(start), int(end) if end else None]

    def _create_bins_data(self, bins_list, range_unit, is_time):
        """bins 데이터 생성"""
        return {
            "range_unit": range_unit,
            "value_unit": "%",
            "bins": [
                {
                    "range": self._parse_range(item["title"], is_time),
                    "value": item["value"],
                }
                for item in bins_list
            ],
        }

    def _calc_avg_bins(self, bins_collection):
        """여러 시설의 평균 계산"""
        if not bins_collection:
            return []
        agg = {}
        for bin_list in bins_collection:
            for item in bin_list:
                agg.setdefault(item["title"], []).append(item["value"])
        return [
            {
                "title": item["title"],
                "value": int(round(np.mean(agg.get(item["title"], [0])))),
            }
            for item in bins_collection[0]
        ]

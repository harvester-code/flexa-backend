import math
from typing import Any, Dict, Optional
from fractions import Fraction

import numpy as np
import pandas as pd



class Calculator:
    def __init__(
        self,
        pax_df: pd.DataFrame,
        facility_info: Optional[Dict[str, Any]] = None,
        calculate_type: str = "mean",
        percentile: int | None = None,
    ):
        # on, done 값이 없는 경우, 처리 안된 여객이므로 제외하고 시작함
        self.pax_df = pax_df.copy()
        for process in [col.replace("_on_pred", "") for col in self.pax_df.columns if "on_pred" in col]:
            cols_to_check = [
                f"{process}_on_pred",
                f"{process}_done_pred"
            ]
            self.pax_df = self.pax_df.dropna(subset=cols_to_check)
        self.facility_info = facility_info
        self.calculate_type = calculate_type
        self.percentile = percentile
        self.time_unit = "10min"
        self.process_list = self._get_process_list()
        self.facility_ratio = self.make_facility_ratio() if self.facility_info is not None else {}

    # ===== 메인 함수들 =====
    def get_terminal_overview_line_queue(self):
        """시간별 시설 대기열 데이터를 maps.json 형식으로 변환

        Returns:
            dict: maps.json 형식의 데이터
        """
        # 시간별 시설 대기열 데이터프레임 생성
        time_df = self._create_facility_queue_dataframe()

        # 결과 딕셔너리 초기화
        result = {}

        # 각 시간대별로 데이터 변환
        for time in time_df.index:
            time_str = time.strftime("%Y-%m-%d %H:%M:%S")
            result[time_str] = []

            # 각 시설별로 데이터 추가
            for facility in time_df.columns:
                queue_length = time_df.loc[time, facility]
                if pd.notna(queue_length):  # NaN이 아닌 경우만 추가
                    result[time_str].append(
                        {
                            "title": facility,
                            "queue_length": int(queue_length),  # 정수로 변환
                        }
                    )
        return result

    def get_process_kpi_by_mode(self, mode="waiting_time", return_row=False):
        """
        mode: "waiting_time" or "queue_length"
        return_row: True면 (dict, row) 반환
        Returns:
            dict: {process_name: value or {hour, minute, second}}
        """
        result = {}
        process_values = {}
        for process in self.process_list:
            if mode == "waiting_time":
                values = self._calculate_waiting_time_minutes(self.pax_df, process)
            elif mode == "queue_length":
                values = self.pax_df[f"{process}_que"]
            else:
                raise ValueError("mode must be 'waiting_time' or 'queue_length'")
            process_values[process] = values

        import numpy as np
        import pandas as pd

        df = pd.DataFrame(process_values)
        df = df.dropna()

        if df.empty:
            for process in self.process_list:
                result[process] = (
                    {"hour": 0, "minute": 0, "second": 0}
                    if mode == "waiting_time"
                    else 0
                )
            return (result, None) if return_row else result

        if self.calculate_type == "mean":
            # 평균
            for process in self.process_list:
                value = df[process].mean()
                if mode == "waiting_time":
                    result[process] = self._format_waiting_time(value)
                else:
                    result[process] = int(round(value))
            return (result, None) if return_row else result
        else:
            # 합산 기준 n%에 해당하는 row 찾기
            df["sum"] = df.sum(axis=1)
            df_sorted = df.sort_values("sum", ascending=False).reset_index(drop=True)
            n = int(np.ceil(len(df_sorted) * (self.percentile / 100)))
            n = max(1, n)
            row = df_sorted.iloc[n - 1]
            for process in self.process_list:
                value = row[process]
                if mode == "waiting_time":
                    result[process] = self._format_waiting_time(value)
                else:
                    result[process] = int(round(value))
            return (result, row) if return_row else result

    def get_summary(self):
        """메인 함수: 요약 KPI 6개 + pax_experience 반환 (percentile이 있을 때 한 명의 전체 경험 기준)"""
        throughput = int(self.pax_df[f"{self.process_list[-1]}_pt_pred"].notna().sum())
        facility_utilization = self.facility_ratio["all_facility"][
            "activated_per_installed"
        ]
        processed_per_activated = self.facility_ratio["all_facility"][
            "processed_per_activated"
        ]
        processed_per_installed = self.facility_ratio["all_facility"][
            "processed_per_installed"
        ]

        if self.calculate_type == "mean":
            waiting_time = self._calculate_kpi_values(method="waiting_time")
            waiting_time = self._format_waiting_time(waiting_time)
            queue_length = self._calculate_kpi_values(method="queue_length")
            pax_experience = {
                "waiting_time": self.get_process_kpi_by_mode(mode="waiting_time"),
                "queue_length": self.get_process_kpi_by_mode(mode="queue_length"),
            }
        else:
            # 합산 기준 n%에 해당하는 row를 기준으로 summary/pax_experience 모두 생성
            waiting_time_dict, row = self.get_process_kpi_by_mode(
                mode="waiting_time", return_row=True
            )
            waiting_time = self._format_waiting_time(row[self.process_list].sum())
            queue_length_dict, row_ql = self.get_process_kpi_by_mode(
                mode="queue_length", return_row=True
            )
            queue_length = int(round(row_ql[self.process_list].sum()))
            pax_experience = {
                "waiting_time": waiting_time_dict,
                "queue_length": queue_length_dict,
            }

        return {
            "throughput": throughput,
            "waiting_time": waiting_time,
            "queue_length": queue_length,
            "facility_utilization": facility_utilization,
            "processed_per_activated": processed_per_activated,
            "processed_per_installed": processed_per_installed,
            "pax_experience": pax_experience,
        }

    def get_alert_issues(self, top_n=8, time_interval="30min"):
        """대기 시간 알림 데이터 생성 메인 함수"""
        # 모든 프로세스의 데이터프레임 생성 및 결합
        df_list = [
            self._create_process_dataframe(process, time_interval)
            for process in self.process_list
        ]
        result_df = pd.concat(df_list, ignore_index=True)

        # 데이터 정렬 및 중복 제거
        result_df = result_df.sort_values(
            "waiting_time", ascending=False
        ).drop_duplicates(subset=["datetime", "process_name"])

        # 대기 시간 형식 변환
        result_df["waiting_time"] = result_df["waiting_time"].apply(
            self._format_waiting_time
        )

        # 알림 JSON 구조 생성
        alert_json = {
            "all_facilities": [
                self._create_alert_data_entry(row)
                for _, row in result_df.head(top_n).iterrows()
            ]
        }

        # 각 프로세스별 데이터 추가
        for process in self.process_list:
            process_data = result_df[result_df["process"] == process].head(top_n)
            alert_json[process] = [
                self._create_alert_data_entry(row) for _, row in process_data.iterrows()
            ]

        # return self._format_json_numbers(alert_json)
        return alert_json

    def get_facility_details(self):
        """시설별 세부 데이터를 계산하고 반환 (UI 요구에 맞게 필드명 및 KPI 추가)"""
        result = []
        for process in self.process_list:
            cols_needed = [
                f"{process}_pred",
                f"{process}_facility_number",
                f"{process}_que",
                f"{process}_pt",
                f"{process}_on_pred",
                f"{process}_pt_pred",
            ]
            process_df = self.pax_df[cols_needed].copy()
            overview = self._calculate_overview_metrics(process_df, process)
            category_obj = {"category": process, "overview": overview, "components": []}

            facilities = sorted(process_df[f"{process}_pred"].unique())
            for facility in facilities:
                facility_df = process_df[process_df[f"{process}_pred"] == facility]
                waiting_time = self._calculate_waiting_time(facility_df, process)

                def get_stat(series):
                    if series.empty:
                        return 0
                    if self.calculate_type == "top":
                        return series.quantile(1 - self.percentile / 100)
                    return series.mean()

                # opened 계산 (facility_info에서 해당 facility의 opened/total)
                opened = 0
                total = 0
                # facility_info에서 해당 process의 node 중 facility 이름이 일치하는 것 찾기
                if self.facility_info and "components" in self.facility_info:
                    for comp in self.facility_info["components"]:
                        if comp["name"] == process:
                            for node in comp["nodes"]:
                                if f"{process}_{node['name']}" == facility:
                                    schedules = node.get("facility_schedules", [])
                                    try:
                                        per_facility = list(zip(*schedules))
                                    except Exception:
                                        per_facility = []
                                    for col in per_facility:
                                        if any((v is not None and v > 0) for v in col):
                                            opened += 1
                                    total += node.get(
                                        "facility_count", len(per_facility)
                                    )
                if total == 0:
                    opened = 0
                component = {
                    "title": facility,
                    "opened": [opened, total],
                    "throughput": len(facility_df),
                    "queuePax": int(get_stat(facility_df[f"{process}_que"])),
                    "waitTime": self._format_timedelta(get_stat(waiting_time)),
                    "ai_ratio": self.facility_ratio.get(facility, {}).get(
                        "activated_per_installed"
                    ),
                    "pa_ratio": self.facility_ratio.get(facility, {}).get(
                        "processed_per_activated"
                    ),
                }
                category_obj["components"].append(component)
            result.append(category_obj)
        return result

    def get_flow_chart_data(self):
        """시간별 대기열 및 대기 시간 데이터 생성 (시설별 세부 데이터 포함)"""
        # --- 1. 시간축 생성 ---
        time_df = self._create_time_dataframe()
        times = time_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()

        # --- 2. 결과 구조 초기화 ---
        result = {"times": times}

        # --- 3. 프로세스 및 시설별 데이터 계산 ---
        for process in self.process_list:
            # 'all_zones' 집계를 위한 프로세스 레벨 데이터 저장소
            process_inflow_dfs = []
            process_outflow_dfs = []
            process_queue_dfs = []
            process_waiting_time_dfs = []
            process_capacity_dfs = []

            # 개별 시설 데이터를 임시 저장할 딕셔너리
            process_facility_data = {}

            # 해당 프로세스의 모든 시설 목록 가져오기
            facilities = sorted(self.pax_df[f"{process}_pred"].dropna().unique())

            for facility_name in facilities:
                # 특정 시설에 대한 데이터만 필터링
                facility_df = self.pax_df[
                    self.pax_df[f"{process}_pred"] == facility_name
                ].copy()
                short_name = facility_name.split("_")[-1]

                # 시간대별 집계
                # inflow
                inflow_counts = facility_df.groupby(
                    facility_df[f"{process}_on_pred"].dt.floor("10min")
                ).size()
                inflow_series = inflow_counts.reindex(time_df.index, fill_value=0)

                # outflow
                outflow_counts = facility_df.groupby(
                    facility_df[f"{process}_done_pred"].dt.floor("10min")
                ).size()
                outflow_series = outflow_counts.reindex(time_df.index, fill_value=0)

                # --- 4. 대기열 및 대기 시간 계산 ---
                # 이 시설의 누적 inflow 및 outflow 계산
                cumulative_inflow = inflow_series.cumsum()
                cumulative_outflow = outflow_series.cumsum()

                # 대기열 길이(Queue Length) 계산
                queue_length_series = (cumulative_inflow - cumulative_outflow).clip(
                    lower=0
                )

                # 대기 시간(Waiting Time) 계산
                # 간단한 Little's Law 변형 적용: W = L / λ (여기서 λ는 outflow)
                # outflow가 0일 경우 대기 시간을 0으로 처리하여 0으로 나누는 것을 방지
                waiting_time_series = (
                    (queue_length_series / (outflow_series / 600))
                    .replace([float("inf"), -float("inf")], 0)
                    .fillna(0)
                )

                # === capacity 계산 ===
                capacity_list = self._calculate_node_capacity_list(process, short_name)
                # capacity_list가 비어있으면 0으로 채운 144개 리스트로 대체
                if not capacity_list:
                    capacity_list = [0] * len(time_df.index)

                process_facility_data[short_name] = {
                    "inflow": inflow_series.astype(int).tolist(),
                    "outflow": outflow_series.astype(int).tolist(),
                    "queue_length": queue_length_series.astype(int).tolist(),
                    "waiting_time": waiting_time_series.astype(int).tolist(),
                    "capacity": capacity_list,
                }
                process_inflow_dfs.append(inflow_series)
                process_outflow_dfs.append(outflow_series)
                process_queue_dfs.append(queue_length_series)
                process_waiting_time_dfs.append(waiting_time_series)
                process_capacity_dfs.append(pd.Series(capacity_list, index=time_df.index))

            # --- 7. 'all_zones' 집계 및 프로세스 결과 구성 ---
            if process_inflow_dfs:
                # 프로세스 레벨의 모든 시설 데이터 집계
                sum_inflow = pd.concat(process_inflow_dfs, axis=1).sum(axis=1)
                sum_outflow = pd.concat(process_outflow_dfs, axis=1).sum(axis=1)
                avg_queue = pd.concat(process_queue_dfs, axis=1).mean(axis=1)
                avg_waiting_time = pd.concat(process_waiting_time_dfs, axis=1).mean(
                    axis=1
                )

                all_zones_data = {
                    "inflow": sum_inflow.astype(int).tolist(),
                    "outflow": sum_outflow.astype(int).tolist(),
                    "queue_length": avg_queue.round().astype(int).tolist(),
                    "waiting_time": avg_waiting_time.round().astype(int).tolist(),
                }
                # capacity 평균값 항상 추가 (없으면 0)
                if process_capacity_dfs:
                    all_capacity = pd.concat(process_capacity_dfs, axis=1).sum(axis=1)
                    all_zones_data["capacity"] = all_capacity.round().astype(int).tolist()
                else:
                    all_zones_data["capacity"] = [0] * len(time_df.index)

                result[process] = {"all_zones": all_zones_data}
                result[process].update(process_facility_data)
            else:
                result[process] = {}

        return result

    def get_histogram_data(self):
        """시설별, 그리고 그 안의 구역별 통계 데이터 생성 (all_zones 포함)"""
        result = {}

        for process in self.process_list:
            # 'all_zones' 집계를 위한 데이터 저장소
            process_wt_bins_collection = []
            process_ql_bins_collection = []

            # 개별 시설 데이터를 임시 저장할 딕셔너리
            process_facility_data = {}

            # 해당 프로세스의 모든 시설 목록 가져오기
            facilities = sorted(self.pax_df[f"{process}_pred"].dropna().unique())

            for facility_name in facilities:
                # 특정 시설에 대한 데이터만 필터링
                facility_df = self.pax_df[
                    self.pax_df[f"{process}_pred"] == facility_name
                ].copy()

                # 히스토그램 계산
                waiting_time_bins = self._calculate_waiting_time_distribution(
                    process, facility_df
                )
                queue_length_bins = self._calculate_queue_length_distribution(
                    process, facility_df
                )

                # 짧은 이름으로 키 생성
                short_name = facility_name.split("_")[-1]

                # 개별 시설 결과 저장
                process_facility_data[short_name] = {
                    "waiting_time": {
                        "range_unit": "min",
                        "value_unit": "%",
                        "bins": [
                            {
                                "range": self._parse_range(
                                    item["title"], "waiting_time"
                                ),
                                "value": item["value"],
                            }
                            for item in waiting_time_bins
                        ],
                    },
                    "queue_length": {
                        "range_unit": "pax",
                        "value_unit": "%",
                        "bins": [
                            {
                                "range": self._parse_range(
                                    item["title"], "queue_length"
                                ),
                                "value": item["value"],
                            }
                            for item in queue_length_bins
                        ],
                    },
                }

                # 'all_zones' 집계를 위해 데이터 추가
                if waiting_time_bins:
                    process_wt_bins_collection.append(waiting_time_bins)
                if queue_length_bins:
                    process_ql_bins_collection.append(queue_length_bins)

            # --- 'all_zones' 집계 및 최종 결과 구성 ---
            if process_wt_bins_collection and process_ql_bins_collection:
                # 각 bin 별로 평균값 계산
                def calculate_average_bins(bins_collection):
                    aggregated_values = {}
                    if not bins_collection:
                        return []
                    
                    # 모든 bin의 title을 기준으로 값들을 수집
                    for bin_list in bins_collection:
                        for bin_item in bin_list:
                            title = bin_item["title"]
                            value = bin_item["value"]
                            if title not in aggregated_values:
                                aggregated_values[title] = []
                            aggregated_values[title].append(value)
                    
                    # 평균 계산 및 최종 bin 리스트 생성 (순서 유지를 위해 첫번째 리스트의 title 순서 사용)
                    average_bins = []
                    for bin_item in bins_collection[0]:
                        title = bin_item["title"]
                        # 해당 title에 값이 없는 경우를 대비하여 기본값 0으로 처리
                        avg_value = round(np.mean(aggregated_values.get(title, [0])))
                        average_bins.append(
                            {"title": title, "value": int(avg_value)}
                        )
                    return average_bins

                avg_wt_bins = calculate_average_bins(process_wt_bins_collection)
                avg_ql_bins = calculate_average_bins(process_ql_bins_collection)

                # 'all_zones' 데이터 생성
                all_zones_data = {
                    "waiting_time": {
                        "range_unit": "min",
                        "value_unit": "%",
                        "bins": [
                            {
                                "range": self._parse_range(
                                    item["title"], "waiting_time"
                                ),
                                "value": item["value"],
                            }
                            for item in avg_wt_bins
                        ],
                    },
                    "queue_length": {
                        "range_unit": "pax",
                        "value_unit": "%",
                        "bins": [
                            {
                                "range": self._parse_range(
                                    item["title"], "queue_length"
                                ),
                                "value": item["value"],
                            }
                            for item in avg_ql_bins
                        ],
                    },
                }
                # 'all_zones'를 맨 앞에 추가
                result[process] = {"all_zones": all_zones_data}
                result[process].update(process_facility_data)
            else:
                # 집계할 데이터가 없으면 개별 시설 데이터만 포함
                result[process] = process_facility_data
                
        return result

    def get_sankey_diagram_data(self):
        """시설 이용 흐름을 분석하여 Sankey 다이어그램 데이터를 생성"""
        target_columns = [
            col
            for col in self.pax_df.columns
            if col.endswith("_pred") and not any(x in col for x in ["on", "done", "pt"])
        ]
        flow_df = self.pax_df.groupby(target_columns).size().reset_index(name="count")
        unique_values = {}
        current_index = 0
        for col in target_columns:
            unique_values[col] = {
                val: i + current_index for i, val in enumerate(flow_df[col].unique())
            }
            current_index += len(flow_df[col].unique())
        sources, targets, values = [], [], []
        for i in range(len(target_columns) - 1):
            col1, col2 = target_columns[i], target_columns[i + 1]
            grouped = flow_df.groupby([col1, col2])["count"].sum().reset_index()
            for _, row in grouped.iterrows():
                sources.append(unique_values[col1][row[col1]])
                targets.append(unique_values[col2][row[col2]])
                values.append(int(row["count"]))
        labels = []
        for col in target_columns:
            labels.extend(list(flow_df[col].unique()))
        return {
            "label": labels,
            "link": {"source": sources, "target": targets, "value": values},
        }

    # ===== 공통 유틸리티 함수들 =====
    def _get_process_list(self):
        """프로세스 목록을 추출"""
        return [
            col.replace("_on_pred", "")
            for col in self.pax_df.columns
            if "on_pred" in col
        ]

    def _format_timedelta(self, td):
        """Timedelta를 'XXh YYm ZZs' 형식으로 변환"""
        if pd.isna(td):
            return "0s"
        try:
            total_seconds = int(td.total_seconds())
            return self._format_waiting_time(total_seconds)
        except:
            return "0s"

    def _calculate_waiting_time(self, process_df, process):
        """대기 시간 계산 (Timedelta 반환)"""
        return process_df[f"{process}_pt_pred"] - process_df[f"{process}_on_pred"]

    def _calculate_waiting_time_minutes(self, process_df, process):
        """대기 시간 계산 (분 단위 반환)"""
        waiting_time = self._calculate_waiting_time(process_df, process)
        return waiting_time.dt.total_seconds()

    def _create_time_dataframe(self):
        """시간별 데이터프레임 생성"""
        days = self.pax_df["show_up_time"].dt.date.unique()[-1:]
        time_ranges = [
            pd.date_range(
                start=f"{date} 00:00:00", end=f"{date} 23:50:00", freq=self.time_unit
            )
            for date in days
        ]
        all_times = pd.DatetimeIndex(
            [dt for time_range in time_ranges for dt in time_range]
        )
        return pd.DataFrame(index=all_times).sort_index()

    # ===== 보조 함수들 =====
    def _extract_min_process_time(self, facility_info):
        """facility_info에서 각 노드별 기기별 최소 처리시간(min_per_device) 추출"""
        result = {}
        for component in facility_info["components"]:
            process = component["name"]
            result[process] = {}
            for node in component["nodes"]:
                facility_schedules = node["facility_schedules"]
                process_time = []
                for col in zip(*facility_schedules):
                    significant_values = [v for v in col if v > 1e-9]
                    if significant_values:
                        process_time.append(min(significant_values))
                    else:
                        process_time.append(None)
                result[process][node["name"]] = {
                    "min_per_device": process_time,
                    "facility_schedules": facility_schedules,
                }
        return result

    def _calculate_capacity_by_process_time(self, min_per_device, slot_seconds):
        """기기별 최소 처리시간으로부터 슬롯/하루 처리량 계산"""
        slots_per_day = 86400 // slot_seconds
        capacity_per_slot = [
            math.floor(slot_seconds / t) if t and t > 0 else None
            for t in min_per_device
        ]
        installed_capacity = [
            c * slots_per_day if c is not None else None for c in capacity_per_slot
        ]
        return capacity_per_slot, installed_capacity

    def _calculate_activated_capacity_per_device(
        self, facility_schedules, slot_seconds
    ):
        """실제 스케줄 기반 하루 처리량 계산 (기기별)"""
        num_devices = len(facility_schedules[0])
        device_totals = [0] * num_devices
        for slot in facility_schedules:
            for idx, t in enumerate(slot):
                if t and t > 1e-9:
                    device_totals[idx] += math.floor(slot_seconds / t)
        device_totals = [v if 0 < v < 1e9 else None for v in device_totals]
        return device_totals

    def _calculate_activated_per_installed(
        self, activated_capacity, installed_capacity
    ):
        """활성화된 용량 대비 설치된 용량 비율 계산"""
        activated_per_installed = []
        for op_cap, fac_cap in zip(activated_capacity, installed_capacity):
            if op_cap is not None and fac_cap is not None and fac_cap != 0:
                activated_per_installed.append(round((op_cap / fac_cap) * 100, 2))
            else:
                activated_per_installed.append(None)
        return activated_per_installed

    def _get_facility_counts(self, facility_info):
        """facility_info에서 각 노드별 시설 수 추출"""
        facility_counts = {}
        for component in facility_info["components"]:
            process = component["name"]
            facility_counts[process] = {}
            for node in component["nodes"]:
                facility_counts[process][node["name"]] = node["facility_count"]
        return facility_counts

    def _build_initial_facility_summary(self, slot_seconds):
        """시설 정보에서 초기 시설 요약 정보 생성"""
        complete_summary = {}

        # 시설 정보 추출
        min_time_result = self._extract_min_process_time(self.facility_info)

        # 시설 카운트 정보 가져오기
        facility_counts = self._get_facility_counts(self.facility_info)

        # 각 프로세스별로 처리
        for process, nodes in min_time_result.items():
            for node_name, data in nodes.items():
                min_per_device = data["min_per_device"]
                facility_schedules = data["facility_schedules"]
                capacity_per_slot, installed_capacity = (
                    self._calculate_capacity_by_process_time(
                        min_per_device, slot_seconds
                    )
                )
                activated_capacity = self._calculate_activated_capacity_per_device(
                    facility_schedules, slot_seconds
                )

                # activated_per_installed 계산
                activated_per_installed = self._calculate_activated_per_installed(
                    activated_capacity, installed_capacity
                )

                # 각 기기별로 정보 구성
                for device_idx in range(facility_counts[process][node_name]):
                    facility_key = f"{process}_{node_name}_{device_idx+1}"

                    # 임시 딕셔너리 생성
                    temp_dict = {}

                    # 해당 장비의 정보가 있는지 확인
                    if device_idx < len(min_per_device):
                        temp_dict["installed_capacity"] = installed_capacity[device_idx]
                        temp_dict["activated_capacity"] = activated_capacity[device_idx]
                        temp_dict["processed_pax"] = None  # 초기값 설정
                        temp_dict["activated_per_installed"] = activated_per_installed[
                            device_idx
                        ]
                        temp_dict["processed_per_activated"] = None  # 초기값 설정
                        temp_dict["processed_per_installed"] = None  # 초기값 설정
                    else:
                        # 기기 정보가 없는 경우
                        temp_dict["installed_capacity"] = None
                        temp_dict["activated_capacity"] = None
                        temp_dict["processed_pax"] = None
                        temp_dict["activated_per_installed"] = None
                        temp_dict["processed_per_activated"] = None
                        temp_dict["processed_per_installed"] = None

                    complete_summary[facility_key] = temp_dict

        return complete_summary

    def _add_processed_pax_info(self, facility_summary):
        """pax_df에서 처리된 승객 정보를 추출하여 시설 요약 정보에 추가"""
        for process in self.process_list:
            # 해당 프로세스의 모든 시설 목록 가져오기 (None 제외)
            if f"{process}_pred" in self.pax_df.columns:
                facilities = sorted(
                    [
                        f
                        for f in self.pax_df[f"{process}_pred"].unique()
                        if f is not None
                    ]
                )

                # 각 시설별로 처리
                for facility in facilities:
                    # 시설 이름에서 노드 이름 추출 (예: "check_in_A" -> "A")
                    if "_" in facility:
                        node_name = facility.split("_")[-1]
                    else:
                        continue

                    # 해당 시설의 데이터만 필터링
                    facility_df = self.pax_df[
                        self.pax_df[f"{process}_pred"] == facility
                    ].copy()

                    # 시설 번호별로 그룹화하여 처리된 여객 수 계산
                    if f"{process}_facility_number" in facility_df.columns:
                        facility_counts_series = facility_df.groupby(
                            f"{process}_facility_number"
                        ).size()

                        # 각 시설별로 처리된 승객 수 추가
                        for facility_idx, count in facility_counts_series.items():
                            facility_key = f"{process}_{node_name}_{facility_idx}"
                            if facility_key in facility_summary:
                                # processed_pax 값 업데이트
                                processed_pax = int(count)
                                facility_summary[facility_key][
                                    "processed_pax"
                                ] = processed_pax

                                # processed_per_activated 계산
                                op_cap = facility_summary[facility_key][
                                    "activated_capacity"
                                ]
                                if op_cap is not None and op_cap > 0:
                                    facility_summary[facility_key][
                                        "processed_per_activated"
                                    ] = round((processed_pax / op_cap) * 100, 2)
                                else:
                                    facility_summary[facility_key][
                                        "processed_per_activated"
                                    ] = None

                                # processed_per_installed 계산
                                fac_cap = facility_summary[facility_key][
                                    "installed_capacity"
                                ]
                                if fac_cap is not None and fac_cap > 0:
                                    facility_summary[facility_key][
                                        "processed_per_installed"
                                    ] = round((processed_pax / fac_cap) * 100, 2)
                                else:
                                    facility_summary[facility_key][
                                        "processed_per_installed"
                                    ] = None

        return facility_summary

    def _aggregate_metrics(self, facilities_data):
        """여러 시설의 메트릭을 집계"""
        if not facilities_data:
            return None

        # 집계할 데이터 초기화
        aggregated = {
            "installed_capacity": 0,
            "activated_capacity": 0,
            "processed_pax": 0,
            "activated_per_installed": None,
            "processed_per_activated": None,
            "processed_per_installed": None,
        }

        # 각 시설 데이터 집계
        for facility_data in facilities_data:
            # 용량 데이터는 합산
            if facility_data["installed_capacity"] is not None:
                aggregated["installed_capacity"] += facility_data["installed_capacity"]

            if facility_data["activated_capacity"] is not None:
                aggregated["activated_capacity"] += facility_data["activated_capacity"]

            if facility_data["processed_pax"] is not None:
                aggregated["processed_pax"] += facility_data["processed_pax"]

        # 비율 계산
        if aggregated["installed_capacity"] > 0:
            aggregated["activated_per_installed"] = round(
                (aggregated["activated_capacity"] / aggregated["installed_capacity"])
                * 100,
                2,
            )

            if aggregated["activated_capacity"] > 0:
                aggregated["processed_per_activated"] = round(
                    (aggregated["processed_pax"] / aggregated["activated_capacity"])
                    * 100,
                    2,
                )
            else:
                aggregated["processed_per_activated"] = None

            aggregated["processed_per_installed"] = round(
                (aggregated["processed_pax"] / aggregated["installed_capacity"]) * 100,
                2,
            )
        else:
            aggregated["activated_per_installed"] = None
            aggregated["processed_per_activated"] = None
            aggregated["processed_per_installed"] = None

        return aggregated

    def _collect_node_values(self):
        """pax_df에서 각 프로세스별 노드 값 수집"""
        process_nodes = {}
        for process in self.process_list:
            if f"{process}_pred" in self.pax_df.columns:
                nodes = set()
                for facility in self.pax_df[f"{process}_pred"].unique():
                    if facility is not None:
                        node_name = facility.split("_")[-1]
                        nodes.add(node_name)
                process_nodes[process] = sorted(list(nodes))
        return process_nodes

    def _extract_process_node_info(self, facility_key):
        """
        시설 키에서 프로세스, 노드, 인덱스 정보를 추출

        Args:
            facility_key (str): 시설 키 (예: "check_in_A_1")

        Returns:
            tuple: (프로세스, 노드, 인덱스) 정보, 추출 실패시 None
        """
        # 프로세스 목록에 있는 프로세스인지 확인
        for process in self.process_list:
            if facility_key.startswith(process + "_"):
                # 프로세스 제거 후 나머지 부분
                remainder = facility_key[len(process) + 1 :]

                # 마지막 언더스코어 위치 찾기
                last_underscore = remainder.rfind("_")

                if last_underscore != -1:
                    # 노드와 인덱스 분리
                    node = remainder[:last_underscore]
                    idx = remainder[last_underscore + 1 :]

                    # 인덱스가 숫자인지 확인
                    if idx.isdigit():
                        return process, node, int(idx)

        return None

    def _aggregate_node_level(self, facility_summary, process, node):
        """노드 레벨의 집계 수행"""
        node_facilities = []

        # 해당 노드에 속하는 시설 찾기
        for facility_key, data in facility_summary.items():
            # 각 시설 키에서 프로세스, 노드, 인덱스 추출
            key_info = self._extract_process_node_info(facility_key)

            # 추출 성공 및 해당 프로세스와 노드가 일치하는지 확인
            if key_info and key_info[0] == process and key_info[1] == node:
                node_facilities.append(data)

        # 집계 수행
        if node_facilities:
            return self._aggregate_metrics(node_facilities)
        return None

    def _aggregate_process_level(self, facility_summary, process):
        """프로세스 레벨의 집계 수행"""
        process_facilities = []

        # 해당 프로세스에 속하는 모든 개별 시설 찾기
        for facility_key, data in facility_summary.items():
            # 각 시설 키에서 프로세스 정보 추출
            key_info = self._extract_process_node_info(facility_key)

            # 추출 성공 및 해당 프로세스가 일치하는지 확인
            if key_info and key_info[0] == process:
                process_facilities.append(data)

        # 집계 수행
        if process_facilities:
            return self._aggregate_metrics(process_facilities)
        return None

    def _aggregate_all_facilities(self, facility_summary):
        """전체 시설 레벨의 집계 수행"""
        all_facilities = []

        # 유효한 모든 시설 포함
        for facility_key, data in facility_summary.items():
            if self._extract_process_node_info(facility_key):  # 유효한 키인지 확인
                all_facilities.append(data)

        return self._aggregate_metrics(all_facilities)

    def make_facility_ratio(self, slot_seconds=600):
        """
        시설 정보를 계층적으로 집계하여 하나의 딕셔너리에 통합

        Args:
            slot_seconds (int): 슬롯당 초 수 (기본값: 600)

        Returns:
            dict: 계층적으로 집계된 시설 정보
        """
        # 1. 개별 시설 레벨의 초기 요약 정보 생성
        facility_summary = self._build_initial_facility_summary(slot_seconds)

        # 2. pax_df에서 처리된 승객 정보 추가
        facility_summary = self._add_processed_pax_info(facility_summary)

        # 3. 임시 저장소 딕셔너리 초기화
        temp_ratio = {}

        # 4. 전체 시설 집계
        all_facility_metrics = self._aggregate_all_facilities(facility_summary)

        # 5. 프로세스 레벨 집계
        process_metrics = {}
        for process in self.process_list:
            metrics = self._aggregate_process_level(facility_summary, process)
            if metrics:
                process_metrics[process] = metrics

        # 6. 노드 레벨 집계
        process_nodes = self._collect_node_values()
        node_metrics = {}
        for process, nodes in process_nodes.items():
            for node in nodes:
                node_key = f"{process}_{node}"
                metrics = self._aggregate_node_level(facility_summary, process, node)
                if metrics:
                    node_metrics[node_key] = metrics

        # 7. 최종 결과 딕셔너리 구성 (위계 순서대로)
        sorted_ratio = {}

        # 먼저 all_facility 추가
        sorted_ratio["all_facility"] = all_facility_metrics

        # 그 다음 프로세스 추가
        for process in self.process_list:
            if process in process_metrics:
                sorted_ratio[process] = process_metrics[process]

                # 해당 프로세스의 노드 추가
                if process in process_nodes:
                    for node in process_nodes[process]:
                        node_key = f"{process}_{node}"
                        if node_key in node_metrics:
                            sorted_ratio[node_key] = node_metrics[node_key]

                            # 해당 노드의 개별 시설 추가
                            for facility_key in facility_summary.keys():
                                key_info = self._extract_process_node_info(facility_key)
                                if (
                                    key_info
                                    and key_info[0] == process
                                    and key_info[1] == node
                                ):
                                    sorted_ratio[facility_key] = facility_summary[
                                        facility_key
                                    ]

        return sorted_ratio

    def _calculate_kpi_values(self, method):
        """KPI 값 계산"""
        if method == "waiting_time":
            process_times = [
                self._calculate_waiting_time_minutes(self.pax_df, process)
                for process in self.process_list
            ]
            process_times_df = pd.concat(process_times, axis=1)
            # NaN이 하나라도 있는 행은 제거
            process_times_df = process_times_df.dropna()
            all_pax_data = process_times_df.sum(axis=1).tolist()
        elif method == "queue_length":
            queue_lengths = [
                self.pax_df[f"{process}_que"] for process in self.process_list
            ]
            queue_lengths_df = pd.concat(queue_lengths, axis=1)
            queue_lengths_df = queue_lengths_df.dropna()
            all_pax_data = queue_lengths_df.sum(axis=1).tolist()
        else:
            all_pax_data = []

        if not all_pax_data:
            return np.nan
        if self.calculate_type == "mean":
            return round(np.mean(all_pax_data))
        else:
            return round(np.percentile(all_pax_data, 100 - self.percentile))

    def _create_process_dataframe(self, process, time_interval="30min"):
        """각 프로세스별 데이터프레임 생성"""
        return pd.DataFrame(
            {
                "datetime": self.pax_df[f"{process}_on_pred"].dt.floor(time_interval),
                "waiting_time": self._calculate_waiting_time(self.pax_df, process),
                "queue_length": self.pax_df[f"{process}_que"],
                "process_name": self.pax_df[f"{process}_pred"],
                "process": process,
            }
        )

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

    def _create_alert_data_entry(self, row):
        """각 행을 알림 데이터 형식으로 변환"""
        return {
            "time": row["datetime"].strftime("%H:%M:%S"),
            "waiting_time": row["waiting_time"],
            "queue_length": row["queue_length"],
            "node": row["process_name"],
        }

    def _calculate_overview_metrics(self, df, process):
        """프로세스 전체 개요 지표 계산 (UI 맞춤, opened 계산 개선, ai/pa/pi_ratio 노드 합산)"""

        def get_opened_count():
            if not self.facility_info or "components" not in self.facility_info:
                return [0, 0]
            for comp in self.facility_info["components"]:
                if comp["name"] == process:
                    opened = 0
                    total = 0
                    for node in comp["nodes"]:
                        schedules = node.get("facility_schedules", [])
                        try:
                            per_facility = list(zip(*schedules))
                        except Exception:
                            per_facility = []
                        for col in per_facility:
                            if any((v is not None and v > 0) for v in col):
                                opened += 1
                        total += node.get("facility_count", len(per_facility))
                    return [opened, total]
            return [0, 0]

        def get_node_level_keys(process):
            prefix = process + "_"
            return [
                k
                for k in self.facility_ratio.keys()
                if k.startswith(prefix) and "_" not in k[len(prefix) :]
            ]

        def calc_process_ratios(process):
            sub_keys = get_node_level_keys(process)
            installed = sum(
                self.facility_ratio[k].get("installed_capacity", 0) or 0
                for k in sub_keys
            )
            activated = sum(
                self.facility_ratio[k].get("activated_capacity", 0) or 0
                for k in sub_keys
            )
            processed = sum(
                self.facility_ratio[k].get("processed_pax", 0) or 0 for k in sub_keys
            )
            ai_ratio = round((activated / installed) * 100, 2) if installed else None
            pa_ratio = round((processed / activated) * 100, 2) if activated else None
            pi_ratio = round((processed / installed) * 100, 2) if installed else None
            return ai_ratio, pa_ratio, pi_ratio

        opened_count = get_opened_count()
        waiting_time = self._calculate_waiting_time(df, process)

        def get_stat(series):
            if series.empty:
                return 0
            if self.calculate_type == "top":
                return series.quantile(1 - self.percentile / 100)
            return series.mean()

        ai_ratio, pa_ratio, pi_ratio = calc_process_ratios(process)

        return {
            "opened": opened_count,
            "throughput": len(df),
            "queuePax": int(get_stat(df[f"{process}_que"])),
            "waitTime": self._format_timedelta(get_stat(waiting_time)),
            "ai_ratio": ai_ratio,
            "pa_ratio": pa_ratio,
            "pi_ratio": pi_ratio,
        }

    def _calculate_waiting_time_distribution(self, process, df=None):
        """대기 시간 분포를 계산"""
        if df is None:
            df = self.pax_df
        waiting_time_minutes = self._calculate_waiting_time_minutes(df, process)
        if waiting_time_minutes.empty:
            return []
        bins = [0, 15, 30, 45, 60, float("inf")]
        labels = ["00:00-15:00", "15:00-30:00", "30:00-45:00", "45:00-60:00", "60:00-"]
        time_groups = pd.cut(
            waiting_time_minutes, bins=bins, labels=labels, right=False
        )
        # 모든 레이블에 대한 카운트를 얻기 위해 reindex 사용
        all_counts = time_groups.value_counts().reindex(labels, fill_value=0)
        total = all_counts.sum()
        if total == 0:
            percentages = all_counts # 모두 0
        else:
            percentages = ((all_counts / total) * 100).round(0)

        return [
            {"title": label, "value": int(percentages[label]), "unit": "%"}
            for label in labels
        ]

    def _calculate_queue_length_distribution(self, process, df=None):
        """대기열 길이 분포를 계산"""
        if df is None:
            df = self.pax_df
        if f"{process}_que" not in df.columns or df[f"{process}_que"].empty:
            return []

        bins = [0, 50, 100, 150, 200, 250, float("inf")]
        labels = ["0-50", "50-100", "100-150", "150-200", "200-250", "250+"]
        queue_groups = pd.cut(
            df[f"{process}_que"], bins=bins, labels=labels, right=False
        )
        # 모든 레이블에 대한 카운트를 얻기 위해 reindex 사용
        all_counts = queue_groups.value_counts().reindex(labels, fill_value=0)
        total = all_counts.sum()
        if total == 0:
            percentages = all_counts # 모두 0
        else:
            percentages = ((all_counts / total) * 100).round(0)

        return [
            {"title": label, "value": int(percentages[label]), "unit": "%"}
            for label in labels
        ]

    def _create_facility_queue_dataframe(self):
        """시간별 시설 대기열 데이터프레임 생성

        Returns:
            pd.DataFrame: 시간별 각 시설의 대기열 길이를 보여주는 데이터프레임
        """
        # 시간별 데이터프레임 생성
        time_df = self._create_time_dataframe()

        # 각 프로세스별로 시설 목록과 대기열 데이터 추가
        for process in self.process_list:
            # 해당 프로세스의 모든 시설 목록 가져오기
            facilities = sorted(self.pax_df[f"{process}_pred"].unique())

            # 각 시설별로 대기열 데이터 추가
            for facility in facilities:
                # 해당 시설의 데이터만 필터링
                facility_df = self.pax_df[
                    self.pax_df[f"{process}_pred"] == facility
                ].copy()

                # 시간별로 대기열 평균 계산
                queue_data = facility_df.groupby(
                    facility_df[f"{process}_on_pred"].dt.floor(self.time_unit)
                )[f"{process}_que"].mean()

                # 데이터프레임에 추가 (컬럼명 중복 제거)
                time_df[f"{facility}"] = queue_data.round(2).fillna(0)
        time_df.fillna(0, inplace=True)
        return time_df

    def _parse_range(self, title, mode):
        # title: "00:00-15:00" or "0-50" etc.
        if mode == "waiting_time":
            # "00:00-15:00" -> [0, 15], "60:00-" -> [60, None]
            if ":" in title:
                start, rest = title.split("-")
                start_min = int(start.split(":")[0]) * 60 + int(start.split(":")[1])
                start_min = start_min // 60  # 초 -> 분 변환
                if rest == "":
                    return [start_min, None]
                end_min = int(rest.split(":")[0]) * 60 + int(rest.split(":")[1]) if rest != "" else None
                end_min = end_min // 60 if end_min is not None else None  # 초 -> 분 변환
                return [start_min, end_min]
            elif "-" in title:
                start, end = title.split("-")
                return [int(start), int(end) if end else None]
            else:
                return [0, None]
        else:
            # queue_length: "0-50", "250+"
            if "+" in title:
                return [int(title.replace("+", "")), None]
            elif "-" in title:
                start, end = title.split("-")
                return [int(start), int(end) if end else None]
            else:
                return [0, None]

    def _calculate_node_capacity_list(self, process_name, node_name):
        """프로세스와 노드 이름을 기반으로 10분 단위 용량 리스트(144개)를 계산합니다."""
        if self.facility_info:
            for comp in self.facility_info.get("components", []):
                if comp["name"] == process_name:
                    for node in comp.get("nodes", []):
                        if node["name"] == node_name:
                            schedules = node.get("facility_schedules", [])
                            if schedules:
                                capacity_list = []
                                for slot in schedules:
                                    slot_capacity = Fraction(0)
                                    for t in slot:
                                        if t and t > 0:
                                            slot_capacity += Fraction(600, 1) / Fraction(t)
                                    capacity_list.append(int(slot_capacity))
                                return capacity_list
        return []

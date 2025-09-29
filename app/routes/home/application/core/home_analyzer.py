from typing import Any, Dict, Optional

import numpy as np
import pandas as pd


class HomeAnalyzer:
    def __init__(
        self,
        pax_df: pd.DataFrame,
        facility_info: Optional[Dict[str, Any]] = None,
        calculate_type: str = "mean",
        percentile: int | None = None,
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

        self.facility_info = None
        self.calculate_type = calculate_type
        self.percentile = percentile
        self.time_unit = "10min"
        self.process_list = self._get_process_list()
        self.facility_ratio = {}

    # ===============================
    # 메인 함수들
    # ===============================

    def get_summary(self):
        """요약 데이터 생성"""
        # 기본 데이터 계산
        throughput = int(self.pax_df[f"{self.process_list[-1]}_done_time"].notna().sum())

        waiting_times_df = pd.DataFrame(
            {
                process: self.pax_df[f"{process}_waiting_time"]
                for process in self.process_list
            }
        ).dropna()
        waiting_time_data = self._get_pax_experience_data(
            waiting_times_df, "time", self.calculate_type, self.percentile
        )

        queue_lengths_df = pd.DataFrame(
            {process: self.pax_df[f"{process}_queue_length"] for process in self.process_list}
        ).dropna()
        queue_data = self._get_pax_experience_data(
            queue_lengths_df, "count", self.calculate_type, self.percentile
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
        """플로우 차트 데이터 생성"""
        time_unit = time_unit or self.time_unit
        time_df = self._create_time_df_index(time_unit)
        data = {"times": time_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()}

        for process in self.process_list:
            facilities = sorted(self.pax_df[f"{process}_zone"].dropna().unique())
            if not facilities:
                data[process] = {}
                continue

            process_data = self.pax_df[self.pax_df[f"{process}_zone"].notna()].copy()
            process_data[f"{process}_waiting"] = process_data[f"{process}_waiting_time"].dt.total_seconds()

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
                )[f"{process}_waiting"].mean(),
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

            for facility_name in facilities:
                node_name = facility_name.split("_")[-1]

                facility_data = {
                    k: pivoted[k].get(facility_name, pd.Series(0, index=time_df.index))
                    for k in metrics.keys()
                }

                # 집계
                for k in facility_data.keys():
                    aggregated[k] += facility_data[k]

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

            # all_zones
            facility_count = len(facilities)
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

            data[process] = {"all_zones": all_zones_data, **process_facility_data}
        return data

    def get_facility_details(self):
        """시설 세부 정보 생성"""

        if self.calculate_type != "mean" and self.percentile is None:
            raise ValueError(
                "percentile 방식을 사용하려면 percentile 값을 제공해야 합니다."
            )

        data = []
        for process in self.process_list:
            cols = [
                f"{process}_{x}"
                for x in ["zone", "facility", "queue_length", "on_pred", "done_time", "waiting_time"]
            ]
            process_df = self.pax_df[cols].copy()

            # Overview 계산
            waiting_time = self._calculate_waiting_time(process_df, process)

            overview = {
                "throughput": len(process_df),
                "queuePax": int(
                    process_df[f"{process}_queue_length"].quantile(1 - self.percentile / 100)
                    if self.calculate_type == "top"
                    else process_df[f"{process}_queue_length"].mean()
                ),
                "waitTime": self._format_waiting_time(
                    waiting_time.quantile(1 - self.percentile / 100)
                    if self.calculate_type == "top"
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
                            if self.calculate_type == "top"
                            else facility_df[f"{process}_queue_length"].mean()
                        ),
                        "waitTime": self._format_waiting_time(
                            waiting_time.quantile(1 - self.percentile / 100)
                            if self.calculate_type == "top"
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
                wt_mins = df[f"{process}_waiting_time"].dt.total_seconds() / 60
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
        """산키 다이어그램 데이터 생성"""
        # facility 기반으로 승객 플로우 생성 (시간 순서로 정렬)
        facility_cols = [
            col for col in self.pax_df.columns 
            if col.endswith("_facility")
        ]
        
        # {process}_done_time 기준으로 시간 순서 정렬
        timed_facilities = []
        for col in facility_cols:
            process_name = col.replace("_facility", "")
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
            target_columns = facility_cols

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

        return {
            "label": labels,
            "link": {"source": sources, "target": targets, "value": values},
        }



    def get_etc_info(self):
        """기본 시뮬레이션 정보 생성"""

        df = self.pax_df.copy()

        # 1. 공항 이름
        airport_code = ""
        if (
            "departure_airport_iata" in df.columns
            and not df["departure_airport_iata"].empty
        ):
            airport_code = df["departure_airport_iata"].iloc[0]
        elif (
            "arrival_airport_iata" in df.columns
            and not df["arrival_airport_iata"].empty
        ):
            airport_code = df["arrival_airport_iata"].iloc[0]

        # 2. 첫 번째/마지막 승객 도착 시간
        first_showup_passenger = None
        last_showup_passenger = None
        if "show_up_time" in df.columns and not df["show_up_time"].empty:
            first_showup_passenger = (
                df["show_up_time"].min().strftime("%Y-%m-%d %H:%M:%S")
            )
            last_showup_passenger = (
                df["show_up_time"].max().strftime("%Y-%m-%d %H:%M:%S")
            )

        # 3. 항공기를 놓친 승객 수
        missed_flight_passengers = 0
        # 4. 정시 처리 승객 수
        ontime_flight_passengers = 0
        # 5. 상업시설 이용시간 평균 (분 단위)
        commercial_usage_time_avg = 0
        # 6. 평균 공항 체류 시간 (분 단위)
        avg_airport_stay_time = 0
        # 7. 항공기 출발전 처리완료된 승객비율 (%)
        passengers_processed_before_departure_rate = 0.0
        # 8. 러시아워 (가장 붐비는 시간대)
        rush_hour = "N/A"
        # 9. 병목 프로세스 (가장 오래 걸리는 프로세스)
        bottleneck_process = "N/A"
        # 10. 얼리버드 승객 비율 (2시간 이전 도착)
        early_bird_ratio = 0.0

        if self.process_list and "scheduled_departure_local" in df.columns:
            last_process = self.process_list[-1]
            last_done_col = f"{last_process}_done_time"

            if last_done_col in df.columns and "show_up_time" in df.columns:
                # 유효한 데이터만 필터링 (done 시간과 출발 시간이 모두 있는 경우)
                valid_data = df.dropna(
                    subset=[
                        last_done_col,
                        "scheduled_departure_local",
                        "show_up_time",
                    ]
                )

                if not valid_data.empty:
                    # 승객 분류
                    missed_passengers_data = valid_data[
                        valid_data[last_done_col]
                        > valid_data["scheduled_departure_local"]
                    ]
                    ontime_passengers_data = valid_data[
                        valid_data[last_done_col]
                        <= valid_data["scheduled_departure_local"]
                    ]

                    # 3. 항공기를 놓친 승객 수
                    missed_flight_passengers = len(missed_passengers_data)

                    # 4. 정시 처리 승객 수
                    ontime_flight_passengers = len(ontime_passengers_data)

                    # 5. 상업시설 이용시간 평균 계산 (모든 승객 대상, 음수 포함, 총 분 단위)
                    commercial_time_diff = (
                        valid_data["scheduled_departure_local"]
                        - valid_data[last_done_col]
                    )
                    avg_commercial_time_seconds = (
                        commercial_time_diff.mean().total_seconds()
                    )
                    commercial_usage_time_avg = int(avg_commercial_time_seconds / 60)

                    # 6. 평균 공항 체류 시간 계산 (show_up_time부터 실제 공항 떠나는 시점까지)
                    # 실제 떠나는 시점 = max(마지막 프로세스 완료 시간, 항공기 출발 시간)
                    actual_departure_time = pd.concat(
                        [
                            valid_data[last_done_col],
                            valid_data["scheduled_departure_local"],
                        ],
                        axis=1,
                    ).max(axis=1)

                    airport_stay_time_diff = (
                        actual_departure_time - valid_data["show_up_time"]
                    )
                    avg_airport_stay_seconds = (
                        airport_stay_time_diff.mean().total_seconds()
                    )
                    avg_airport_stay_time = int(avg_airport_stay_seconds / 60)

        # 8. 러시아워 분석 (가장 붐비는 시간대)
        if "show_up_time" in df.columns and not df["show_up_time"].empty:
            hourly_counts = df["show_up_time"].dt.hour.value_counts()
            peak_hour = hourly_counts.index[0]
            rush_hour = f"{peak_hour:02d}:00-{peak_hour+1:02d}:00"

        # 9. 병목 프로세스 분석 (가장 평균 대기시간이 긴 프로세스)
        if self.process_list:
            process_wait_times = {}
            for process in self.process_list:
                waiting_col = f"{process}_waiting_time"
                if waiting_col in df.columns:
                    wait_time = df[waiting_col].dt.total_seconds().mean()
                    if not pd.isna(wait_time):
                        process_wait_times[process] = wait_time

            if process_wait_times:
                bottleneck_process = max(process_wait_times, key=process_wait_times.get)

        # 10. 얼리버드 승객 비율 (2시간 이전 도착)
        if (
            "show_up_time" in df.columns
            and "scheduled_departure_local" in df.columns
        ):
            early_arrival_data = df.dropna(
                subset=["show_up_time", "scheduled_departure_local"]
            )
            if not early_arrival_data.empty:
                time_before_departure = (
                    early_arrival_data["scheduled_departure_local"]
                    - early_arrival_data["show_up_time"]
                ).dt.total_seconds() / 3600  # 시간 단위로 변환

                early_birds = early_arrival_data[time_before_departure >= 2]
                early_bird_ratio = round(
                    (len(early_birds) / len(early_arrival_data)) * 100, 2
                )

        # 쇼핑 가능 여부 판단
        shopping_available = (
            "Possible" if commercial_usage_time_avg >= 0 else "Impossible"
        )

        return {
            "simulation_basic_info": {
                "airport_code": {
                    "value": airport_code,
                    "description": "Airport IATA code (e.g., ICN, NRT)",
                },
                "first_showup_passenger": {
                    "value": first_showup_passenger,
                    "description": "Arrival time of the first passenger at the airport",
                },
                "last_showup_passenger": {
                    "value": last_showup_passenger,
                    "description": "Arrival time of the last passenger at the airport",
                },
            },
            "performance_kpi": {
                "missed_flight_passengers": {
                    "value": missed_flight_passengers,
                    "description": "Number of passengers who completed final process after flight departure time",
                },
                "ontime_flight_passengers": {
                    "value": ontime_flight_passengers,
                    "description": "Number of passengers who completed all processes before flight departure",
                },
                "avg_airport_dwell_time(min)": {
                    "value": avg_airport_stay_time,
                    "description": "Average passenger dwell time at airport (arrival to actual departure from airport)",
                },
            },
            "commercial_info": {
                "commercial_facility_usage_time_avg(min)": {
                    "value": commercial_usage_time_avg,
                    "description": "Average available time for commercial facilities after final process completion",
                },
                "shopping_available": {
                    "value": shopping_available,
                    "description": "Shopping availability based on average spare time (Possible if positive, Impossible if negative)",
                },
            },
            "operational_insights": {
                "rush_hour": {
                    "value": rush_hour,
                    "description": "Peak hour when most passengers arrive at the airport",
                },
                "bottleneck_process": {
                    "value": bottleneck_process,
                    "description": "Process with the longest average waiting time (improvement priority)",
                },
                "early_bird_ratio(%)": {
                    "value": early_bird_ratio,
                    "description": "Percentage of passengers arriving 2+ hours before departure",
                },
            },
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

    def _get_pax_experience_data(self, df, data_type, calculate_type, percentile):
        """승객 경험 데이터 계산"""
        df["total"] = df.sum(axis=1)
        if calculate_type == "mean":
            target_value = df["total"].mean()
        else:
            target_value = np.percentile(df["total"], 100 - percentile)

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
                "waiting_time": self.pax_df[f"{process}_waiting_time"],
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


    def _calculate_waiting_time(self, process_df, process):
        """대기 시간 계산"""
        return process_df[f"{process}_waiting_time"]







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

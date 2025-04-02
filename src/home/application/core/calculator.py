import pandas as pd
import numpy as np


class HomeCalculator:
    def __init__(
        self,
        pax_df: pd.DataFrame,
        calculate_type: str = "mean",
        percentile: int | None = None,
    ):
        self.pax_df = pax_df
        self.calculate_type = calculate_type
        self.percentile = percentile
        self.time_unit = "30min"
        self.process_list = self._get_process_list()

    # ===== 메인 함수들 =====
    def get_summary(self):
        """메인 함수: 요약 데이터를 생성하여 반환"""
        departure_flights = self.pax_df["flight_number"].nunique()
        delayed_flights = self.pax_df[self.pax_df["gate_departure_delay"] > 15][
            "flight_number"
        ].nunique()
        cancelled_flights = int(self.pax_df["is_cancelled"].sum())
        total_pax = len(self.pax_df)
        throughput = int(self.pax_df[f"{self.process_list[-1]}_pt_pred"].notna().sum())
        waiting_time = self._calculate_kpi_values(method="waiting_time")
        waiting_time = f"{waiting_time // 60:02d}:{waiting_time % 60:02d}"
        queue_length = self._calculate_kpi_values(method="queue_length")
        facility_utilizations = []
        for process in self.process_list:
            utilization = (
                self.pax_df[f"{process}_done_pred"].dt.floor("10min").nunique() / 144
            ) * 100
            facility_utilizations.append(utilization)
        facility_utilization = sum(facility_utilizations) / len(facility_utilizations)

        data = {
            "normal": [
                {"title": "Departure Flights", "value": departure_flights},
                {"title": "Arrival Flights", "value": 0},
                {
                    "title": "Delay / Return",
                    "value": [delayed_flights, cancelled_flights],
                },
                {"title": "Departure Pax", "value": total_pax},
                {"title": "Arrival Pax", "value": 0},
                {"title": "Transfer Pax", "value": 0},
            ],
            "kpi": [
                {"title": "Passenger Throughput", "value": throughput},
                {"title": "Wait Time", "value": waiting_time},
                {"title": "Queue Length", "value": queue_length},
                {
                    "title": "Facility Utilization",
                    "value": f"{facility_utilization:.1f}%",
                },
            ],
        }
        return data

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
            # alert_json["data"].append(
            #     {
            #         process: [
            #             self._create_alert_data_entry(row)
            #             for _, row in process_data.iterrows()
            #         ]
            #     }
            # )
            alert_json[process] = [
                self._create_alert_data_entry(row) for _, row in process_data.iterrows()
            ]

        return alert_json

    def get_facility_details(self):
        """시설별 세부 데이터를 계산하고 반환"""
        result = []
        for process in self.process_list:
            cols_needed = [
                f"{process}_pred",
                f"{process}_que",
                f"{process}_pt",
                f"{process}_on_pred",
                f"{process}_pt_pred",
            ]
            process_df = self.pax_df[cols_needed].copy()
            overview = self._calculate_overview_metrics(process_df, process)
            category_obj = {"category": process, "overview": overview, "components": []}
            self._add_facility_components(process_df, process, category_obj)
            result.append(category_obj)
        return result

    def get_flow_chart_data(self):
        """시간별 대기열 및 대기 시간 데이터 생성"""
        time_df = self._create_time_dataframe()
        time_df = self._add_queue_data(time_df)
        time_df.fillna(0, inplace=True)
        times = time_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()
        components = []
        for column in time_df.columns:
            values = (
                time_df[column].astype(float).tolist()
                if time_df[column].dtype.kind in "ifc"
                else time_df[column].tolist()
            )
            components.append({"label": column, "values": values})
        return {"x_values": times, "y_values": components}

    def get_histogram_data(self):
        """시설별 통계 데이터 생성"""
        # facility_data = [
        #     {
        #         process: {
        #             "waiting_time": self._calculate_waiting_time_distribution(process),
        #             "queue_length": self._calculate_queue_length_distribution(process),
        #         },
        #     }
        #     for process in self.process_list
        # ]

        # all_facility_data = self._calculate_average_distribution(facility_data)
        # return {"data": [{"All Facility": all_facility_data}] + facility_data}
        result = {}
        for process in self.process_list:
            result[process] = {
                "waiting_time": self._calculate_waiting_time_distribution(process),
                "queue_length": self._calculate_queue_length_distribution(process),
            }
        all_facility_data = self._calculate_average_distribution([result])
        result["all_facility"] = all_facility_data

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

    def _format_seconds_to_time(self, seconds):
        """초 단위 값을 HH:MM:SS 형식으로 변환"""
        if pd.isna(seconds):
            return "00:00:00"
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _format_timedelta(self, td):
        """Timedelta를 HH:MM:SS 형식으로 변환"""
        if pd.isna(td):
            return "00:00:00"
        try:
            return str(td).split()[-1].split(".")[0]
        except:
            return "00:00:00"

    def _calculate_waiting_time(self, process_df, process):
        """대기 시간 계산 (Timedelta 반환)"""
        return process_df[f"{process}_pt_pred"] - process_df[f"{process}_on_pred"]

    def _calculate_waiting_time_minutes(self, process_df, process):
        """대기 시간 계산 (분 단위 반환)"""
        waiting_time = self._calculate_waiting_time(process_df, process)
        return waiting_time.dt.total_seconds() / 60

    def _create_time_dataframe(self):
        """시간별 데이터프레임 생성"""
        days = self.pax_df["show_up_time"].dt.date.unique()
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
    def _calculate_kpi_values(self, method):
        """KPI 값 계산"""
        all_pax_data = []
        if method == "waiting_time":
            for process in self.process_list:
                process_time = self._calculate_waiting_time_minutes(
                    self.pax_df, process
                )
                all_pax_data.extend(process_time.dropna().tolist())
        elif method == "queue_length":
            for process in self.process_list:
                queue_length = self.pax_df[f"{process}_que"]
                all_pax_data.extend(queue_length.dropna().tolist())
        if not all_pax_data:
            return np.nan
        if self.percentile is None:
            return round(np.mean(all_pax_data))
        else:
            return round(np.percentile(all_pax_data, 100 - self.percentile))

    def _create_process_dataframe(self, process, time_interval="30min"):
        """각 프로세스별 데이터프레임 생성"""
        return pd.DataFrame(
            {
                "datetime": self.pax_df[f"{process}_on_pred"].dt.floor(time_interval),
                "waiting_time": self.pax_df[f"{process}_pt_pred"]
                - self.pax_df[f"{process}_on_pred"],
                "queue_length": self.pax_df[f"{process}_que"],
                "process_name": self.pax_df[f"{process}_pred"],
                "process": process,
            }
        )

    def _format_waiting_time(self, timedelta):
        """대기 시간을 MM:SS 형식의 문자열로 변환"""
        return f"{int(timedelta.total_seconds() // 60):02d}:{int(timedelta.total_seconds() % 60):02d}"

    def _create_alert_data_entry(self, row):
        """각 행을 알림 데이터 형식으로 변환"""
        return {
            "time": row["datetime"].strftime("%H:%M:%S"),
            "waiting_time": row["waiting_time"],
            "queue_length": str(row["queue_length"]),
            "node": row["process_name"],
        }

    def _calculate_overview_metrics(self, df, process):
        """프로세스 전체 개요 지표 계산"""
        opened_count = df[f"{process}_pred"].nunique()
        waiting_time = self._calculate_waiting_time(df, process)
        return {
            "opened": [opened_count, opened_count],
            "isOpened": opened_count > 0,
            "throughput": len(df),
            "maxQueue": int(
                df[f"{process}_que"].max() if not df[f"{process}_que"].empty else 0
            ),
            "queueLength": int(
                df[f"{process}_que"].quantile(0.95)
                if not df[f"{process}_que"].empty
                else 0
            ),
            "procTime": self._format_seconds_to_time(
                df[f"{process}_pt"].quantile(0.95)
                if not df[f"{process}_pt"].empty
                else 0
            ),
            "waitTime": self._format_timedelta(
                waiting_time.quantile(0.95)
                if not waiting_time.empty
                else pd.Timedelta(0)
            ),
        }

    def _add_facility_components(self, df, process, category_obj):
        """시설별 컴포넌트를 계산하고 category_obj에 추가"""
        facilities = df[f"{process}_pred"].unique()
        for facility in facilities:
            facility_df = df[df[f"{process}_pred"] == facility]
            waiting_time = self._calculate_waiting_time(facility_df, process)
            component = {
                "title": facility,
                "opened": [1, 1] if not facility_df.empty else [0, 0],
                "isOpened": not facility_df.empty,
                "throughput": len(facility_df),
                "maxQueue": int(
                    facility_df[f"{process}_que"].max()
                    if not facility_df[f"{process}_que"].empty
                    else 0
                ),
                "queueLength": int(
                    facility_df[f"{process}_que"].quantile(0.95)
                    if not facility_df[f"{process}_que"].empty
                    else 0
                ),
                "procTime": self._format_seconds_to_time(
                    facility_df[f"{process}_pt"].quantile(0.95)
                    if not facility_df[f"{process}_pt"].empty
                    else 0
                ),
                "waitTime": self._format_timedelta(
                    waiting_time.quantile(0.95)
                    if not waiting_time.empty
                    else pd.Timedelta(0)
                ),
            }
            category_obj["components"].append(component)

    def _add_queue_data(self, time_df):
        """대기열 및 대기 시간 데이터를 데이터프레임에 추가"""
        temp = self.pax_df.copy()
        for process in self.process_list:
            queue = temp.groupby(temp[f"{process}_on_pred"].dt.floor(self.time_unit))[
                f"{process}_que"
            ].mean()
            time_df[f"{process}_que"] = round(queue, 2).fillna(0)
            waiting_time_minutes = self._calculate_waiting_time_minutes(temp, process)
            waiting_time = waiting_time_minutes.groupby(
                temp[f"{process}_on_pred"].dt.floor(self.time_unit)
            ).mean()

            time_df[f"{process}_waiting_time"] = waiting_time.round(1).fillna(0.0)

            done_counts = (
                temp[f"{process}_done_pred"]
                .dropna()
                .dt.floor(self.time_unit)
                .value_counts()
                .sort_index()
            )
            time_df[f"{process}_throughput"] = 0
            for time_idx, count in done_counts.items():
                if time_idx in time_df.index:
                    time_df.loc[time_idx, f"{process}_throughput"] = count
        return time_df

    def _calculate_waiting_time_distribution(self, process):
        """대기 시간 분포를 계산"""
        waiting_time_minutes = self._calculate_waiting_time_minutes(
            self.pax_df, process
        )
        bins = [0, 15, 30, 45, 60, float("inf")]
        labels = ["00:00-15:00", "15:00-30:00", "30:00-45:00", "45:00-60:00", "60:00-"]
        time_groups = pd.cut(
            waiting_time_minutes, bins=bins, labels=labels, right=False
        )
        percentages = (time_groups.value_counts(normalize=True) * 100).round(1)
        return [
            {"title": label, "value": f"{percentages[label]:.0f}%"} for label in labels
        ]

    def _calculate_queue_length_distribution(self, process):
        """대기열 길이 분포를 계산"""
        bins = [0, 50, 100, 150, 200, 250, float("inf")]
        labels = ["0-50", "50-100", "100-150", "150-200", "200-250", "250+"]
        queue_groups = pd.cut(
            self.pax_df[f"{process}_que"], bins=bins, labels=labels, right=False
        )
        percentages = (
            (queue_groups.value_counts(normalize=True) * 100).round(1).sort_index()
        )
        return [
            {"title": label, "value": f"{percentages[label]:.0f}%"} for label in labels
        ]

    def _calculate_average_distribution(self, histogram_data):
        """전체 시설의 평균 분포를 계산"""
        all_waiting_time = {}
        all_queue_length = {}
        for facility in histogram_data:
            # facility의 첫 번째 (유일한) 키-값 쌍을 가져옴
            process_data = list(facility.values())[0]
            for wt in process_data["waiting_time"]:
                value = float(wt["value"].replace("%", ""))
                all_waiting_time.setdefault(wt["title"], []).append(value)
            for ql in process_data["queue_length"]:
                value = float(ql["value"].replace("%", ""))
                all_queue_length.setdefault(ql["title"], []).append(value)

        avg_waiting_time = [
            {"title": title, "value": f"{sum(values)/len(values):.0f}%"}
            for title, values in all_waiting_time.items()
        ]
        avg_queue_length = [
            {"title": title, "value": f"{sum(values)/len(values):.0f}%"}
            for title, values in all_queue_length.items()
        ]
        return {"waiting_time": avg_waiting_time, "queue_length": avg_queue_length}

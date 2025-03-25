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
        self.time_unit = "10min"

    # Summary
    def _calculate_kpi_values(self, method):
        # 모든 프로세스의 처리 시간 차이를 계산하여 하나의 리스트로 합침
        all_pax_data = []

        process_list = [
            col.replace("_on_pred", "")
            for col in self.pax_df.columns
            if "_on_pred" in col
        ]

        if method == "wait_time":
            for process in process_list:
                # 각 프로세스의 처리 시간 계산 (분 단위)
                process_time = (
                    self.pax_df[f"{process}_pt_pred"]
                    - self.pax_df[f"{process}_on_pred"]
                ).dt.total_seconds()

                all_pax_data.extend(process_time.dropna().tolist())
        elif method == "queue_length":
            for process in process_list:
                queue_length = self.pax_df[f"{process}_que"]
                all_pax_data.extend(queue_length.dropna().tolist())

        if not all_pax_data:  # 빈 리스트인 경우
            return np.nan

        if self.percentile is None:
            # 전체 평균 반환
            return round(np.mean(all_pax_data))
        else:
            # # 상위 n% 값들의 평균 계산
            # threshold = np.percentile(all_pax_data, 100 - self.percentile)
            # top_values = [x for x in all_pax_data if x >= threshold]
            # return round(np.mean(top_values))
            # 상위 n% 지점의 값을 반환
            return round(np.percentile(all_pax_data, 100 - self.percentile))

    def get_summary(self):
        """메인 함수: 요약 데이터를 생성하여 반환"""
        # 기본 데이터 계산
        departure_flights = self.pax_df["flight_number"].nunique()
        delayed_flights = self.pax_df[self.pax_df["gate_departure_delay"] > 15][
            "flight_number"
        ].nunique()
        cancelled_flights = int(
            pd.to_numeric(
                self.pax_df["is_cancelled"]
                .astype(str)
                .str.strip()
                .map({"True": 1, "False": 0}),
                errors="coerce",
            ).sum()
        )
        total_pax = len(self.pax_df)

        # KPI 데이터
        throughput = int(self.pax_df["passport_pt_pred"].notna().sum())
        wait_time = self._calculate_kpi_values(method="wait_time")
        wait_time = f"{wait_time // 60:02d}:{wait_time % 60:02d}"
        queue_length = self._calculate_kpi_values(method="queue_length")

        return {
            "summary": [
                {
                    "label": "normal",
                    "data": [
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
                },
                {
                    "label": "KPI",
                    "data": [
                        {"title": "Passenger Throughput", "value": throughput},
                        {"title": "Wait Time", "value": wait_time},
                        {"title": "Queue Length", "value": queue_length},
                        {"title": "Facility Utilization", "value": "100%"},
                    ],
                },
            ]
        }

    # Alert Issues
    def get_alert_issues(self) -> dict:
        if self.percentile is not None and not 0 <= self.percentile <= 100:
            raise ValueError("percentile은 0에서 100 사이의 값이어야 합니다")

        result = self._analyze_all_processes(self.percentile)

        if self.percentile is None:
            column_name = "mean"
        else:
            column_name = f"{self.percentile}%"

        if column_name not in result.columns:
            raise ValueError(f"열 '{column_name}'이 데이터프레임에 존재하지 않습니다.")

        return {
            row["facility_id"]: [row[column_name], row["peak_time"], row["count"]]
            for _, row in result.iterrows()
        }

    def _analyze_time(self, process: str, percentile=None) -> pd.DataFrame:
        def to_hhmm(td: pd.Timedelta) -> str:
            if pd.isna(td):
                return td
            total_minutes = int(td.total_seconds() / 60)
            return f"{total_minutes // 60:02d}:{total_minutes % 60:02d}"

        # 시간 차이 및 피크 시간 계산
        time_diff = (
            self.pax_df[f"{process}_done_pred"] - self.pax_df[f"{process}_on_pred"]
        )
        peak_time = self.pax_df.loc[time_diff.idxmax(), f"{process}_on_pred"]

        # 백분위수 설정 - 상위 백분위수로 변환 (입력이 1%면 99%를 계산)
        percentiles = [0.25, 0.5, 0.75]
        if percentile is not None:
            if not 0 <= percentile <= 100:
                raise ValueError("percentile은 0에서 100 사이의 값이어야 합니다")

            if percentile == 0:  # 0인 경우 최댓값 반환을 의미함
                # max 값은 이미 기본 통계에 포함되므로 추가 백분위수 필요 없음
                pass
            else:
                # 상위 백분위수로 변환 (예: 5% -> 95%)
                inverted_percentile = (100 - percentile) / 100
                percentiles.append(inverted_percentile)

        # 통계 계산 및 데이터 정리
        stats = (
            self.pax_df.groupby(f"{process}_pred")
            .apply(
                lambda x: (x[f"{process}_done_pred"] - x[f"{process}_on_pred"])
                .sort_values(ascending=False)
                .describe(percentiles=percentiles)
            )
            .dropna()
            .reset_index()
            .rename(columns={f"{process}_pred": "facility_id"})
        )

        # 시간 형식 변환
        time_columns = ["mean", "std", "min", "25%", "50%", "75%", "max"]
        if percentile is not None:
            if percentile == 0:
                # 0%는 최댓값을 의미함
                stats["0%"] = stats["max"]
                time_columns.insert(-1, "0%")
            else:
                # 원래 표기법 유지를 위해 컬럼 이름은 사용자가 입력한 값 그대로 사용
                # 예: 5%로 입력했지만 실제로는 95% 값을 계산
                inv_col = f"{(100 - percentile)}%"
                if inv_col in stats.columns:
                    stats[f"{percentile}%"] = stats[inv_col]
                    stats = stats.drop(columns=[inv_col])
                    time_columns.insert(-1, f"{percentile}%")

        # 시간 변환 적용
        for col in time_columns:
            if col in stats.columns:
                stats[col] = stats[col].apply(to_hhmm)

        # 피크 시간 추가
        stats["peak_time"] = peak_time.strftime("%H:%M")
        return stats

    def _analyze_all_processes(self, percentile: int = 5) -> pd.DataFrame:
        process_cols = [
            col.replace("_on_pred", "")
            for col in self.pax_df.columns
            if "on_pred" in col
        ]
        results = pd.concat(
            [
                self._analyze_time(process, percentile=percentile)
                for process in process_cols
            ],
            ignore_index=True,
        )
        return results

    # Details
    def get_facility_details(self):
        """시설별 세부 데이터를 계산하고 반환합니다."""
        # 빈 데이터 구조 초기화
        result = {"details": []}

        # 프로세스 컬럼 추출
        process_cols = [
            col.replace("_on_pred", "")
            for col in self.pax_df.columns
            if "on_pred" in col
        ]

        # 각 프로세스별로 처리
        for process in process_cols:
            # 필요한 컬럼만 선택하여 데이터 처리 효율성 향상
            cols_needed = [
                f"{process}_pred",
                f"{process}_que",
                f"{process}_pt",
                f"{process}_on_pred",
                f"{process}_pt_pred",
            ]

            process_df = self.pax_df[cols_needed].copy()

            # 대기 시간 미리 계산 (한 번만 계산)
            process_df["wait_diff"] = (
                process_df[f"{process}_pt_pred"] - process_df[f"{process}_on_pred"]
            )

            # 전체 개요 계산
            overview = self._calculate_overview_metrics(process_df, process)

            # 카테고리 객체 초기화
            category_obj = {"category": process, "overview": overview, "components": []}

            # 시설별 컴포넌트 추가
            self._add_facility_components(process_df, process, category_obj)

            # result에 카테고리 추가
            result["details"].append(category_obj)

        return result

    def _calculate_overview_metrics(self, df, process):
        """프로세스 전체 개요 지표를 계산합니다."""
        opened_count = df[f"{process}_pred"].nunique()

        return {
            "opened": [opened_count, opened_count],
            "isOpened": opened_count > 0,
            "throughput": len(df),
            "maxQueue": int(
                df[f"{process}_que"].max() if not df[f"{process}_que"].empty else 0
            ),
            "queueLength": int(
                df[f"{process}_que"].mean() if not df[f"{process}_que"].empty else 0
            ),
            "procTime": self._format_seconds_to_time(
                df[f"{process}_pt"].quantile(0.95)
                if not df[f"{process}_pt"].empty
                else 0
            ),
            "waitTime": self._format_timedelta(
                df["wait_diff"].quantile(0.95)
                if not df["wait_diff"].empty
                else pd.Timedelta(0)
            ),
        }

    def _add_facility_components(self, df, process, category_obj):
        """각 시설별 컴포넌트를 계산하고 category_obj에 추가합니다."""
        facilities = df[f"{process}_pred"].unique()

        for facility in facilities:
            facility_df = df[df[f"{process}_pred"] == facility]

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
                    facility_df[f"{process}_que"].mean()
                    if not facility_df[f"{process}_que"].empty
                    else 0
                ),
                "procTime": self._format_seconds_to_time(
                    facility_df[f"{process}_pt"].quantile(0.95)
                    if not facility_df[f"{process}_pt"].empty
                    else 0
                ),
                "waitTime": self._format_timedelta(
                    facility_df["wait_diff"].quantile(0.95)
                    if not facility_df["wait_diff"].empty
                    else pd.Timedelta(0)
                ),
            }

            category_obj["components"].append(component)

    def _format_seconds_to_time(self, seconds):
        """초 단위 값을 HH:MM:SS 형식으로 변환합니다."""
        if pd.isna(seconds):
            return "00:00:00"
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _format_timedelta(self, td):
        """Timedelta를 HH:MM:SS 형식으로 변환합니다."""
        if pd.isna(td):
            return "00:00:00"
        try:
            return str(td).split()[-1].split(".")[0]
        except:
            return "00:00:00"

    # Flow Chart
    def get_flow_chart_data(self):
        """
        시간별 대기열 및 대기 시간 데이터를 프론트엔드용 형식으로 반환합니다.

        Args:
            time_unit: 시간 간격 (기본값: "10min")

        Returns:
            프론트엔드에서 사용할 수 있는 데이터 구조
        """
        # 시간별 데이터프레임 생성
        time_df = self._create_time_dataframe()

        # 대기열 및 대기 시간 데이터 추가
        time_df = self._add_queue_data(time_df)

        # 프론트엔드 형식으로 변환
        return self._convert_to_frontend_format(time_df)

    def _create_time_dataframe(self):
        """
        시간별 데이터프레임 생성 함수

        Args:
            time_unit: 시간 간격 (기본값: "10min")

        Returns:
            시간 인덱스가 있는 빈 데이터프레임
        """
        # 고유 날짜 추출
        days = self.pax_df["show_up_time"].dt.date.unique()

        # 날짜별 시간범위 생성
        time_ranges = [
            pd.date_range(
                start=f"{date} 00:00:00", end=f"{date} 23:50:00", freq=self.time_unit
            )
            for date in days
        ]

        # 모든 시간대를 하나의 리스트로 통합
        all_times = pd.DatetimeIndex(
            [dt for time_range in time_ranges for dt in time_range]
        )

        # 빈 DataFrame 생성 및 정렬
        time_df = pd.DataFrame(index=all_times).sort_index()
        # time_df.index.name = "timestamp"

        return time_df

    def _add_queue_data(self, time_df):
        """
        대기열 및 대기 시간 데이터를 데이터프레임에 추가하는 함수

        Args:
            time_df: 시간 인덱스가 있는 데이터프레임
            time_unit: 시간 간격 (기본값: "10min")

        Returns:
            대기열 및 대기 시간 데이터가 추가된 데이터프레임
        """
        # 임시 데이터프레임 생성
        temp = self.pax_df.copy()

        # 프로세스 컬럼 추출
        process_cols = [
            col.replace("_on_pred", "") for col in temp.columns if "on_pred" in col
        ]

        # 각 프로세스에 대한 데이터 추가
        for process in process_cols:
            # 큐 데이터를 시간 단위로 그룹화하고 평균 계산
            queue = temp.groupby(temp[f"{process}_on_pred"].dt.floor(self.time_unit))[
                f"{process}_que"
            ].mean()

            # 대기열 데이터 추가
            time_df[f"{process}_que"] = round(queue, 2)
            time_df[f"{process}_que"] = time_df[f"{process}_que"].fillna(0)

            # 대기 시간 계산
            temp[f"{process}_wait_time"] = (
                temp[f"{process}_pt_pred"] - temp[f"{process}_on_pred"]
            )

            # 대기 시간을 시간 단위로 그룹화하고 평균 계산
            wait_time = temp.groupby(
                temp[f"{process}_on_pred"].dt.floor(self.time_unit)
            )[f"{process}_wait_time"].mean()

            # 대기 시간을 분 단위로 변환 (소수점 1자리)
            wait_time_in_minutes = wait_time.apply(lambda td: td.total_seconds() / 60)

            # 대기 시간 데이터 추가
            time_df[f"{process}_wait_time"] = wait_time_in_minutes.round(1)
            time_df[f"{process}_wait_time"] = time_df[f"{process}_wait_time"].fillna(
                0.0
            )

            # 처리량(throughput) 계산
            # 각 시간 간격 내에 처리를 완료한 승객 수 계산
            done_counts = (
                temp[f"{process}_done_pred"]
                .dropna()
                .dt.floor(self.time_unit)
                .value_counts()
                .sort_index()
            )

            # 시간대별 처리 완료 승객 수를 데이터프레임에 추가
            time_df[f"{process}_throughput"] = 0  # 기본값 0으로 초기화

            # 각 시간대에 처리 완료된 승객 수 추가
            for time_idx, count in done_counts.items():
                if time_idx in time_df.index:
                    time_df.loc[time_idx, f"{process}_throughput"] = count

        return time_df

    def _convert_to_frontend_format(self, df):
        """
        데이터프레임을 프론트엔드 형식으로 변환하는 함수

        Args:
            df: 대기열 및 대기 시간 데이터가 있는 데이터프레임

        Returns:
            프론트엔드에서 사용할 수 있는 데이터 구조
        """
        # 시간 리스트 생성
        times = df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()

        # 컴포넌트 리스트 생성
        components = []

        # 모든 열에 대해 컴포넌트 생성
        for column in df.columns:
            # 값 추출 및 타입 변환
            if df[column].dtype.kind in "ifc":  # 숫자 데이터일 경우
                values = df[column].astype(float).tolist()
            else:
                values = df[column].tolist()

            # 컴포넌트 생성
            component = {"column": column, "values": values}
            components.append(component)

        # 최종 구조 생성
        result = {"flow_chart": {"x_values": times, "y_values": components}}

        return result

    def get_sankey_diagram_data(self):
        """시설 이용 흐름을 분석하여 Sankey 다이어그램 데이터를 생성합니다."""

        # 예측 컬럼 필터링
        target_columns = [
            col
            for col in self.pax_df.columns
            if col.endswith("_pred") and not any(x in col for x in ["on", "done", "pt"])
        ]

        # 시설 이용 흐름 계산
        flow_df = self.pax_df.groupby(target_columns).size().reset_index(name="count")

        # 각 컬럼의 고유 값에 대한 인덱스 매핑 생성
        unique_values = {}
        current_index = 0
        for col in target_columns:
            unique_values[col] = {
                val: i + current_index for i, val in enumerate(flow_df[col].unique())
            }
            current_index += len(flow_df[col].unique())

        # Sankey 다이어그램 링크 데이터 생성
        sources, targets, values = [], [], []
        for i in range(len(target_columns) - 1):
            col1, col2 = target_columns[i], target_columns[i + 1]
            grouped = flow_df.groupby([col1, col2])["count"].sum().reset_index()

            for _, row in grouped.iterrows():
                sources.append(unique_values[col1][row[col1]])
                targets.append(unique_values[col2][row[col2]])
                values.append(int(row["count"]))

        # 라벨 생성
        labels = []
        for col in target_columns:
            labels.extend(list(flow_df[col].unique()))

        return {
            "label": labels,
            "link": {"source": sources, "target": targets, "value": values},
        }

    def _calculate_wait_time_distribution(self, process):
        """대기 시간 분포를 계산하여 JSON 형태로 반환"""
        bins = [0, 15, 30, 45, 60, float("inf")]
        labels = ["00:00-15:00", "15:00-30:00", "30:00-45:00", "45:00-60:00", "60:00-"]

        time_diff = (
            self.pax_df[f"{process}_pt_pred"] - self.pax_df[f"{process}_on_pred"]
        ).dt.total_seconds() / 60

        time_groups = pd.cut(time_diff, bins=bins, labels=labels, right=False)
        percentages = (time_groups.value_counts(normalize=True) * 100).round(1)

        return [
            {"title": label, "value": f"{percentages[label]:.0f}%"} for label in labels
        ]

    def _calculate_queue_length_distribution(self, process):
        """대기열 길이 분포를 계산하여 JSON 형태로 반환"""
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
        all_wait_time = {}
        all_queue_length = {}

        for facility in histogram_data:
            for wt in facility["data"]["wait_time"]:
                value = float(wt["value"].replace("%", ""))
                all_wait_time.setdefault(wt["title"], []).append(value)

            for ql in facility["data"]["queue_length"]:
                value = float(ql["value"].replace("%", ""))
                all_queue_length.setdefault(ql["title"], []).append(value)

        avg_wait_time = [
            {"title": title, "value": f"{sum(values)/len(values):.0f}%"}
            for title, values in all_wait_time.items()
        ]

        avg_queue_length = [
            {"title": title, "value": f"{sum(values)/len(values):.0f}%"}
            for title, values in all_queue_length.items()
        ]

        return {"wait_time": avg_wait_time, "queue_length": avg_queue_length}

    def _get_process_list(self):
        """프로세스 목록을 추출"""
        return [
            col.replace("_on_pred", "")
            for col in self.pax_df.columns
            if "on_pred" in col
        ]

    def get_histogram_data(self):
        """시설별 통계 데이터 생성"""
        process_list = self._get_process_list()

        # 개별 시설 데이터 먼저 생성
        facility_data = [
            {
                "label": process,
                "data": {
                    "wait_time": self._calculate_wait_time_distribution(process),
                    "queue_length": self._calculate_queue_length_distribution(process),
                },
            }
            for process in process_list
        ]

        # All Facility 데이터 계산
        all_facility_data = self._calculate_average_distribution(facility_data)

        # 결과에서 All Facility를 첫 번째로 위치시킴
        result = {
            "histogram": [{"label": "All Facility", "data": all_facility_data}]
            + facility_data
        }

        return result

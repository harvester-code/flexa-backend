import pandas as pd
import numpy as np


class HomeCalculator:
    def __init__(
        self, pax_df: pd.DataFrame, calculate_type: str, percentile: int | None = None
    ):
        self.pax_df = pax_df
        self.calculate_type = calculate_type
        self.percentile = percentile

    def get_time_range(self):
        start_time = pd.to_datetime(self.pax_df["show_up_time"].min()).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = pd.to_datetime(self.pax_df["show_up_time"].max()).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return {
            "time_range": {"start": start_time, "end": end_time},
        }

    def get_flight_summary(self):
        departure_flights = self.pax_df["flight_number"].nunique()
        arrival_flights = 0
        delayed_flights = self.pax_df[self.pax_df["gate_departure_delay"] > 15][
            "flight_number"
        ].nunique()
        returned_flights = int(self.pax_df["is_cancelled"].sum())

        return {
            "departure_flights": departure_flights,
            "arrival_flights": arrival_flights,
            "delayed_flights": delayed_flights,
            "returned_flights": returned_flights,
        }

    def get_pax_summary(self):
        return {
            "departure_pax": len(self.pax_df),
            "arrival_pax": 0,
            "transfer_pax": 0,
        }

    def get_kpi(self):
        return {
            "pax_throughout": int(self.pax_df["passport_pt_pred"].notna().sum()),
            "pax_waiting_time": self._calculate_pt_average(),
            "pax_queue_length": 0,
            "facility_utilization": 0,
        }

    def _calculate_pt_average(
        self,
    ):
        columns = [col for col in self.pax_df.columns if col.endswith("_pt")]

        # nan 값을 제외하고 행별 평균 계산
        row_means = self.pax_df[columns].mean(axis=1, skipna=True)
        # nan이 아닌 값들만 선택
        row_means = row_means.dropna()

        if len(row_means) == 0:
            return np.nan

        if self.percentile is None:
            # 전체 평균 반환
            return row_means.mean()
        else:
            # 상위 백분위수 계산
            threshold = np.percentile(row_means, 100 - self.percentile)
            # 상위 값들만 필터링하여 평균 계산
            top_means = row_means[row_means >= threshold]
            return top_means.mean()

    def get_facility_times_with_peak(self) -> dict:
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

    def _analyze_all_processes(self, percentile: int) -> pd.DataFrame:
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

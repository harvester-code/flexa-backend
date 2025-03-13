import pandas as pd
import numpy as np


class HomeCalculator:
    def __init__(self):
        pass

    @staticmethod
    def get_time_range(df: pd.DataFrame) -> dict:
        start_time = pd.to_datetime(df["show_up_time"].min()).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = pd.to_datetime(df["show_up_time"].max()).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return {"time_range": {"start": start_time, "end": end_time}}

    @staticmethod
    def get_time_range(df: pd.DataFrame) -> dict:
        start_time = pd.to_datetime(df["show_up_time"].min()).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        end_time = pd.to_datetime(df["show_up_time"].max()).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        return {
            "time_range": {"start": start_time, "end": end_time},
        }

    @staticmethod
    def get_flight_summary(df: pd.DataFrame) -> dict:
        departure_flights = df["flight_number"].nunique()
        arrival_flights = 0
        delayed_flights = df[df["gate_departure_delay"] > 15]["flight_number"].nunique()
        returned_flights = int(df["is_cancelled"].sum())

        return {
            "departure_flights": departure_flights,
            "arrival_flights": arrival_flights,
            "delayed_flights": delayed_flights,
            "returned_flights": returned_flights,
        }

    @staticmethod
    def get_pax_summary(df: pd.DataFrame) -> dict:
        return {
            "departure_pax": len(df),
            "arrival_pax": 0,
            "transfer_pax": 0,
        }

    @staticmethod
    def get_kpi(df: pd.DataFrame) -> dict:
        return {
            "pax_throughout": int(df["passport_pt_pred"].notna().sum()),
            "pax_waiting_time": HomeCalculator.calculate_pt_average(df),
            "pax_queue_length": 0,
            "facility_utilization": 0,
        }

    @staticmethod
    def calculate_pt_average(
        df: pd.DataFrame,
        percentile: int | None = None,
    ):
        columns = [col for col in df.columns if col.endswith("_pt")]

        # nan 값을 제외하고 행별 평균 계산
        row_means = df[columns].mean(axis=1, skipna=True)
        # nan이 아닌 값들만 선택
        row_means = row_means.dropna()

        if len(row_means) == 0:
            return np.nan

        if percentile is None:
            # 전체 평균 반환
            return row_means.mean()
        else:
            # 상위 백분위수 계산
            threshold = np.percentile(row_means, 100 - percentile)
            # 상위 값들만 필터링하여 평균 계산
            top_means = row_means[row_means >= threshold]
            return top_means.mean()

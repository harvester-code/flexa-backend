"""
승객 스케줄 생성 핵심 로직 (Passenger Schedule Generation Core Logic)

이 모듈은 복잡한 승객 스케줄 생성 로직을 담고 있습니다:
- 항공편 데이터를 개별 승객으로 확장
- 인구통계 정보 할당 (국적, 프로필 등)
- 승객별 공항 도착시간 생성
- S3에 승객 데이터 저장
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from fastapi import HTTPException, status
from loguru import logger

import awswrangler as wr
from packages.secrets import get_secret
from packages.storages import boto3_session, check_s3_object_exists


class PassengerGenerator:
    """승객 스케줄 생성 핵심 로직"""

    async def generate(self, scenario_id: str, config: dict) -> Dict:
        """승객 스케줄 생성 메인 메서드"""
        try:
            # 1. 설정값 추출 (기본값 없이 강제 입력)
            settings = config.get("settings", {})

            # 필수 입력값 검증
            if "load_factor" not in settings:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="load_factor is required in settings",
                )
            if "target_date" not in settings:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="target_date is required in settings",
                )
            if "departure_airport" not in settings:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="departure_airport is required in settings",
                )

            load_factor = settings["load_factor"]
            target_date = settings["target_date"]
            departure_airport = settings["departure_airport"]

            # 2. S3에서 flight-schedule 데이터 로드 (필터링 설정 전달)
            flight_data = await self._load_flight_data_from_s3(
                scenario_id, target_date, departure_airport
            )
            if not flight_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Flight schedule data not found. Please load flight schedule first.",
                )

            # 3. 승객 데이터 생성
            flight_df = pd.DataFrame(flight_data)

            # 4. 승객 확장
            pax_df = await self._expand_flights_to_passengers(flight_df, load_factor)

            # 5. 인구통계 할당
            pax_df = await self._assign_passenger_demographics(pax_df, config)

            # 6. 도착시간 생성
            pax_df = await self._assign_show_up_times(pax_df, config)

            # 7. S3에 저장
            await self._save_passenger_data_to_s3(pax_df, scenario_id)

            # 8. 응답 데이터 생성 및 반환
            return await self._build_passenger_schedule_response(pax_df, config)

        except HTTPException:
            # HTTPException은 그대로 재발생
            raise
        except Exception as e:
            logger.error(f"Passenger schedule generation failed: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate passenger schedule: {str(e)}",
            )

    # =====================================
    # Private Helper Methods
    # =====================================

    async def _load_flight_data_from_s3(
        self, scenario_id: str, target_date: str, departure_airport: str
    ) -> Optional[List[Dict]]:
        """S3에서 항공편 데이터 로드"""
        try:
            object_exists = check_s3_object_exists(
                bucket_name=get_secret("AWS_S3_BUCKET_NAME"),
                object_key=f"{scenario_id}/flight-schedule.parquet",
            )

            if not object_exists:
                return None

            df = wr.s3.read_parquet(
                path=f"s3://{get_secret('AWS_S3_BUCKET_NAME')}/{scenario_id}/flight-schedule.parquet",
                boto3_session=boto3_session,
            )

            logger.info(f"원본 데이터: {len(df):,}개")

            # 1. 날짜 필터링 (동적 target_date 기준)
            if "flight_date" in df.columns:
                # flight_date가 문자열인 경우 datetime으로 변환
                if df["flight_date"].dtype == "object":
                    df["flight_date"] = pd.to_datetime(df["flight_date"])

                # 동적 target_date로 필터링
                target_dt = pd.to_datetime(target_date)
                df = df[df["flight_date"].dt.date == target_dt.date()]
                logger.info(f"날짜 필터링 ({target_date}): {len(df):,}개")
            else:
                logger.warning("flight_date 컬럼이 없어 날짜 필터링 생략")

            # 2. 출발공항 필터링 (동적 departure_airport 기준)
            if "departure_airport_iata" in df.columns:
                df = df[df["departure_airport_iata"] == departure_airport]
                logger.info(f"출발공항 필터링 ({departure_airport}): {len(df):,}개")
            else:
                logger.warning(
                    "departure_airport_iata 컬럼이 없어 출발공항 필터링 생략"
                )

            # 3. 좌석수 필터링 (total_seats > 0인 여객기만)
            if "total_seats" in df.columns:
                df = df[(df["total_seats"] > 0) & (df["total_seats"].notna())]
                logger.info(f"여객기 필터링 (total_seats > 0): {len(df):,}개")
            else:
                logger.warning("total_seats 컬럼이 없어 좌석수 필터링 생략")

            # 4. 시간 정보 완성성 필터링 (모든 시간 컬럼이 존재해야 함)
            datetime_cols = [
                "scheduled_departure_local",
                "scheduled_departure_utc",
                "scheduled_arrival_local",
                "scheduled_arrival_utc",
            ]

            existing_datetime_cols = [col for col in datetime_cols if col in df.columns]
            if existing_datetime_cols:
                # 모든 datetime 컬럼이 null이 아닌 행만 유지
                for col in existing_datetime_cols:
                    df = df[df[col].notna()]
                logger.info(
                    f"시간정보 완성 필터링 ({len(existing_datetime_cols)}개 컬럼): {len(df):,}개"
                )
            else:
                logger.warning("시간 컬럼이 없어 시간정보 필터링 생략")

            return df.to_dict("records")
        except Exception as e:
            logger.error(f"Failed to load flight data from S3: {str(e)}")
            return None

    async def _expand_flights_to_passengers(
        self, flight_df: pd.DataFrame, load_factor: float
    ) -> pd.DataFrame:
        """항공편을 승객 수만큼 확장"""
        pax_rows = []

        for _, flight_row in flight_df.iterrows():
            pax_count = int(flight_row["total_seats"] * load_factor)

            if pax_count <= 0:
                continue

            for i in range(pax_count):
                pax_row = flight_row.copy()
                pax_rows.append(pax_row)

        result_df = pd.DataFrame(pax_rows)
        logger.info(f"Expanded flights to {len(result_df):,} passenger rows")
        return result_df

    async def _assign_passenger_demographics(
        self, pax_df: pd.DataFrame, config: Dict
    ) -> pd.DataFrame:
        """승객 인구통계 할당"""
        pax_demographics = config.get("pax_demographics", {})

        for distribution_type in pax_demographics.keys():
            column_name = distribution_type.replace("_distribution", "")
            logger.info(f"Assigning {column_name} demographics...")

            pax_df[column_name] = pax_df.apply(
                lambda row: self._assign_demographic_value(
                    row,
                    distribution_type,
                    distribution_config=pax_demographics[distribution_type],
                ),
                axis=1,
            )

        return pax_df

    def _assign_demographic_value(
        self, pax_row: pd.Series, distribution_type: str, distribution_config: Dict
    ) -> str:
        """개별 승객의 인구통계 값 할당"""
        rules = distribution_config.get("rules", [])

        # 조건 확인
        for rule in rules:
            conditions = rule.get("conditions", {})
            if self._check_conditions(pax_row, conditions):
                # 분포에 따라 값 선택
                distribution = rule.get("distribution", {})
                if distribution:
                    values = list(distribution.keys())
                    probs = list(distribution.values())
                    return np.random.choice(values, p=probs)

        # 기본값 처리
        default = distribution_config.get("default", {})
        if default:
            values = list(default.keys())
            probs = list(default.values())
            return np.random.choice(values, p=probs)

        # 빈 default인 경우 첫 번째 규칙의 분포를 사용
        if rules:
            first_rule = rules[0]
            distribution = first_rule.get("distribution", {})
            if distribution:
                values = list(distribution.keys())
                probs = list(distribution.values())
                return np.random.choice(values, p=probs)

        return "Unknown"

    async def _assign_show_up_times(
        self, pax_df: pd.DataFrame, config: Dict
    ) -> pd.DataFrame:
        """승객별 공항 도착시간 할당"""
        pax_df["show_up_time"] = pax_df.apply(
            lambda row: self._generate_show_up_time(row, config), axis=1
        )

        return pax_df

    def _generate_show_up_time(self, pax_row: pd.Series, config: Dict) -> datetime:
        """개별 승객의 공항 도착시간 생성"""
        rule = self._match_arrival_rule(pax_row, config)

        if rule:
            mean = rule["mean"]
            std = rule["std"]
        else:
            default = config["pax_arrival_patterns"]["default"]
            mean = default["mean"]
            std = default["std"]

        # 정규분포에서 도착시간 생성
        minutes_before = np.random.normal(mean, std)

        # min_arrival_minutes 필수 입력값 검증
        min_minutes = config.get("settings", {}).get("min_arrival_minutes")
        if min_minutes is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="min_arrival_minutes is required in settings",
            )
        minutes_before = max(minutes_before, min_minutes)

        departure_time = pd.to_datetime(pax_row["scheduled_departure_local"])
        show_up_time = departure_time - timedelta(minutes=minutes_before)

        return show_up_time.replace(microsecond=0)

    async def _save_passenger_data_to_s3(self, pax_df: pd.DataFrame, scenario_id: str):
        """승객 데이터를 S3에 저장"""
        try:
            wr.s3.to_parquet(
                df=pax_df,
                path=f"s3://{get_secret('AWS_S3_BUCKET_NAME')}/{scenario_id}/show-up-passenger.parquet",
                boto3_session=boto3_session,
            )
            logger.info(f"Saved {len(pax_df):,} passenger records to S3")
        except Exception as e:
            logger.error(f"Failed to save passenger data to S3: {str(e)}")
            raise

    async def _build_passenger_schedule_response(
        self, pax_df: pd.DataFrame, config: Dict
    ) -> Dict:
        """승객 스케줄 응답 데이터 구성 - 원본과 동일한 형태"""

        # Summary 데이터 생성 (원본과 동일)
        if len(pax_df) > 0:
            # 항공편 정보에서 통계 계산
            unique_flights = pax_df.drop_duplicates(
                subset=["flight_number", "flight_date"]
            )
            average_seats = (
                unique_flights["total_seats"].mean() if len(unique_flights) > 0 else 0
            )
            total_flights = len(unique_flights)
        else:
            average_seats = 0
            total_flights = 0

        summary = {
            "flights": total_flights,
            "avg_seats": round(average_seats, 2),
            "load_factor": int(config["settings"]["load_factor"] * 100),  # 85 형태로
        }

        # 차트 데이터 생성 (원본 로직과 동일)
        chart_result = {}
        chart_x_data = []

        if len(pax_df) > 0:
            # 주요 그룹 컬럼들 (원본과 동일)
            group_columns = [
                "operating_carrier_name",
                "departure_terminal",
                "flight_type",
                "arrival_country_code",
                "arrival_region",
            ]
            group_labels = ["airline", "terminal", "type", "country", "region"]

            for i, group_column in enumerate(group_columns):
                if group_column in pax_df.columns:
                    chart_data = await self._create_show_up_summary(
                        pax_df, group_column
                    )
                    if chart_data:
                        chart_result[group_labels[i]] = chart_data["traces"]
                        chart_x_data = chart_data["default_x"]

        return {
            "total": len(pax_df),
            "summary": summary,
            "bar_chart_x_data": chart_x_data,
            "bar_chart_y_data": chart_result,
            "generation_config": {
                "load_factor": config["settings"]["load_factor"],
                "target_date": config["settings"]["target_date"],
                "departure_airport": config["settings"]["departure_airport"],
                "min_arrival_minutes": config["settings"]["min_arrival_minutes"],
                "generated_at": datetime.now().isoformat(),
            },
        }

    # =====================================
    # Helper Utility Methods
    # =====================================

    def _check_conditions(self, pax_row: pd.Series, conditions: Dict) -> bool:
        """승객 행이 주어진 조건들을 만족하는지 확인 - 복잡한 조건 지원"""
        for key, values in conditions.items():
            if key == "total_seats":
                # 좌석수 범위 조건 처리 (여러 범위 지원)
                if "total_seats" in pax_row:
                    seat_count = pax_row["total_seats"]
                    if isinstance(values, list):
                        # 여러 범위 중 하나라도 매치되면 통과
                        range_match = False
                        for range_condition in values:
                            if isinstance(range_condition, dict):
                                min_val = range_condition.get("min", 0)
                                max_val = range_condition.get("max", float("inf"))
                                if min_val <= seat_count <= max_val:
                                    range_match = True
                                    break
                            else:
                                # 단순 값인 경우 (기존 호환성)
                                if seat_count == range_condition:
                                    range_match = True
                                    break
                        if not range_match:
                            return False
                    elif isinstance(values, dict) and (
                        "min" in values or "max" in values
                    ):
                        # 단일 범위 조건
                        min_val = values.get("min", 0)
                        max_val = values.get("max", float("inf"))
                        if not (min_val <= seat_count <= max_val):
                            return False
                else:
                    continue

            elif key == "scheduled_departure_local_hour":
                # 출발시간 local hour 조건 처리 - departure_hour 컬럼 우선 사용
                if "departure_hour" in pax_row:
                    hour = pax_row["departure_hour"]
                    if hour not in values:
                        return False
                elif "scheduled_departure_local" in pax_row:
                    departure_time = pd.to_datetime(
                        pax_row["scheduled_departure_local"]
                    )
                    hour = departure_time.hour
                    if hour not in values:
                        return False
                else:
                    continue

            elif key == "scheduled_departure_utc_hour":
                # 출발시간 UTC hour 조건 처리
                if "scheduled_departure_utc" in pax_row:
                    departure_time = pd.to_datetime(pax_row["scheduled_departure_utc"])
                    hour = departure_time.hour
                    if hour not in values:
                        return False
                else:
                    continue

            elif key == "arrival_country":
                # arrival_country 조건을 destination_country와 매칭
                if "destination_country" in pax_row:
                    if isinstance(values, list):
                        if pax_row["destination_country"] not in values:
                            return False
                    else:
                        if pax_row["destination_country"] != values:
                            return False
                else:
                    continue

            elif key == "route":
                # route는 중첩 구조 처리 (기존 호환성)
                route_match = False
                for route_key, route_values in values.items():
                    if route_key in pax_row and pax_row[route_key] in route_values:
                        route_match = True
                        break
                if not route_match:
                    return False

            else:
                # 일반 조건 처리
                if key in pax_row:
                    if isinstance(values, list):
                        if pax_row[key] not in values:
                            return False
                    elif isinstance(values, dict):
                        # 범위 조건 처리 (다른 숫자 컬럼용)
                        if "min" in values and "max" in values:
                            if not (values["min"] <= pax_row[key] <= values["max"]):
                                return False
                    else:
                        if pax_row[key] != values:
                            return False
                else:
                    # 컬럼이 없으면 해당 조건은 스킵
                    continue

        return True

    def _match_arrival_rule(self, pax_row: pd.Series, config: Dict) -> Optional[Dict]:
        """승객에 맞는 도착 패턴 규칙을 찾음"""
        arrival_patterns = config.get("pax_arrival_patterns", {})
        rules = arrival_patterns.get("rules", [])

        for rule in rules:
            conditions = rule.get("conditions", {})
            if self._check_conditions(pax_row, conditions):
                return {"mean": rule.get("mean"), "std": rule.get("std")}

        return None

    async def _create_show_up_summary(self, pax_df: pd.DataFrame, group_column: str):
        """실제 데이터가 있는 시간 범위만 표시하도록 개선된 차트 데이터 생성"""
        time_unit = "10min"
        pax_df_copy = pax_df.copy()
        pax_df_copy["show_up_time"] = pax_df_copy["show_up_time"].dt.floor(time_unit)

        df_grouped = (
            pax_df_copy.groupby(["show_up_time", group_column])
            .size()
            .unstack(fill_value=0)
        )
        df_grouped = df_grouped.sort_index()

        if df_grouped.empty:
            return {"traces": [], "default_x": []}

        # 🔥 핵심 수정: 실제 승객이 있는 시간 범위만 계산
        # 첫 번째와 마지막 승객이 있는 시간을 찾음
        row_sums = df_grouped.sum(axis=1)
        non_zero_indices = row_sums[row_sums > 0].index

        if len(non_zero_indices) == 0:
            return {"traces": [], "default_x": []}

        # 실제 데이터가 있는 시간 범위로 필터링
        start_time = non_zero_indices.min()
        end_time = non_zero_indices.max()
        df_grouped = df_grouped.loc[start_time:end_time]

        total_groups = df_grouped.shape[1]
        has_etc = total_groups > 9

        if has_etc:
            top_9_columns = df_grouped.sum().nlargest(9).index.tolist()
            df_grouped["etc"] = df_grouped.drop(
                columns=top_9_columns, errors="ignore"
            ).sum(axis=1)
            df_grouped = df_grouped[top_9_columns + ["etc"]]
        else:
            top_9_columns = df_grouped.columns.tolist()

        group_order = df_grouped.sum().sort_values(ascending=False).index.tolist()
        if has_etc and "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

        default_x = df_grouped.index.strftime("%H:%M").tolist()
        traces = [
            {
                "name": column,
                "order": group_order.index(column),
                "y": df_grouped[column].tolist(),
            }
            for column in df_grouped.columns
        ]

        return {"traces": traces, "default_x": default_x}

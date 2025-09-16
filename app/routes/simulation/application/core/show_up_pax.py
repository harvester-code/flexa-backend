"""
승객 스케줄 처리 통합 모듈 (Show-up Passenger Processing)

이 모듈은 승객 스케줄 처리의 Storage와 Response 기능을 통합합니다:
- ShowUpPassengerStorage: 승객 스케줄 생성, 인구통계 할당, S3 저장
- ShowUpPassengerResponse: 프론트엔드용 JSON 응답 생성 (차트 데이터 포함)
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from fastapi import HTTPException, status
from loguru import logger

from packages.aws.s3.s3_manager import S3Manager


class ShowUpPassengerStorage:
    """승객 스케줄 데이터 저장 전담 클래스"""

    def __init__(self):
        self.s3_manager = S3Manager()

    async def generate_and_store(self, scenario_id: str, config: dict) -> pd.DataFrame:
        """승객 스케줄 생성 및 저장"""
        try:
            # 1. 설정값 추출 및 검증
            settings = config.get("settings", {})
            self._validate_settings(settings)
            
            # 2. nationality와 profile 분포값 검증
            self._validate_demographic_distributions(config)

            date = settings["date"]
            airport = settings["airport"]

            # 3. S3에서 flight-schedule 데이터 로드
            flight_data = await self._load_flight_data_from_s3(
                scenario_id, date, airport
            )
            if not flight_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Flight schedule data not found. Please load flight schedule first.",
                )

            # 4. 승객 데이터 생성
            flight_df = pd.DataFrame(flight_data)

            # 5. 승객 확장 (조건부 load_factor 적용)
            pax_df = await self._expand_flights_to_passengers(flight_df, config)

            # 6. 인구통계 할당
            pax_df = await self._assign_passenger_demographics(pax_df, config)

            # 7. 도착시간 생성
            pax_df = await self._assign_show_up_times(pax_df, config)

            # 8. S3에 저장
            await self._save_passenger_data_to_s3(pax_df, scenario_id)

            return pax_df

        except HTTPException:
            # HTTPException은 그대로 재발생
            raise
        except Exception as e:
            logger.error(f"Passenger schedule storage failed: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate passenger schedule: {str(e)}",
            )

    def _validate_settings(self, settings: dict):
        """필수 설정값 검증"""
        required_fields = [
            "date",
            "airport",
            "min_arrival_minutes",
        ]

        for field in required_fields:
            if field not in settings:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"{field} is required in settings",
                )

    def _validate_demographic_distributions(self, config: dict):
        """pax_demographics 내 nationality와 profile 분포 검증"""
        pax_demographics = config.get("pax_demographics", {})
        
        for distribution_type in ["nationality", "profile"]:
            if distribution_type not in pax_demographics:
                continue
                
            distribution_config = pax_demographics[distribution_type]
            
            # rules 검증
            rules = distribution_config.get("rules", [])
            for rule in rules:
                value = rule.get("value", {})
                self._validate_percentage_values(value, f"pax_demographics.{distribution_type}.rules")
            
            # default 검증
            default = distribution_config.get("default", {})
            self._validate_percentage_values(default, f"pax_demographics.{distribution_type}.default")
    
    def _validate_percentage_values(self, distribution: dict, field_path: str):
        """확률 분포값 검증 - 정수값이고 합이 100인지 확인"""
        if not distribution:
            return
            
        # flightCount 제외
        filtered_dist = {k: v for k, v in distribution.items() if k != "flightCount"}
        if not filtered_dist:
            return
            
        # 모든 값이 숫자인지 확인
        for key, value in filtered_dist.items():
            if not isinstance(value, (int, float)):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"{field_path}.{key} must be a number, got {type(value).__name__}",
                )
            if value < 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"{field_path}.{key} must be non-negative, got {value}",
                )
        
        # 합이 100인지 확인 (정수 기준)
        total = sum(filtered_dist.values())
        if abs(total - 100) > 0.001:  # 부동소수점 오차 허용
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"{field_path} percentages must sum to 100, got {total}",
            )

    async def _load_flight_data_from_s3(
        self, scenario_id: str, date: str, airport: str
    ) -> Optional[List[Dict]]:
        """S3에서 항공편 데이터 로드"""
        try:
            object_exists = await self.s3_manager.check_exists_async(
                scenario_id=scenario_id,
                filename="flight-schedule.parquet"
            )

            if not object_exists:
                return None

            # S3Manager를 사용하여 parquet 파일 읽기
            df = await self.s3_manager.get_parquet_async(
                scenario_id=scenario_id,
                filename="flight-schedule.parquet"
            )

            logger.info(f"원본 데이터: {len(df):,}개")

            # 데이터 필터링
            df = self._filter_flight_data(df, date, airport)

            return df.to_dict("records")
        except Exception as e:
            logger.error(f"Failed to load flight data from S3: {str(e)}")
            return None

    def _filter_flight_data(
        self, df: pd.DataFrame, date: str, airport: str
    ) -> pd.DataFrame:
        """항공편 데이터 필터링"""
        # 1. 날짜 필터링
        if "flight_date" in df.columns:
            if df["flight_date"].dtype == "object":
                df["flight_date"] = pd.to_datetime(df["flight_date"])

            target_dt = pd.to_datetime(date)
            df = df[df["flight_date"].dt.date == target_dt.date()]
            logger.info(f"날짜 필터링 ({date}): {len(df):,}개")

        # 2. 공항 필터링 - S3 데이터는 이미 필터링되어 있으므로 스킵
        # S3의 flight-schedule.parquet은 이미 특정 공항으로 필터링된 데이터만 포함
        logger.info(f"공항 데이터 ({airport}): {len(df):,}개 (S3에서 이미 필터링됨)")

        # 3. 좌석수 필터링
        if "total_seats" in df.columns:
            df = df[(df["total_seats"] > 0) & (df["total_seats"].notna())]
            logger.info(f"여객기 필터링 (total_seats > 0): {len(df):,}개")

        # 4. 시간 정보 완성성 필터링
        datetime_cols = [
            "scheduled_departure_local",
            "scheduled_departure_utc",
            "scheduled_arrival_local",
            "scheduled_arrival_utc",
        ]

        existing_datetime_cols = [col for col in datetime_cols if col in df.columns]
        if existing_datetime_cols:
            for col in existing_datetime_cols:
                df = df[df[col].notna()]
            logger.info(f"시간정보 완성 필터링: {len(df):,}개")

        return df

    async def _expand_flights_to_passengers(
        self, flight_df: pd.DataFrame, config: dict
    ) -> pd.DataFrame:
        """항공편을 승객 수만큼 확장 - 조건부 load_factor 적용"""
        pax_rows = []

        for _, flight_row in flight_df.iterrows():
            # 각 항공편별로 조건에 맞는 load_factor 계산
            load_factor = self._get_load_factor_for_flight(flight_row, config)
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
    ):
        """
        개별 승객의 인구통계 값 할당
        
        Returns:
            str: 할당된 인구통계 값
            None: distribution이 설정되지 않은 경우 (pandas에서 NaN으로 처리됨)
        """
        rules = distribution_config.get("rules", [])

        # nationality와 profile의 경우 프론트엔드에서 정수로 보냈으므로 100으로 나눠서 확률로 변환
        should_divide_by_100 = distribution_type in ["nationality", "profile"]

        # 조건 확인
        for rule in rules:
            conditions = rule.get("conditions", {})
            if self._check_conditions(pax_row, conditions):
                distribution = rule.get("value", {})
                if distribution:
                    # flightCount 키를 제외하고 확률 계산
                    filtered_distribution = {k: v for k, v in distribution.items() if k != "flightCount"}
                    if filtered_distribution:
                        values = list(filtered_distribution.keys())
                        probs = list(filtered_distribution.values())
                        
                        # nationality와 profile의 경우 100으로 나눠서 확률로 변환
                        if should_divide_by_100:
                            probs = [p / 100.0 for p in probs]
                        
                        return np.random.choice(values, p=probs)

        # 기본값 처리
        default = distribution_config.get("default", {})
        if default:
            # flightCount 키를 제외하고 확률 계산
            filtered_default = {k: v for k, v in default.items() if k != "flightCount"}
            if filtered_default:
                values = list(filtered_default.keys())
                probs = list(filtered_default.values())
                
                # nationality와 profile의 경우 100으로 나눠서 확률로 변환
                if should_divide_by_100:
                    probs = [p / 100.0 for p in probs]
                
                return np.random.choice(values, p=probs)

        # distribution이 설정되지 않은 경우 None 반환 (pandas에서 NaN으로 처리)
        logger.debug(f"No distribution found for {distribution_type}, returning None (will be NaN in pandas)")
        return None

    async def _assign_show_up_times(
        self, pax_df: pd.DataFrame, config: Dict
    ) -> pd.DataFrame:
        """승객별 공항 도착시간 할당"""
        # 처음 몇 개 항공편의 출발 시간 확인 (디버깅용)
        sample_flights = pax_df[['flight_number', 'scheduled_departure_local']].drop_duplicates().head(5)
        logger.info(f"Sample flight departure times:\n{sample_flights}")

        pax_df["show_up_time"] = pax_df.apply(
            lambda row: self._generate_show_up_time(row, config), axis=1
        )

        # 디버깅: show_up_time 분포 확인
        logger.info(f"Show-up time range: {pax_df['show_up_time'].min()} ~ {pax_df['show_up_time'].max()}")
        logger.info(f"Unique show-up times: {pax_df['show_up_time'].nunique()}")

        # 시간대별 승객 수 확인
        pax_df_temp = pax_df.copy()
        pax_df_temp['show_up_hour'] = pax_df_temp['show_up_time'].dt.strftime('%Y-%m-%d %H:00')
        hourly_counts = pax_df_temp.groupby('show_up_hour').size().sort_index()
        logger.info(f"Hourly passenger counts (top 10):\n{hourly_counts.head(10)}")

        return pax_df

    def _generate_show_up_time(self, pax_row: pd.Series, config: Dict) -> datetime:
        """개별 승객의 공항 도착시간 생성"""
        rule = self._match_arrival_rule(pax_row, config)

        if rule:
            mean = rule["mean"]
            std = rule["std"]
        else:
            default = config.get("pax_arrival_patterns", {}).get("default", {})
            mean = default.get("mean", 120)  # 기본값 120분
            std = default.get("std", 30)  # 기본값 30분

        # 정규분포에서 도착시간 생성
        minutes_before = np.random.normal(mean, std)

        # min_arrival_minutes 설정 적용 - 최소 도착 시간 보장
        # 예: min_arrival_minutes=30이면 최소 30분 전에는 도착해야 함
        min_minutes = config.get("settings", {}).get("min_arrival_minutes", 30)

        # minutes_before가 min_minutes보다 작으면 min_minutes로 조정
        # 단, 음수는 허용하지 않음 (출발 후 도착 방지)
        if minutes_before < min_minutes:
            minutes_before = min_minutes

        departure_time = pd.to_datetime(pax_row["scheduled_departure_local"])
        show_up_time = departure_time - timedelta(minutes=minutes_before)

        return show_up_time.replace(microsecond=0)

    async def _save_passenger_data_to_s3(self, pax_df: pd.DataFrame, scenario_id: str):
        """승객 데이터를 S3에 저장"""
        try:
            # S3Manager를 사용하여 parquet 저장
            await self.s3_manager.save_parquet_async(
                scenario_id=scenario_id,
                filename="show-up-passenger.parquet",
                df=pax_df
            )
            logger.info(f"Saved {len(pax_df):,} passenger records to S3")
        except Exception as e:
            logger.error(f"Failed to save passenger data to S3: {str(e)}")
            raise

    def _get_load_factor_for_flight(self, flight_row: pd.Series, config: Dict) -> float:
        """
        항공편별 조건에 맞는 load_factor 반환
        nationality와 동일한 조건 매칭 로직 사용
        프론트엔드에서 정수(0-100)로 전송하므로 100으로 나눠서 비율로 변환
        """
        # pax_generation으로 최상위 키 변경
        load_factor_config = config.get("pax_generation", {})

        rules = load_factor_config.get("rules", [])

        # 조건 확인
        for rule in rules:
            conditions = rule.get("conditions", {})
            if self._check_conditions(flight_row, conditions):
                rule_value = rule.get("value", {}).get("load_factor")
                if rule_value is not None:
                    # 프론트엔드에서 정수(85)로 오므로 100으로 나눔
                    return rule_value / 100 if rule_value > 1 else rule_value
                raise ValueError(f"load_factor value not found in rule: {rule}")  # 설정 오류 명시

        # 기본값 반환
        default_value = load_factor_config.get("default", {}).get("load_factor")
        if default_value is not None:
            # 프론트엔드에서 정수(85)로 오므로 100으로 나눔
            return default_value / 100 if default_value > 1 else default_value
        raise ValueError("load_factor default value not found in config")

    # Helper 메서드들
    def _check_conditions(self, pax_row: pd.Series, conditions: Dict) -> bool:
        """승객 행이 주어진 조건들을 만족하는지 확인"""
        for key, values in conditions.items():
            if key == "total_seats":
                if "total_seats" in pax_row:
                    seat_count = pax_row["total_seats"]
                    if isinstance(values, list):
                        range_match = False
                        for range_condition in values:
                            if isinstance(range_condition, dict):
                                min_val = range_condition.get("min", 0)
                                max_val = range_condition.get("max", float("inf"))
                                if min_val <= seat_count <= max_val:
                                    range_match = True
                                    break
                            else:
                                if seat_count == range_condition:
                                    range_match = True
                                    break
                        if not range_match:
                            return False
                    elif isinstance(values, dict) and (
                        "min" in values or "max" in values
                    ):
                        min_val = values.get("min", 0)
                        max_val = values.get("max", float("inf"))
                        if not (min_val <= seat_count <= max_val):
                            return False
            elif key == "scheduled_departure_local_hour":
                if "scheduled_departure_local" in pax_row:
                    departure_time = pd.to_datetime(
                        pax_row["scheduled_departure_local"]
                    )
                    hour = departure_time.hour
                    if hour not in values:
                        return False
            else:
                # 일반 조건 처리
                if key in pax_row:
                    if isinstance(values, list):
                        if pax_row[key] not in values:
                            return False
                    else:
                        if pax_row[key] != values:
                            return False

        return True

    def _match_arrival_rule(self, pax_row: pd.Series, config: Dict) -> Optional[Dict]:
        """승객에 맞는 도착 패턴 규칙을 찾음"""
        arrival_patterns = config.get("pax_arrival_patterns", {})
        rules = arrival_patterns.get("rules", [])

        for rule in rules:
            conditions = rule.get("conditions", {})
            if self._check_conditions(pax_row, conditions):
                rule_value = rule.get("value", {})
                return {"mean": rule_value.get("mean"), "std": rule_value.get("std")}

        return None


class ShowUpPassengerResponse:
    """승객 스케줄 프론트엔드 응답 생성 전담 클래스"""

    async def build_response(
        self,
        pax_df: pd.DataFrame,
        config: Dict,
        airport: str = None,
        date: str = None,
        scenario_id: str = None,
    ) -> Dict:
        """승객 스케줄 응답 데이터 구성"""

        # Summary 데이터 생성
        summary = self._build_summary(pax_df, config)

        # 차트 데이터 생성
        chart_result = {}
        chart_x_data = []

        if len(pax_df) > 0:
            # flight_type 판단 (settings에서 가져오기)
            flight_type = config.get("settings", {}).get("type", "departure")

            # flight_type에 따라 컬럼 동적 설정
            if flight_type == "departure":
                # 출발편: 출발 터미널, 도착 국가/지역 사용
                group_columns = [
                    "operating_carrier_name",
                    "departure_terminal",
                    "flight_type",
                    "arrival_country",
                    "arrival_region",
                ]
            else:  # arrival
                # 도착편: 도착 터미널, 출발 국가/지역 사용
                group_columns = [
                    "operating_carrier_name",
                    "arrival_terminal",
                    "flight_type",
                    "departure_country",
                    "departure_region",
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

        # 응답 구조: 다른 API들과 일관성 맞춤 - 컨텍스트 정보 먼저
        response = {}
        
        # 요청 컨텍스트 정보 (처음 3개 키)
        if airport:
            response["airport"] = airport
        if date:
            response["date"] = date
        if scenario_id:
            response["scenario_id"] = scenario_id
            
        # 기본 응답 데이터
        response.update({
            "total": len(pax_df),
            "summary": summary,
            "chart_x_data": chart_x_data,
            "chart_y_data": chart_result,
        })

        return response

    def _build_summary(self, pax_df: pd.DataFrame, config: Dict) -> Dict:
        """Summary 데이터 생성"""
        if len(pax_df) > 0:
            # 항공편의 고유성은 carrier + flight_number + date로 결정
            # total_seats는 중복 제거 기준에서 제외
            unique_flights = pax_df[['operating_carrier_iata', 'flight_number', 'flight_date']].drop_duplicates()
            total_flights = len(unique_flights)

            # 평균 좌석 수는 각 항공편의 첫 번째 좌석 수 사용
            unique_flights_with_seats = pax_df[['operating_carrier_iata', 'flight_number', 'flight_date', 'total_seats']].drop_duplicates(
                subset=['operating_carrier_iata', 'flight_number', 'flight_date'],
                keep='first'
            )
            average_seats = unique_flights_with_seats["total_seats"].mean()
        else:
            average_seats = 0
            total_flights = 0

        # 동적 load_factor 평균값 계산
        load_factor_config = config.get("pax_generation", {})
        avg_load_factor = load_factor_config.get("default", {}).get("load_factor")
        if avg_load_factor is None:
            avg_load_factor = 0.0  # 설정이 없으면 0으로 표시

        # 프론트엔드에서 정수로 오는 경우 그대로 표시, 소수로 오는 경우 100 곱함
        display_load_factor = int(avg_load_factor) if avg_load_factor > 1 else int(avg_load_factor * 100)

        return {
            "flights": total_flights,
            "avg_seats": round(average_seats, 2),
            "load_factor": display_load_factor,  # 정수 퍼센트로 표시
            "min_arrival_minutes": config.get("settings", {}).get("min_arrival_minutes"),
        }

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

        # 실제 승객이 있는 시간 범위만 계산
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

        # 날짜와 시간을 모두 포함하여 반환 (ISO 형식)
        default_x = df_grouped.index.strftime("%Y-%m-%d %H:%M").tolist()
        traces = [
            {
                "name": column,
                "order": group_order.index(column),
                "y": df_grouped[column].tolist(),
            }
            for column in df_grouped.columns
        ]

        return {"traces": traces, "default_x": default_x}


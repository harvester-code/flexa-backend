"""
항공편 스케줄 처리 통합 모듈 (Flight Schedule Processing)

이 모듈은 항공편 스케줄 처리의 Storage와 Response 기능을 통합합니다:
- FlightScheduleStorage: Redshift에서 항공편 데이터 조회 및 S3 저장
- FlightScheduleResponse: 프론트엔드용 JSON 응답 생성 (차트 데이터 포함)
"""

from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
from fastapi import HTTPException
from loguru import logger
from sqlalchemy import Connection

from packages.aws.s3.s3_manager import S3Manager
from app.routes.simulation.application.queries import (
    SELECT_AIRPORT_FLIGHTS_EXTENDED,
    SELECT_AIRPORT_SCHEDULE,
)


class FlightScheduleStorage:
    """항공편 스케줄 데이터 저장 전담 클래스"""

    def __init__(self):
        self.s3_manager = S3Manager()

    async def fetch_and_store(
        self,
        db: Connection,
        date: str,
        airport: str,
        flight_type: str,
        conditions: list | None,
        scenario_id: str,
        storage: str = "redshift",
    ) -> List[dict]:
        """항공편 스케줄 데이터 조회 및 저장"""
        try:
            # 1. 조건 변환
            converted_conditions = (
                self._convert_filter_conditions(conditions) if conditions else None
            )

            # 2. 데이터 조회
            flight_schedule_data = await self._fetch_flight_schedule_data(
                db,
                date,
                airport,
                flight_type,
                converted_conditions,
                scenario_id,
                storage,
            )

            # 3. S3 저장
            await self._save_flight_schedule_to_s3(flight_schedule_data, scenario_id)

            return flight_schedule_data

        except Exception as e:
            logger.error(f"Flight schedule storage failed: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to process flight schedule: {str(e)}"
            )

    async def _fetch_flight_schedule_data(
        self,
        db: Connection,
        date: str,
        airport: str,
        flight_type: str,
        conditions: list | None,
        scenario_id: str,
        storage: str = "s3",
    ):
        """항공기 스케줄 데이터 조회 (S3 우선, Redshift 대체)"""
        flight_schedule_data = None

        # S3 데이터 확인
        if storage == "s3":
            object_exists = await self.s3_manager.check_exists_async(
                scenario_id=scenario_id,
                filename="flight-schedule.parquet"
            )

            if object_exists:
                # S3Manager를 사용하여 parquet 파일을 dict로 읽기
                flight_schedule_data = await self.s3_manager.get_parquet_async(
                    scenario_id=scenario_id,
                    filename="flight-schedule.parquet",
                    as_dict=True
                )

        # Redshift에서 데이터 조회
        if not flight_schedule_data:
            # 날짜에 따른 테이블 선택
            target_date = datetime.strptime(date, "%Y-%m-%d").date()
            today = datetime.now().date()

            if target_date < today:
                # 과거 데이터: flights_extended 테이블
                query = SELECT_AIRPORT_FLIGHTS_EXTENDED
            else:
                # 오늘/미래 데이터: schedule 테이블
                query = SELECT_AIRPORT_SCHEDULE

            # redshift-connector를 직접 사용하여 경고 방지
            cursor = db.cursor()

            # flight_type에 따라 쿼리 파라미터 조정
            if flight_type == "departure":
                # departure 전용 쿼리로 수정
                modified_query = query.replace(
                    "AND (fe.departure_airport_iata = %s OR fe.arrival_airport_iata = %s)",
                    "AND fe.departure_airport_iata = %s",
                ).replace(
                    "AND (s.departure_station_code_iata = %s OR s.arrival_station_code_iata = %s)",
                    "AND s.departure_station_code_iata = %s",
                )
                cursor.execute(modified_query, (date, airport))
            elif flight_type == "arrival":
                # arrival 전용 쿼리로 수정
                modified_query = query.replace(
                    "AND (fe.departure_airport_iata = %s OR fe.arrival_airport_iata = %s)",
                    "AND fe.arrival_airport_iata = %s",
                ).replace(
                    "AND (s.departure_station_code_iata = %s OR s.arrival_station_code_iata = %s)",
                    "AND s.arrival_station_code_iata = %s",
                )
                cursor.execute(modified_query, (date, airport))
            else:
                # 기본값: 기존 OR 조건 유지
                cursor.execute(query, (date, airport, airport))

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            # DataFrame으로 변환
            flight_schedule_df = pd.DataFrame(rows, columns=columns)
            
            # ✅ 중복 제거: 같은 날짜 + 같은 항공사 + 같은 편명은 유니크하게 처리
            duplicate_columns = ['flight_date', 'operating_carrier_iata', 'flight_number']
            available_columns = [col for col in duplicate_columns if col in flight_schedule_df.columns]
            
            if available_columns and len(available_columns) == 3:
                before_count = len(flight_schedule_df)
                flight_schedule_df = flight_schedule_df.drop_duplicates(subset=available_columns, keep='first')
                after_count = len(flight_schedule_df)
                
                if before_count != after_count:
                    logger.info(f"🔧 중복 제거: {before_count}개 → {after_count}개 ({before_count - after_count}개 중복 제거)")
            
            flight_schedule_data = flight_schedule_df.to_dict("records")

            # 🚨 대량 데이터 보호: 조건 없으면 최대 500개로 제한
            if not conditions:
                if len(flight_schedule_data) > 500:
                    print(f"⚠️ Large dataset detected ({len(flight_schedule_data)} rows). Limiting to 500 for performance.")
                    flight_schedule_data = flight_schedule_data[:500]
            else:
                # 조건 필터링 (field들은 AND, values는 OR 조건)
                filtered_data = []
                for flight in flight_schedule_data:
                    include_flight = True

                    # 모든 field 조건을 만족해야 함 (AND 조건)
                    for cond in conditions:
                        field = cond["field"]
                        values = cond["values"]
                        flight_value = flight.get(field)

                        # 🔧 NULL 값도 올바르게 비교 (None in [None] 허용)
                        # values 중 하나라도 매치되면 됨 (OR 조건)
                        if flight_value not in values:
                            include_flight = False
                            break

                    if include_flight:
                        filtered_data.append(flight)

                flight_schedule_data = filtered_data

        return flight_schedule_data

    def _convert_filter_conditions(self, filter_conditions: list) -> list:
        """✅ 필터 조건을 그대로 사용 (매핑 제거) + unknown → NULL 변환"""
        if not filter_conditions:
            return []

        # ✅ 매핑 없이 받은 컬럼명 그대로 사용
        converted = []
        for filter_cond in filter_conditions:
            field = (
                filter_cond.get("field")
                if isinstance(filter_cond, dict)
                else filter_cond.field
            )
            values = (
                filter_cond.get("values")
                if isinstance(filter_cond, dict)
                else filter_cond.values
            )

            # 🆕 범용 로직: 모든 "unknown" 값을 NULL로 변환
            processed_values = []
            for value in values:
                if value == "unknown":
                    processed_values.append(None)  # NULL 조건으로 변환
                else:
                    processed_values.append(value)

            # ✅ 변환된 조건 추가
            converted.append({"field": field, "values": processed_values})

        return converted

    async def _save_flight_schedule_to_s3(
        self, flight_schedule_data: list, scenario_id: str
    ):
        """S3에 항공편 스케줄 데이터 저장"""
        if not flight_schedule_data:
            return

        # S3Manager를 사용하여 parquet 저장
        await self.s3_manager.save_parquet_async(
            scenario_id=scenario_id,
            filename="flight-schedule.parquet",
            df=pd.DataFrame(flight_schedule_data)
        )


class FlightScheduleResponse:
    """항공편 스케줄 프론트엔드 응답 생성 전담 클래스"""

    async def build_response(
        self,
        flight_schedule_data: list,
        applied_conditions: list | None,
        flight_type: str,
        airport: str = None,
        date: str = None,
        scenario_id: str = None,
    ) -> dict:
        """항공편 스케줄 응답 데이터 구성 (차트 + 메타데이터 전용)"""
        if not flight_schedule_data:
            return self._get_empty_response(airport, date, scenario_id)

        flight_df = pd.DataFrame(flight_schedule_data)

        # 차트 데이터 생성 (flight_type에 따라 구분)
        chart_data = await self._build_chart_data(flight_df, flight_type)

        # Parquet 메타데이터 생성 (Passenger Schedule에서 사용)
        parquet_metadata = self._build_parquet_metadata(flight_df)

        # 응답 구조: flight-filter.json처럼 컨텍스트 정보 먼저 포함
        response = {}
        
        # 요청 컨텍스트 정보 (처음 3개 키)
        if airport:
            response["airport"] = airport
        if date:
            response["date"] = date
        if scenario_id:
            response["scenario_id"] = scenario_id
            
        # 기존 응답 데이터
        response.update({
            "total": len(flight_df),
            "chart_x_data": chart_data.get("x_data", []),
            "chart_y_data": chart_data.get("y_data", {}),
            "parquet_metadata": parquet_metadata,
        })

        return response

    def _get_empty_response(self, airport: str = None, date: str = None, scenario_id: str = None) -> dict:
        """빈 응답 데이터 반환 (차트 + 메타데이터 전용)"""
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
            "total": 0,
            "chart_x_data": [],
            "chart_y_data": {},
            "parquet_metadata": [],
        })
        
        return response

    async def _build_chart_data(
        self, flight_df: pd.DataFrame, flight_type: str = "departure"
    ) -> dict:
        """차트 데이터 생성 - departure/arrival 구분"""
        chart_result = {}
        chart_x_data = []

        # flight_type에 따라 사용할 터미널 컬럼 결정
        terminal_column = f"{flight_type}_terminal"

        # flight_type에 따라 country/region 컬럼 결정
        if flight_type == "departure":
            country_column = "arrival_country"  # 국가 이름 사용
            region_column = "arrival_region"
        else:  # arrival
            country_column = "departure_country"  # 국가 이름 사용
            region_column = "departure_region"

        # 차트 생성을 위한 그룹 컬럼들
        group_columns = [
            "operating_carrier_name",
            terminal_column,
            "flight_type",
            country_column,
            region_column,
        ]
        group_labels = ["airline", "terminal", "type", "country", "region"]

        for i, group_column in enumerate(group_columns):
            if group_column in flight_df.columns:
                chart_result_data = await self._create_flight_schedule_chart(
                    flight_df, group_column, flight_type
                )

                if chart_result_data:
                    chart_result[group_labels[i]] = chart_result_data["traces"]
                    chart_x_data = chart_result_data["default_x"]

        return {
            "x_data": chart_x_data,
            "y_data": chart_result,
        }

    async def _create_flight_schedule_chart(
        self, flight_df: pd.DataFrame, group_column: str, flight_type: str = "departure"
    ):
        """항공편 스케줄 차트 데이터 생성"""
        # flight_type에 따라 사용할 시간 컬럼 결정
        time_column = f"scheduled_{flight_type}_local"

        if time_column not in flight_df.columns:
            return None

        # null 값을 "Unknown"으로 변환 (모든 그룹 컬럼에 대해)
        flight_df = flight_df.copy()
        flight_df[group_column] = flight_df[group_column].fillna("Unknown")

        flight_df[time_column] = pd.to_datetime(flight_df[time_column]).dt.floor("h")

        df_grouped = (
            flight_df.groupby([time_column, group_column]).size().unstack(fill_value=0)
        )
        df_grouped = df_grouped.sort_index()

        if df_grouped.empty:
            return None

        total_groups = df_grouped.shape[1]
        has_etc = total_groups > 9

        if has_etc:
            top_9_columns = df_grouped.sum().nlargest(9).index.tolist()
            df_grouped["ETC"] = df_grouped.drop(
                columns=top_9_columns, errors="ignore"
            ).sum(axis=1)
            df_grouped = df_grouped[top_9_columns + ["ETC"]]
        else:
            top_9_columns = df_grouped.columns.tolist()

        day = df_grouped.index[0].date()
        all_hours = pd.date_range(
            start=pd.Timestamp(day),
            end=pd.Timestamp(day) + pd.Timedelta(hours=23),
            freq="h",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        group_order = df_grouped.sum().sort_values(ascending=False).index.tolist()
        if has_etc and "ETC" in group_order:
            group_order.remove("ETC")
            group_order.append("ETC")

        default_x = df_grouped.index.strftime("%H:%M").tolist()
        traces = [
            {
                "name": column,
                "order": group_order.index(column),
                "y": df_grouped[column].tolist(),
                "acc_y": df_grouped[column].cumsum().tolist(),
            }
            for column in df_grouped.columns
        ]

        return {"traces": traces, "default_x": default_x}

    def _build_parquet_metadata(self, flight_df: pd.DataFrame) -> list:
        """
        새로운 Parquet 메타데이터 생성 - flights + indices 포함
        
        각 컬럼의 유니크값별로 해당하는 항공편 조합과 인덱스를 제공합니다.
        프론트엔드에서 필터 선택 시 구체적인 항공편들을 바로 확인할 수 있고,
        백엔드에서는 인덱스를 통해 빠른 데이터 조회가 가능합니다.
        
        Args:
            flight_df: 항공편 스케줄 DataFrame
            
        Returns:
            컬럼별 메타데이터 리스트 [{"column": "컬럼명", "values": {"값": {"flights": [...], "indices": [...]}}}]
        """
        if flight_df.empty:
            return []
        
        # 핵심 컬럼 존재 여부 확인
        required_cols = ['operating_carrier_iata', 'flight_number']
        if not all(col in flight_df.columns for col in required_cols):
            logger.error("필수 컬럼이 누락됨: operating_carrier_iata, flight_number")
            return []
        
        metadata = []
        
        # 선택된 컬럼들만 처리 (departure 컬럼은 arrival 쌍도 포함)
        target_columns = [
            'operating_carrier_name',
            'departure_airport_iata', 'arrival_airport_iata',
            'scheduled_departure_local', 'scheduled_arrival_local',
            'aircraft_type_icao',
            'departure_terminal', 'arrival_terminal',
            'flight_type',
            'departure_city', 'arrival_city',
            'departure_country', 'arrival_country',
            'departure_region', 'arrival_region',
            'total_seats'
        ]
        
        for column_name in target_columns:
            if column_name not in flight_df.columns:
                continue
                
            try:
                # 1. NaN 제거 후 유니크값 추출
                unique_values = flight_df[column_name].dropna().unique()
                
                # 2. 각 유니크값에 대한 데이터 구성
                values_dict = {}
                
                for unique_value in unique_values:
                    # 해당 값에 매치되는 행들 찾기
                    mask = flight_df[column_name] == unique_value
                    matched_rows = flight_df[mask]
                    
                    # flights 조합 생성 (operating_carrier_iata + flight_number)
                    flights = []
                    for _, row in matched_rows.iterrows():
                        carrier = str(row['operating_carrier_iata']) if pd.notna(row['operating_carrier_iata']) else ""
                        flight_num = str(row['flight_number']) if pd.notna(row['flight_number']) else ""
                        if carrier and flight_num:  # 둘 다 유효한 값일 때만 추가
                            flights.append(f"{carrier}{flight_num}")
                    
                    # 인덱스 추출 (원본 DataFrame 기준)
                    indices = matched_rows.index.tolist()
                    
                    # 결과 저장 (유효한 데이터가 있을 때만)
                    if flights and indices:
                        values_dict[str(unique_value)] = {
                            "flights": flights,
                            "indices": indices
                        }
                
                # 컬럼 메타데이터 추가 (값이 있을 때만)
                if values_dict:
                    metadata.append({
                        "column": column_name,
                        "values": values_dict
                    })
                
            except Exception as e:
                logger.warning(f"컬럼 '{column_name}' 메타데이터 생성 실패: {str(e)}")
                continue
        
        logger.info(f"📊 새로운 Parquet 메타데이터 생성 완료: {len(metadata)}개 컬럼")
        return metadata

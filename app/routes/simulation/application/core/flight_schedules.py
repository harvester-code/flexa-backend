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

import awswrangler as wr
from packages.doppler.client import get_secret
from packages.aws.s3.storage import boto3_session, check_s3_object_exists
from app.routes.simulation.application.queries import (
    SELECT_AIRPORT_FLIGHTS_EXTENDED,
    SELECT_AIRPORT_SCHEDULE,
)


class FlightScheduleStorage:
    """항공편 스케줄 데이터 저장 전담 클래스"""

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
            object_exists = await check_s3_object_exists(
                bucket_name=get_secret("AWS_S3_BUCKET_NAME"),
                object_key=f"{scenario_id}/flight-schedule.parquet",
            )

            if object_exists:
                flight_schedule_data = wr.s3.read_parquet(
                    path=f"s3://{get_secret('AWS_S3_BUCKET_NAME')}/{scenario_id}/flight-schedule.parquet",
                    boto3_session=boto3_session,
                ).to_dict("records")

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
            flight_schedule_data = flight_schedule_df.to_dict("records")

            # 조건 필터링 (field들은 AND, values는 OR 조건)
            if conditions:
                filtered_data = []
                for flight in flight_schedule_data:
                    include_flight = True

                    # 모든 field 조건을 만족해야 함 (AND 조건)
                    for cond in conditions:
                        field = cond["field"]
                        values = cond["values"]
                        flight_value = flight.get(field)

                        if flight_value is None:
                            include_flight = False
                            break

                        # values 중 하나라도 매치되면 됨 (OR 조건)
                        if flight_value not in values:
                            include_flight = False
                            break

                    if include_flight:
                        filtered_data.append(flight)

                flight_schedule_data = filtered_data

        return flight_schedule_data

    def _convert_filter_conditions(self, filter_conditions: list) -> list:
        """필터 조건을 데이터베이스 컬럼명으로 매핑"""
        if not filter_conditions:
            return []

        field_mapping = {
            "types": "flight_type",
            "terminal": "departure_terminal",  # 기본값, flight_type에 따라 동적으로 변경됨
            "airline": "operating_carrier_iata",
        }

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

            if mapped_field := field_mapping.get(field):
                converted.append({"field": mapped_field, "values": values})

        return converted

    async def _save_flight_schedule_to_s3(
        self, flight_schedule_data: list, scenario_id: str
    ):
        """S3에 항공편 스케줄 데이터 저장"""
        if not flight_schedule_data:
            return

        wr.s3.to_parquet(
            df=pd.DataFrame(flight_schedule_data),
            path=f"s3://{get_secret('AWS_S3_BUCKET_NAME')}/{scenario_id}/flight-schedule.parquet",
            boto3_session=boto3_session,
        )


class FlightScheduleResponse:
    """항공편 스케줄 프론트엔드 응답 생성 전담 클래스"""

    async def build_response(
        self,
        flight_schedule_data: list,
        applied_conditions: list | None,
        flight_type: str,
    ) -> dict:
        """항공편 스케줄 응답 데이터 구성"""
        if not flight_schedule_data:
            return self._get_empty_response()

        flight_df = pd.DataFrame(flight_schedule_data)

        # 항공사별 타입 분류
        types_data = self._build_airline_types(flight_df)

        # 터미널별 항공사 분류 (flight_type에 따라 구분)
        terminals_data = self._build_terminal_airlines(flight_df, flight_type)

        # 차트 데이터 생성 (flight_type에 따라 구분)
        chart_data = await self._build_chart_data(flight_df, flight_type)

        return {
            "total": len(flight_df),
            "types": types_data,
            "terminals": terminals_data,
            "chart_x_data": chart_data.get("x_data", []),
            "chart_y_data": chart_data.get("y_data", {}),
        }

    def _get_empty_response(self) -> dict:
        """빈 응답 데이터 반환"""
        return {
            "total": 0,
            "types": {},
            "terminals": {},
            "chart_x_data": [],
            "chart_y_data": {},
        }

    def _build_airline_types(self, flight_df: pd.DataFrame) -> dict:
        """항공사별 타입 분류"""
        # 항공사별 고유 데이터 추출
        airline_df = flight_df[
            ["operating_carrier_iata", "operating_carrier_name", "flight_type"]
        ].drop_duplicates()

        # 타입별 항공사 분류
        international_mask = airline_df["flight_type"] == "International"
        domestic_mask = airline_df["flight_type"] == "Domestic"

        international_airlines = (
            airline_df[international_mask][
                ["operating_carrier_iata", "operating_carrier_name"]
            ]
            .rename(
                columns={
                    "operating_carrier_iata": "iata",
                    "operating_carrier_name": "name",
                }
            )
            .to_dict("records")
        )

        domestic_airlines = (
            airline_df[domestic_mask][
                ["operating_carrier_iata", "operating_carrier_name"]
            ]
            .rename(
                columns={
                    "operating_carrier_iata": "iata",
                    "operating_carrier_name": "name",
                }
            )
            .to_dict("records")
        )

        return {
            "International": international_airlines,
            "Domestic": domestic_airlines,
        }

    def _build_terminal_airlines(
        self, flight_df: pd.DataFrame, flight_type: str = "departure"
    ) -> dict:
        """터미널별 항공사 분류 - departure/arrival 구분"""
        # flight_type에 따라 사용할 터미널 컬럼 결정
        terminal_column = f"{flight_type}_terminal"

        if terminal_column not in flight_df.columns:
            return {}

        # 터미널별 항공사 그룹화 (중복 제거)
        terminal_groups = (
            flight_df[
                [
                    terminal_column,
                    "operating_carrier_iata",
                    "operating_carrier_name",
                ]
            ]
            .fillna({terminal_column: "unknown"})
            .drop_duplicates()
            .groupby(terminal_column)
        )

        terminals = {}
        for terminal, group in terminal_groups:
            airlines = (
                group[["operating_carrier_iata", "operating_carrier_name"]]
                .rename(
                    columns={
                        "operating_carrier_iata": "iata",
                        "operating_carrier_name": "name",
                    }
                )
                .to_dict("records")
            )
            terminals[terminal] = airlines

        return terminals

    async def _build_chart_data(
        self, flight_df: pd.DataFrame, flight_type: str = "departure"
    ) -> dict:
        """차트 데이터 생성 - departure/arrival 구분"""
        chart_result = {}
        chart_x_data = []

        # flight_type에 따라 사용할 터미널 컬럼 결정
        terminal_column = f"{flight_type}_terminal"

        # 차트 생성을 위한 그룹 컬럼들
        group_columns = [
            "operating_carrier_name",
            terminal_column,
            "flight_type",
            "arrival_country_code",
            "arrival_region",
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
            df_grouped["etc"] = df_grouped.drop(
                columns=top_9_columns, errors="ignore"
            ).sum(axis=1)
            df_grouped = df_grouped[top_9_columns + ["etc"]]
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
        if has_etc and "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

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

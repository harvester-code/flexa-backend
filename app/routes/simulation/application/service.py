import json
from datetime import datetime
from typing import List

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError
from dependency_injector.wiring import inject
from fastapi import HTTPException, status
from sqlalchemy import Connection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from ulid import ULID

from app.routes.simulation.application.queries import (
    SELECT_AIRPORT_FLIGHTS_EXTENDED,
    SELECT_AIRPORT_SCHEDULE,
)
from app.routes.simulation.domain.simulation import (
    ScenarioInformation,
    ScenarioMetadata,
)
from app.routes.simulation.infra.models import UserInformation
from app.routes.simulation.infra.repository import SimulationRepository
from packages.boto3_session import boto3_session
from packages.common import TimeStamp

from packages.secrets import get_secret
from packages.storages import check_s3_object_exists


class SimulationService:
    """
    시뮬레이션 서비스 - 정리된 버전
    현재 사용하는 메서드들만 포함
    """

    @inject
    def __init__(self, simulation_repo: SimulationRepository):
        self.simulation_repo = simulation_repo
        self.timestamp = TimeStamp()

    # =====================================
    # NOTE: 시뮬레이션 시나리오 관리

    async def fetch_scenario_information(
        self,
        db: AsyncSession,
        user_id: str,
    ):
        scenario = await self.simulation_repo.fetch_scenario_information(db, user_id)
        return scenario

    async def create_scenario_information(
        self,
        db: AsyncSession,
        user_id: str,
        editor: str,
        name: str,
        terminal: str,
        airport: str | None,
        memo: str | None,
    ):
        scenario_id = str(ULID())

        scenario_information: ScenarioInformation = ScenarioInformation(
            id=None,
            user_id=user_id,
            editor=editor,
            name=name,
            terminal=terminal,
            airport=airport,
            memo=memo,
            target_flight_schedule_date=None,
            created_at=self.timestamp.time_now(timezone="UTC"),
            updated_at=self.timestamp.time_now(timezone="UTC"),
            scenario_id=scenario_id,
        )

        scenario_metadata: ScenarioMetadata = ScenarioMetadata(
            scenario_id=scenario_id,
            overview=None,
            history=[],
            flight_schedule=None,
            passenger_schedule=None,
            processing_procedures=None,
            facility_connection=None,
            facility_information=None,
        )

        await self.simulation_repo.create_scenario_information(
            db, scenario_information, scenario_metadata
        )

        return {
            "scenario_id": scenario_id,
            "message": "Scenario created successfully",
        }

    async def update_scenario_information(
        self,
        db: AsyncSession,
        scenario_id: str,
        name: str | None,
        terminal: str | None,
        airport: str | None,
        memo: str | None,
    ):
        await self.simulation_repo.update_scenario_information(
            db, scenario_id, name, terminal, airport, memo
        )

    async def deactivate_scenario_information(self, db: AsyncSession, ids: List[str]):
        await self.simulation_repo.deactivate_scenario_information(db, ids)

    async def update_master_scenario(
        self, db: AsyncSession, user_id: str, scenario_id: str
    ):
        # 사용자의 그룹 ID 조회
        result = await db.execute(
            select(UserInformation.group_id).where(UserInformation.user_id == user_id)
        )
        user_group_id = result.scalar_one_or_none()
        if not user_group_id:
            raise ValueError("User group not found")

        await self.simulation_repo.update_master_scenario(
            db, user_group_id, scenario_id
        )

    async def update_scenario_target_flight_schedule_date(
        self, db: AsyncSession, scenario_id: str, date: str
    ):
        target_flight_schedule_date = datetime.strptime(date, "%Y-%m-%d")
        await self.simulation_repo.update_scenario_target_flight_schedule_date(
            db, scenario_id, target_flight_schedule_date
        )

    # =====================================
    # NOTE: 항공편 스케줄 처리

    async def generate_scenario_flight_schedule(
        self,
        db: Connection,
        date: str,
        airport: str,
        condition: list | None,
        scenario_id: str,
    ):
        """시나리오별 항공편 스케줄 조회 및 차트 데이터 생성"""
        try:
            # 1. 조건 변환
            converted_conditions = (
                self._convert_filter_conditions(condition) if condition else None
            )

            # 2. 데이터 조회
            flight_schedule_data = await self.fetch_flight_schedule_data(
                db, date, airport, converted_conditions, scenario_id, storage="redshift"
            )

            # 3. S3 저장
            await self._save_flight_schedule_to_s3(flight_schedule_data, scenario_id)

            # 4. 응답 생성
            return await self._build_flight_schedule_response(
                flight_schedule_data, condition
            )

        except Exception as e:
            raise HTTPException(
                status_code=500, detail=f"Failed to process flight schedule: {str(e)}"
            )

    async def fetch_flight_schedule_data(
        self,
        db: Connection,
        date: str,
        airport: str,
        condition: list | None,
        scenario_id: str,
        storage: str = "s3",
    ):
        """항공기 스케줄 데이터 조회"""
        flight_schedule_data = None

        # S3 데이터 확인
        if storage == "s3":
            object_exists = check_s3_object_exists(
                bucket_name=get_secret("AWS_S3_BUCKET_NAME"),
                object_key=f"scenarios/{scenario_id}/flight-schedule.parquet",
            )

            if object_exists:
                flight_schedule_data = wr.s3.read_parquet(
                    path=f"s3://{get_secret('AWS_S3_BUCKET_NAME')}/scenarios/{scenario_id}/flight-schedule.parquet",
                    boto3_session=boto3_session,
                ).to_dict("records")

        # Redshift에서 데이터 조회
        if not flight_schedule_data:
            # 날짜에 따른 테이블 선택
            from datetime import datetime

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
            cursor.execute(query, (date, airport, airport))
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            # DataFrame으로 변환
            flight_schedule_df = pd.DataFrame(rows, columns=columns)
            flight_schedule_data = flight_schedule_df.to_dict("records")

            # 조건 필터링
            if condition:
                filtered_data = []
                for flight in flight_schedule_data:
                    include_flight = True
                    for cond in condition:
                        criteria = cond["criteria"]
                        value = cond["value"]
                        flight_value = flight.get(criteria)

                        if flight_value is None:
                            include_flight = False
                            break

                        if isinstance(value, list):
                            if flight_value not in value:
                                include_flight = False
                                break
                        else:
                            if flight_value != value:
                                include_flight = False
                                break

                    if include_flight:
                        filtered_data.append(flight)

                flight_schedule_data = filtered_data

        return flight_schedule_data

    # =====================================
    # NOTE: 승객 스케줄 처리

    async def generate_passenger_schedule(
        self,
        db: Connection,
        destribution_conditions: list,
        scenario_id: str,
    ):
        # S3에서 기존 항공편 스케줄 데이터 읽어오기
        flight_schedule_data = await self.fetch_flight_schedule_data(
            db=db,
            date="",
            airport="",
            condition=None,
            scenario_id=scenario_id,
            storage="s3",
        )

        if not flight_schedule_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Flight schedule data not found. Please load flight schedule first.",
            )

        flight_schedule_df = pd.DataFrame(flight_schedule_data)

        # Summary 데이터 생성
        average_seats = flight_schedule_df["total_seats"].mean()
        total_flights = len(flight_schedule_df)
        summary = {
            "flights": total_flights,
            "avg_seats": round(average_seats, 2),
            "load_factor": 85,
        }

        # 여객 데이터 생성
        pax_df = await self._calculate_show_up_pattern(
            flight_schedule_data, destribution_conditions
        )

        # S3에 데이터 저장
        wr.s3.to_parquet(
            df=pax_df,
            path=f"s3://{get_secret('AWS_S3_BUCKET_NAME')}/scenarios/{scenario_id}/show-up-passenger.parquet",
            boto3_session=boto3_session,
        )

        # 차트 데이터 생성 (주요 그룹 컬럼들로 직접 처리)
        chart_result = {}
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
                chart_data = await self._create_show_up_summary(pax_df, group_column)
                chart_result[group_labels[i]] = chart_data["traces"]

        return {
            "total": pax_df.shape[0],
            "summary": summary,
            "bar_chart_x_data": chart_data["default_x"],
            "bar_chart_y_data": chart_result,
        }

    # =====================================
    # NOTE: S3 메타데이터 처리

    async def save_scenario_metadata(self, scenario_id: str, metadata: dict) -> dict:
        """시나리오 메타데이터를 S3에 직접 저장합니다."""
        bucket_name = get_secret("AWS_S3_BUCKET_NAME")
        object_key = f"scenarios/{scenario_id}/metadata-for-frontend.json"

        try:
            s3 = boto3.client(
                "s3",
                config=Config(region_name="ap-northeast-2"),
            )

            s3.put_object(
                ContentType="application/json",
                Bucket=bucket_name,
                Key=object_key,
                Body=json.dumps(metadata, ensure_ascii=False, indent=2),
            )

            return {
                "s3_key": object_key,
                "bucket": bucket_name,
                "saved_at": datetime.now().isoformat(),
            }

        except Exception as e:
            raise Exception(f"Failed to save metadata to S3: {str(e)}")

    async def load_scenario_metadata(self, scenario_id: str) -> dict:
        """S3에서 시나리오 메타데이터를 불러옵니다."""
        bucket_name = get_secret("AWS_S3_BUCKET_NAME")
        object_key = f"scenarios/{scenario_id}/metadata-for-frontend.json"

        try:
            s3 = boto3.client(
                "s3",
                config=Config(region_name="ap-northeast-2"),
            )

            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            metadata = json.loads(response["Body"].read().decode("utf-8"))

            return {
                "scenario_id": scenario_id,
                "metadata": metadata,
                "s3_key": object_key,
                "loaded_at": datetime.now().isoformat(),
            }

        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "NoSuchKey":
                return {
                    "scenario_id": scenario_id,
                    "metadata": {
                        "tabs": {
                            "overview": {},
                            "flightSchedule": {},
                            "passengerSchedule": {},
                            "facilityConnection": {},
                            "facilityInformation": {},
                        }
                    },
                    "s3_key": object_key,
                    "loaded_at": datetime.now().isoformat(),
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"S3 error ({error_code}): {str(e)}",
                )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to load metadata from S3: {str(e)}",
            )

    # =====================================
    # NOTE: 헬퍼 메서드들

    def _convert_filter_conditions(self, filter_conditions: list) -> list:
        """FilterCondition을 기존 Condition 형식으로 변환"""
        if not filter_conditions:
            return []

        criteria_mapping = {
            "types": "I/D",
            "airline": "Airline",
            "terminal": "Terminal",
        }

        converted = []
        for filter_cond in filter_conditions:
            criteria = (
                filter_cond.get("criteria")
                if isinstance(filter_cond, dict)
                else filter_cond.criteria
            )
            value = (
                filter_cond.get("value")
                if isinstance(filter_cond, dict)
                else filter_cond.value
            )

            if mapped_criteria := criteria_mapping.get(criteria):
                converted.append({"criteria": mapped_criteria, "value": value})

        return converted

    async def _save_flight_schedule_to_s3(
        self, flight_schedule_data: list, scenario_id: str
    ):
        """S3에 항공편 스케줄 데이터 저장"""
        if not flight_schedule_data:
            return

        wr.s3.to_parquet(
            df=pd.DataFrame(flight_schedule_data),
            path=f"s3://{get_secret('AWS_S3_BUCKET_NAME')}/scenarios/{scenario_id}/flight-schedule.parquet",
            boto3_session=boto3_session,
        )

    async def _build_flight_schedule_response(
        self, flight_schedule_data: list, applied_conditions: list | None
    ) -> dict:
        """항공편 스케줄 응답 데이터 구성"""
        if not flight_schedule_data:
            return self._get_empty_response()

        flight_df = pd.DataFrame(flight_schedule_data)

        # 항공사별 타입 분류
        types_data = self._build_airline_types(flight_df)

        # 터미널별 항공사 분류
        terminals_data = self._build_terminal_airlines(flight_df)

        # 차트 데이터 생성
        chart_data = await self._build_chart_data(flight_df)

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

    def _build_terminal_airlines(self, flight_df: pd.DataFrame) -> dict:
        """터미널별 항공사 분류"""
        # 터미널별 항공사 그룹화 (중복 제거)
        terminal_groups = (
            flight_df[
                [
                    "departure_terminal",
                    "operating_carrier_iata",
                    "operating_carrier_name",
                ]
            ]
            .fillna({"departure_terminal": "unknown"})
            .drop_duplicates()
            .groupby("departure_terminal")
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

    async def _build_chart_data(self, flight_df: pd.DataFrame) -> dict:
        """차트 데이터 생성"""
        chart_result = {}
        chart_x_data = []

        # 차트 생성을 위한 그룹 컬럼들 (직접 처리)
        group_columns = [
            "operating_carrier_name",
            "departure_terminal",
            "flight_type",
            "arrival_country_code",
            "arrival_region",
        ]
        group_labels = ["airline", "terminal", "type", "country", "region"]

        for i, group_column in enumerate(group_columns):
            if group_column in flight_df.columns:
                chart_result_data = await self._create_flight_schedule_chart(
                    flight_df, group_column
                )

                if chart_result_data:
                    chart_result[group_labels[i]] = chart_result_data["traces"]
                    chart_x_data = chart_result_data["default_x"]

        return {
            "x_data": chart_x_data,
            "y_data": chart_result,
        }

    async def _create_flight_schedule_chart(
        self, flight_df: pd.DataFrame, group_column: str
    ):
        flight_df["scheduled_departure_local"] = pd.to_datetime(
            flight_df["scheduled_departure_local"]
        ).dt.floor("h")

        df_grouped = (
            flight_df.groupby(["scheduled_departure_local", group_column])
            .size()
            .unstack(fill_value=0)
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
            }
            for column in df_grouped.columns
        ]

        return {"traces": traces, "default_x": default_x}

    async def _calculate_show_up_pattern(
        self, data: list, destribution_conditions: list
    ):
        df = pd.DataFrame(data)
        pax_df = pd.DataFrame()

        for filter in destribution_conditions:
            partial_df = df.copy()
            for condition in filter.conditions:
                # 직접 매핑 (프론트엔드에서 컬럼명 직접 사용)
                column_mapping = {
                    "flight_type": "flight_type",
                    "operating_carrier_iata": "operating_carrier_iata",
                    "region_name": "arrival_region",
                    "country_code": "arrival_country_code",
                }
                column_name = column_mapping.get(condition.criteria, condition.criteria)
                partial_df = partial_df[partial_df[column_name].isin(condition.value)]
            partial_pax_df = partial_df.loc[
                partial_df.index.repeat((partial_df["total_seats"] * 0.85).astype(int))
            ]
            arrival_times = []
            for _, row in partial_df.iterrows():
                samples = np.random.normal(
                    loc=filter.mean,
                    scale=filter.standard_deviation,
                    size=int(row["total_seats"] * 0.85),
                )
                arrival_times.extend(samples)
            partial_pax_df["show_up_time"] = pd.to_datetime(
                partial_pax_df["scheduled_departure_local"]
            ) - pd.to_timedelta(arrival_times, unit="minutes")
            partial_pax_df["show_up_time"] = partial_pax_df["show_up_time"].dt.round(
                "s"
            )
            pax_df = pd.concat([pax_df, partial_pax_df], ignore_index=True)
            df.drop(index=partial_df.index, inplace=True)

        return pax_df

    async def _create_show_up_summary(self, pax_df: pd.DataFrame, group_column: str):
        time_unit = "10min"
        pax_df["show_up_time"] = pax_df["show_up_time"].dt.floor(time_unit)
        df_grouped = (
            pax_df.groupby(["show_up_time", group_column]).size().unstack(fill_value=0)
        )
        df_grouped = df_grouped.sort_index()

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

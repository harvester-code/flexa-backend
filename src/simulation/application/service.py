import json
import os
from datetime import datetime, time, timedelta
from typing import List

import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
import pendulum
from botocore.config import Config
from dependency_injector.wiring import inject
from fastapi import BackgroundTasks
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncSession
from ulid import ULID

from src.boto3_session import boto3_session
from src.common import TimeStamp
from src.constants import COL_FILTER_MAP, CRITERIA_MAP
from src.simulation.application.queries import (
    SELECT_AIRPORT_ARRIVAL,
    SELECT_AIRPORT_DEPARTURE,
    SELECT_AIRPORT_DEPARTURE_SCHEDULE,
)
from src.simulation.domain.simulation import ScenarioMetadata, SimulationScenario
from src.simulation.infra.repository import SimulationRepository
from src.simulation.infra.sqs.producer import send_message_to_sqs
from src.storages import check_s3_object_exists


class SimulationService:
    """
    //매서드 정의//
    generate : 각 controller의 최상위 매서드 (하위 매서드를 포함)
    run_simulation : 최종 시뮬레이션 코드 (현재 분할 필요)

    _calculate: 새로운 데이터 생성 (내장함수)
    _create : 차트 생성 (내장함수)

    fetch : get
    create : post
    duplicate : post
    update : patch / put
    deactivate : patch (delete)
    """

    # TODO: 그래프 만드는 코드들이 서로 비슷한데 합칠수는 없을까?

    @inject
    def __init__(self, simulation_repo: SimulationRepository):
        self.simulation_repo = simulation_repo
        self.timestamp = TimeStamp()

    # =====================================
    # NOTE: 시뮬레이션 시나리오

    async def fetch_simulation_scenario(
        self,
        db: AsyncSession,
        user_id: str,
        group_id: str,
        page: int,
        items_per_page: int,
    ):

        scenario = await self.simulation_repo.fetch_simulation_scenario(
            db, user_id, group_id, page, items_per_page
        )

        return scenario

    async def fetch_simulation_location(
        self,
        db: AsyncSession,
        group_id: str,
    ):

        location = await self.simulation_repo.fetch_simulation_location(db, group_id)

        return location

    async def create_simulation_scenario(
        self,
        db: AsyncSession,
        user_id: str,
        name: str,
        memo: str,
        airport: str,
        terminal: str,
        editor: str,
    ):
        id = str(ULID())

        simulation_scenario: SimulationScenario = SimulationScenario(
            id=id,
            user_id=user_id,
            simulation_name=name,
            size=None,
            airport=airport,
            terminal=terminal,
            editor=editor,
            memo=memo,
            simulation_date=None,
            updated_at=self.timestamp.time_now(),
            created_at=self.timestamp.time_now(),
        )

        scenario_metadata: ScenarioMetadata = ScenarioMetadata(
            scenario_id=id,
            overview=None,
            history=None,
            flight_sch=None,
            passenger_sch=None,
            passenger_attr=None,
            facility_conn=None,
            facility_info=None,
        )

        await self.simulation_repo.create_simulation_scenario(
            db, simulation_scenario, scenario_metadata
        )

        return {
            "scenario_id": id,
            "overview": {},
            "history": {},
            "flight_sch": {},
            "passenger_sch": {},
            "passenger_attr": {},
            "facility_conn": {},
            "facility_info": {},
        }

    async def update_simulation_scenario(
        self, db: AsyncSession, id: str, name: str | None, memo: str | None
    ):

        await self.simulation_repo.update_simulation_scenario(db, id, name, memo)

    async def deactivate_simulation_scenario(self, db: AsyncSession, ids: List[str]):

        await self.simulation_repo.deactivate_simulation_scenario(db, ids)

    async def duplicate_simulation_scenario(
        self, db: AsyncSession, user_id: str, old_scenario_id: str, editor: str
    ):

        new_scenario_id = str(ULID())
        time_now = self.timestamp.time_now()

        await self.simulation_repo.duplicate_simulation_scenario(
            db, user_id, old_scenario_id, new_scenario_id, editor, time_now
        )

    async def update_master_scenario(
        self, db: AsyncSession, group_id: str, scenario_id: str
    ):

        await self.simulation_repo.update_master_scenario(db, group_id, scenario_id)

    # =====================================
    # NOTE: 시나리오 메타데이터

    async def fetch_scenario_metadata(self, db: AsyncSession, scenario_id: str):

        result = await self.simulation_repo.fetch_scenario_metadata(db, scenario_id)

        checkpoint = self.timestamp.time_now()
        result["checkpoint"] = checkpoint

        return result

    async def update_scenario_metadata(
        self,
        db: AsyncSession,
        scenario_id: str,
        overview: dict | None,
        history: list | None,
        flight_sch: dict | None,
        passenger_sch: dict | None,
        passenger_attr: dict | None,
        facility_conn: dict | None,
        facility_info: dict | None,
    ):

        # if history:
        #     history["modification_date"] = self.timestamp.time_now()

        scenario_metadata: ScenarioMetadata = ScenarioMetadata(
            scenario_id=scenario_id,
            overview=overview,
            history=history,
            flight_sch=flight_sch,
            passenger_sch=passenger_sch,
            passenger_attr=passenger_attr,
            facility_conn=facility_conn,
            facility_info=facility_info,
        )

        time_now = self.timestamp.time_now()

        await self.simulation_repo.update_scenario_metadata(
            db, scenario_metadata, time_now
        )

    async def fetch_flight_schedule_data(
        self,
        db: Connection,
        date: str,
        airport: str,
        condition: list | None,
        scenario_id: str,
        storage: str = "s3",  # NOTE: "s3" | "snowflake"
    ):
        """항공기 스케줄 데이터 조회

        Args:
            db (Connection): 데이터베이스 연결 객체.
            date (str): 항공 스케줄 데이터를 조회할 날짜 (형식: 'YYYY-MM-DD').
            airport (str): 조회할 공항 코드 (IATA 코드).
            condition (list | None): 필터링 조건 리스트. 각 조건은 필드와 값으로 구성됨.

        Returns:
            list[dict]: 조회된 항공 스케줄 데이터. 각 항목은 항공편 정보를 포함하는 딕셔너리.
        """

        flight_schedule_data = None

        # ======================================================
        # NOTE: S3 데이터 확인
        if storage == "s3":
            object_exists = check_s3_object_exists(
                bucket_name="flexa-dev-ap-northeast-2-data-storage",
                object_key=f"simulations/flight-schedule-data/{scenario_id}.parquet",
            )

            if not object_exists:
                return flight_schedule_data

            flight_schedule_data = wr.s3.read_parquet(
                path=f"s3://flexa-dev-ap-northeast-2-data-storage/simulations/flight-schedule-data/{scenario_id}.parquet",
                boto3_session=boto3_session,
            )
            return flight_schedule_data.to_dict(orient="records")

        # ======================================================
        # NOTE: SNOWFLAKE 데이터 확인
        if storage == "snowflake":
            FLIGHT_IO = "departure"

            # 현재 날짜와 입력된 날짜 비교
            current_date = pendulum.today(tz="Asia/Seoul").date()
            input_date = pendulum.from_format(date, "YYYY-MM-DD").date()

            # 오늘 + 미래 날짜인 경우 스케줄 쿼리 사용
            if input_date >= current_date:
                query_map = {
                    "arrival": SELECT_AIRPORT_ARRIVAL,
                    "departure": SELECT_AIRPORT_DEPARTURE_SCHEDULE,
                }
            else:
                query_map = {
                    "arrival": SELECT_AIRPORT_ARRIVAL,
                    "departure": SELECT_AIRPORT_DEPARTURE,
                }

            base_query = query_map.get(FLIGHT_IO)

            params = {"airport": airport, "date": date}

            # I/D, 항공사, 터미널
            where_conditions = []
            if condition:
                for con in condition:
                    if con.criteria == "I/D":
                        where_conditions.append("FLIGHT_TYPE = :i_d")
                        params["i_d"] = con.value[0]

                    if con.criteria == "Terminal":
                        where_conditions.append("DEPARTURE_TERMINAL = :terminal")
                        params["terminal"] = con.value[0]

                    if con.criteria == "Airline":
                        where_conditions.append("OPERATING_CARRIER_IATA IN :airline")
                        params["airline"] = tuple(con.value)

                if where_conditions:
                    base_query += " AND " + " AND ".join(where_conditions)

            stmt = text(base_query)

            flight_schedule_data = (
                await self.simulation_repo.fetch_flight_schedule_data(
                    db, stmt, params, FLIGHT_IO
                )
            )
            return flight_schedule_data

        return flight_schedule_data

    async def fetch_show_up_passenger_data(self, scenario_id: str):
        showup_passenger_df = None

        # ======================================================
        # NOTE: S3 데이터 확인
        object_exists = check_s3_object_exists(
            bucket_name="flexa-dev-ap-northeast-2-data-storage",
            object_key=f"simulations/show-up-passenger-data/{scenario_id}.parquet",
        )

        if not object_exists:
            return showup_passenger_df

        # TODO: fetch_flight_schedule_data와 같이 반환 타입 맞추기

        showup_passenger_df = wr.s3.read_parquet(
            path=f"s3://flexa-dev-ap-northeast-2-data-storage/simulations/show-up-passenger-data/{scenario_id}.parquet",
            boto3_session=boto3_session,
        )
        return showup_passenger_df

    async def update_simulation_scenario_target_date(
        self, db: AsyncSession, scenario_id: str, target_date: str
    ):

        target_datetime = datetime.strptime(target_date, "%Y-%m-%d")

        await self.simulation_repo.update_simulation_scenario_target_date(
            db, scenario_id, target_datetime
        )

    async def _create_flight_schedule_chart(
        self, flight_df: pd.DataFrame, group_column: str
    ):

        flight_df["scheduled_gate_departure_local"] = pd.to_datetime(
            flight_df["scheduled_gate_departure_local"]
        ).dt.floor("h")

        df_grouped = (
            flight_df.groupby(["scheduled_gate_departure_local", group_column])
            .size()
            .unstack(fill_value=0)
        )
        df_grouped = df_grouped.sort_index()

        # NOTE: [Vincent] 아래와 같은 케이스에 대응하기 위해서 추가함
        # df_grouped = Empty DataFrame
        # Columns: []
        # Index: []
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

    async def generate_flight_schedule(
        self,
        db: Connection,
        date: str,
        airport: str,
        condition: list | None,
        scenario_id: str,
    ):
        """
        최초에 불러올때는 add condition에 사용될 데이터와, 그래프를 만들때 사용할 데이터를 둘다 챙겨야함
        그 이후에는 add condition이 제공이 된다면, 그래프 데이터만 제공하면 된다.
        만약 컨디션을 사용하지 않은 상태에서 진행을 한다면... first_load를 보고 구분한다.

        현재 출도착은 출발을 고정으로 가져온다.
        """

        # ==============================================================
        # NOTE: SNOWFLAKE 데이터 조회
        flight_schedule_data = await self.fetch_flight_schedule_data(
            db, date, airport, condition, scenario_id, storage="snowflake"
        )

        # ==============================================================
        # NOTE: S3에 데이터 저장
        wr.s3.to_parquet(
            df=pd.DataFrame(flight_schedule_data),
            path=f"s3://flexa-dev-ap-northeast-2-data-storage/simulations/flight-schedule-data/{scenario_id}.parquet",
        )

        # ==============================================================
        # NOTE: 데이터 전처리
        flight_df = pd.DataFrame(flight_schedule_data)

        # Condition
        # I/D, 항공사, 터미널
        i_d = flight_df["flight_type"].unique().tolist()
        terminal = flight_df["departure_terminal"].unique().tolist()
        airline = []
        airline_df = flight_df.drop_duplicates(
            subset=["operating_carrier_iata", "operating_carrier_name"]
        )
        for _, row in airline_df.iterrows():
            item = {
                "iata": row["operating_carrier_iata"],
                "name": row["operating_carrier_name"],
            }
            airline.append(item)

        if None in terminal:
            terminal = [t for t in terminal if t is not None]

        add_conditions = [
            {"name": "I/D", "operator": ["="], "value": i_d},
            {"name": "Airline", "operator": ["is in"], "value": airline},
            {"name": "Terminal", "operator": ["="], "value": terminal},
        ]

        # Prioirties
        # ["Airline", "I/D", "Region", "Country"]
        country = flight_df["country_code"].unique().tolist()
        region = flight_df["region_name"].unique().tolist()

        add_priorities = [
            {"name": "I/D", "operator": ["="], "value": i_d},
            {"name": "Airline", "operator": ["is in"], "value": airline},
            {"name": "Country", "operator": ["is in"], "value": country},
            {"name": "Region", "operator": ["is in"], "value": region},
        ]

        chart_result = {}
        for group_column in [
            "operating_carrier_name",  # 항공사 명으로 통일
            "departure_terminal",
            "flight_type",
        ]:
            chart_data = await self._create_flight_schedule_chart(
                flight_df, group_column
            )

            if chart_data:
                chart_result[CRITERIA_MAP[group_column]] = chart_data["traces"]

        return {
            "total": flight_df.shape[0],
            "add_conditions": add_conditions,
            "add_priorities": add_priorities,
            "chart_x_data": chart_data["default_x"],
            "chart_y_data": chart_result,
        }

    async def _calculate_show_up_pattern(
        self, data: list, destribution_conditions: list
    ):

        df = pd.DataFrame(data)
        pax_df = pd.DataFrame()

        for filter in destribution_conditions:
            partial_df = df.copy()
            for condition in filter.conditions:
                partial_df = partial_df[
                    partial_df[COL_FILTER_MAP[condition.criteria]].isin(condition.value)
                ]
            partial_pax_df = partial_df.loc[
                partial_df.index.repeat(
                    (partial_df["total_seat_count"] * 0.85).astype(int)
                )
            ]
            arrival_times = []
            for _, row in partial_df.iterrows():
                samples = np.random.normal(
                    loc=filter.mean,
                    scale=filter.standard_deviation,
                    size=int(row["total_seat_count"] * 0.85),
                )
                arrival_times.extend(samples)
            partial_pax_df["show_up_time"] = pd.to_datetime(
                partial_pax_df["scheduled_gate_departure_local"]
            ) - pd.to_timedelta(arrival_times, unit="minutes")
            partial_pax_df["show_up_time"] = partial_pax_df["show_up_time"].dt.round(
                "s"
            )
            pax_df = pd.concat([pax_df, partial_pax_df], ignore_index=True)
            df.drop(index=partial_df.index, inplace=True)

        # pax_df.to_csv(".idea/pax_df.csv", index=False) # 분산처리 데이터검증목적
        return pax_df

    async def _create_normal_distribution(self, destribution_conditions: list):
        distribution_xy_coords = []
        for condition in destribution_conditions:
            index = condition.index
            mean = condition.mean
            std_dev = condition.standard_deviation
            x = np.linspace(mean - 4 * std_dev, mean + 4 * std_dev, 1000)
            y = (1 / (std_dev * np.sqrt(2 * np.pi))) * np.exp(
                -0.5 * ((x - mean) / std_dev) ** 2
            )

            dist_name = f"Priority{index+1}"
            if index == 9999:
                dist_name = "Default"

            distribution_xy_coords.append(
                {"name": dist_name, "x": x.tolist(), "y": y.tolist()}
            )
        return distribution_xy_coords

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

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq=time_unit,
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        group_order = df_grouped.sum().sort_values(ascending=False).index.tolist()
        if has_etc and "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

        default_x = df_grouped.index.strftime("%Y-%m-%d %H:%M:%S").tolist()

        traces = [
            {
                "name": column,
                "order": group_order.index(column),
                "y": df_grouped[column].tolist(),
            }
            for column in df_grouped.columns
        ]

        return {"default_x": default_x, "traces": traces}

    async def generate_passenger_schedule(
        self,
        db: Connection,
        flight_sch: dict,
        destribution_conditions: list,
        scenario_id: str,
    ):
        flight_schedule_data = await self.fetch_flight_schedule_data(
            db=db,
            date=flight_sch.date,
            airport=flight_sch.airport,
            condition=flight_sch.condition,
            scenario_id=scenario_id,
        )
        flight_schedule_df = pd.DataFrame(flight_schedule_data)

        # ==============================================================
        # NOTE: ///
        average_seats = flight_schedule_df["total_seat_count"].mean()
        total_flights = len(flight_schedule_df)
        total_sub_result = [
            {
                "title": "Flights",
                "value": f"{total_flights:,.0f}",
                "unit": None,
            },
            {
                "title": "Average Seats per Flight",
                "value": f"{round(average_seats, 2)}",
                "unit": None,
            },
            {
                "title": "Load factor",
                "value": "85",
                "unit": "%",
            },
        ]

        # ==============================================================
        # NOTE: ///
        distribution_xy_coords = await self._create_normal_distribution(
            destribution_conditions
        )

        # ==============================================================
        # NOTE: 여객 데이터 생성
        pax_df = await self._calculate_show_up_pattern(
            flight_schedule_data, destribution_conditions
        )
        # NOTE: S3에 데이터 저장
        wr.s3.to_parquet(
            df=pax_df,
            path=f"s3://flexa-dev-ap-northeast-2-data-storage/simulations/show-up-passenger-data/{scenario_id}.parquet",
        )

        # ==============================================================
        # NOTE: 차트 데이터 생성
        chart_result = {}
        for group_column in [
            "operating_carrier_name",
            "departure_terminal",
            "country_code",
            "region_name",
        ]:
            chart_data = await self._create_show_up_summary(pax_df, group_column)
            chart_result[CRITERIA_MAP[group_column]] = chart_data["traces"]

        return {
            "total": pax_df.shape[0],
            "total_sub_obj": total_sub_result,
            "dst_chart": distribution_xy_coords,
            "bar_chart_x_data": chart_data["default_x"],
            "bar_chart_y_data": chart_result,
        }

    # FIXME: 운영세팅이 완료되면 변경될 코드
    async def fetch_processing_procedures(self):
        return await self.simulation_repo.fetch_processing_procedures()

    def _calculate_sample_node(self, row, edited_df):
        """
        Sample a node based on the probabilities from the choice matrix.
        :param row: The row of the DataFrame to sample from
        :param edited_df: The choice matrix DataFrame with probabilities
        :return: The sampled node
        """
        probabilities = edited_df.loc[row]
        return np.random.choice(probabilities.index, p=probabilities.values)

    async def _create_facility_conn_sankey(self, facility_detail, pax_df):
        sankey_data = {"label": [], "link": {"source": [], "target": [], "value": []}}
        node_ids = set()  # 중복 제거를 위한 세트

        for key, value in list(facility_detail.items())[1:]:
            # 노드 추가
            for node in value.nodes:
                if node not in node_ids:
                    sankey_data["label"].append(node)
                    node_ids.add(node)

        for key, value in list(facility_detail.items())[1:]:
            # 링크 추가
            source_nodes = facility_detail[key].nodes
            destination = facility_detail[key].destination
            if destination:
                source_name = facility_detail[key].name
                destination_name = facility_detail[destination].name

                target_nodes = facility_detail[destination].nodes
                for source_node in source_nodes:
                    for target_node in target_nodes:

                        value = pax_df[
                            (pax_df[f"{source_name}_component"] == source_node)
                            & (pax_df[f"{destination_name}_component"] == target_node)
                        ]

                        sankey_data["link"]["source"].append(
                            sankey_data["label"].index(source_node)
                        )
                        sankey_data["link"]["target"].append(
                            sankey_data["label"].index(target_node)
                        )
                        sankey_data["link"]["value"].append(len(value))

        return sankey_data

    async def _create_capacity_chart(self, df_pax, process, node_list):
        times = [
            time(hour=hour, minute=minute)
            for hour in range(24)
            for minute in range(0, 60, 10)
        ]
        time_df = pd.DataFrame({"index": times})
        time_df["index"] = time_df["index"].astype(str)

        node_data = {}

        for node in node_list:
            df_pax_filtered = df_pax[df_pax[f"{process}_component"] == node].copy()
            df_pax_filtered.loc[:, "show_up_time"] = df_pax_filtered[
                "show_up_time"
            ].dt.floor("10min")

            grouped = df_pax_filtered["show_up_time"].value_counts().reset_index()
            grouped["index"] = grouped["show_up_time"].astype(str).str[-8:]

            total_capa = pd.merge(time_df, grouped, on="index", how="left")
            total_capa["count"] = total_capa["count"].fillna(0)
            total_capa = total_capa.drop("show_up_time", axis=1)

            node_data[node] = {
                "total": int(total_capa["count"].sum()),
                "y": total_capa["count"].tolist(),
            }

        total_data = {
            "bar_chart_x_data": total_capa["index"].tolist(),
            "bar_chart_y_data": node_data,
        }

        return total_data

    async def _calculate_add_columns(self, facility_detail, df_pax):
        """
        Add columns to the DataFrame based on the choice matrices for each process transition.
        :param return_dict: Dictionary containing the choice matrices and the passenger DataFrame
        :return: Updated return_dict with added columns to the passenger DataFrame
        """

        # Iterate over each process transition in the choice matrix
        for facility in list(facility_detail.keys())[1:]:

            source_num = facility_detail[facility].source

            vertical_process = facility_detail[
                source_num
            ].name  # 시작점 ex. operating_carrier_iata
            vertical_process = COL_FILTER_MAP.get(vertical_process, vertical_process)

            horizontal_process = facility_detail[facility].name  # 도착점 ex. check-in

            # 매트릭스 테이블 df 만들기
            default_matrix_df = pd.DataFrame.from_dict(
                facility_detail[facility].default_matrix, orient="index"
            )

            # 매트릭스로 새로운 컬럼 생성
            if facility == "1":  # is root
                df_pax[f"{horizontal_process}_component"] = df_pax[
                    vertical_process
                ].apply(
                    self._calculate_sample_node, args=(default_matrix_df.fillna(0),)
                )
            else:
                df_pax[f"{horizontal_process}_component"] = df_pax[
                    f"{vertical_process}_component"
                ].apply(
                    self._calculate_sample_node, args=(default_matrix_df.fillna(0),)
                )

            if facility_detail[facility].priority_matrix:
                for priority_matrix in facility_detail[facility].priority_matrix:
                    priority_matrix_df = pd.DataFrame.from_dict(
                        priority_matrix.matrix, orient="index"
                    )

                    condition = pd.Series(True, index=df_pax.index)
                    for con in priority_matrix.condition:
                        if COL_FILTER_MAP.get(con.criteria, None):
                            condition = condition & (
                                df_pax[COL_FILTER_MAP[con.criteria]].isin(con.value)
                            )

                    if facility == "1":  # is root
                        df_pax.loc[
                            condition, f"{horizontal_process}_component"
                        ] = df_pax.loc[condition, vertical_process].apply(
                            self._calculate_sample_node,
                            args=(priority_matrix_df.fillna(0),),
                        )
                    # else:
                    #     df_pax.loc[
                    #         condition, f"{horizontal_process}_component"
                    #     ] = df_pax.loc[
                    #         condition, f"{vertical_process}_component"
                    #     ].apply(
                    #         self._calculate_sample_node,
                    #         args=(priority_matrix_df.fillna(0),),
                    #     )
                    else:
                        for idx, row in df_pax.loc[condition].iterrows():
                            previous_value = row[f"{vertical_process}_component"]

                            # 이전 값이 매트릭스의 인덱스에 존재하는 경우에만 업데이트
                            if previous_value in priority_matrix_df.index.tolist():
                                df_pax.at[idx, f"{horizontal_process}_component"] = (
                                    self._calculate_sample_node(
                                        previous_value, priority_matrix_df.fillna(0)
                                    )
                                )

        return df_pax

    async def generate_facility_conn(self, processes: dict, scenario_id: str):
        pax_df = await self.fetch_show_up_passenger_data(scenario_id=scenario_id)
        pax_df = await self._calculate_add_columns(processes, pax_df)

        capacity = {}
        for process in list(processes.values())[1:]:
            capacity_data = await self._create_capacity_chart(
                pax_df, process=process.name, node_list=process.nodes
            )
            capacity[process.name] = capacity_data

        return capacity

    async def _calculate_capacity(self, facility_schedule: list, time_unit: int):
        by_pass = 1e-10

        if by_pass in facility_schedule:
            return None

        return sum(
            1 / (schedule / 60) * time_unit if schedule != 0 else 0
            for schedule in facility_schedule
        )

    async def generate_set_opening_hours(self, facility_info):

        time_list = [
            (datetime(2023, 1, 1, 0, 0) + timedelta(minutes=10 * i)).strftime(
                "%H:%M:%S"
            )
            for i in range(144)
        ]

        data_list = [
            await self._calculate_capacity(facility_schedule, facility_info.time_unit)
            for facility_schedule in facility_info.facility_schedules
        ]

        non_none_data = [data for data in data_list if data is not None]
        max_data = max(non_none_data) if non_none_data else 0

        data_list = [data if data is not None else max_data * 2 for data in data_list]

        return {"x": time_list, "y": data_list}

    async def generate_simulation_overview(
        self,
        db: Connection,
        flight_sch: dict,
        destribution_conditions: list,
        processes: dict,
        components: list,
        scenario_id: str,
    ):
        flight_schedule_data = await self.fetch_flight_schedule_data(
            db,
            flight_sch.date,
            flight_sch.airport,
            flight_sch.condition,
            scenario_id=scenario_id,
        )

        pax_df = await self.fetch_show_up_passenger_data(scenario_id=scenario_id)
        pax_df = await self._calculate_add_columns(processes, pax_df)

        # ===========================================================
        # NOTE: Overview 데이터 생성
        pax_df["passenger_pattern"] = (
            pax_df["scheduled_gate_departure_local"] - pax_df["show_up_time"]
        ).dt.total_seconds() / 60

        passenger_pattern = pax_df["passenger_pattern"].mean()

        matric = [
            {"name": "Date", "value": flight_sch.date},
            {
                "name": "Terminal",
                "value": None,
            },
            {
                "name": "Analysis Type",
                "value": "Departure Passengers",
            },
            {
                "name": "Data Source",
                "value": "Cirium",
            },
            {
                "name": "Flights",
                "value": f"{len(flight_schedule_data):,} flights",
            },
            {
                "name": "Passengers",
                "value": f"{len(pax_df):,} pax",
            },
            {
                "name": "Pax Show-up Pattern",
                "value": f"{round(passenger_pattern):,} mins",
            },
            {
                "name": "Generation Method",
                "value": "Normal Distribution",
            },
        ]

        for component in components:
            name: str = component.name
            name = name.replace("_", " ").title().replace("Check In", "Check-In")

            value = []
            node_count = len(component.nodes)
            value.append(f"{node_count:,} Nodes")

            facility_count = 0
            for node in component.nodes:
                facility_count += node.facility_count
            value.append(f"{facility_count:,} Facilities")

            matric.append({"name": name, "value": value})

        # ===========================================================
        # NOTE: Sankey 차트 데이터 생성
        sanky = await self._create_facility_conn_sankey(processes, pax_df)

        return {"matric": matric, "sanky": sanky}

    async def _create_time_range(self, date: str):
        """
        서브 함수
        시뮬레이션 차트에 나올 시간 범위 생성

        Args:
            date: flight schedule에서 가져온 date

        Return:
            pd.date_range
        """

        date_obj = datetime.strptime(date, "%Y-%m-%d")

        day_before = date_obj - timedelta(days=1)
        day_after = date_obj + timedelta(days=1)

        day_before_str = day_before.strftime("%Y-%m-%d")
        day_after_str = day_after.strftime("%Y-%m-%d")

        time_range = pd.date_range(
            start=pd.Timestamp(f"{day_before_str} 20:00:00"),
            end=pd.Timestamp(f"{day_after_str} 01:00:00"),
            freq="10min",
        )

        return time_range

    async def _create_simulation_queue_chart_optimized(
        self, sim_df: pd.DataFrame, process, group_column, time_range
    ):
        """
        서브 함수
        explode 큐 데이터를 만드는 함수 (최적화 버전)

        Args:
            time_range: _create_time_range 함수에서 가져온 데이터
        """

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"

        df = sim_df[[start_time, end_time, group_column]].copy()
        df[start_time] = df[start_time].dt.round("10min")
        df[end_time] = df[end_time].dt.round("10min")

        global_start = df[start_time].min()
        global_end = df[end_time].max()
        all_times = pd.date_range(start=global_start, end=global_end, freq="10min")

        group_list = df[group_column].unique()
        group_counts = pd.DataFrame(0, index=all_times, columns=group_list)

        for group, sub_df in df.groupby(group_column):

            start_idx = np.searchsorted(all_times, sub_df[start_time].values)
            end_idx = np.searchsorted(all_times, sub_df[end_time].values, side="right")

            diff_array = np.zeros(len(all_times) + 1, dtype=int)
            np.add.at(diff_array, start_idx, 1)
            np.add.at(diff_array, end_idx, -1)

            counts = np.cumsum(diff_array)[:-1]
            group_counts[group] = counts

        group_counts = group_counts.reindex(time_range, fill_value=0)
        total_groups = group_counts.shape[1]
        if total_groups > 9:
            top_9 = group_counts.sum().nlargest(9).index.tolist()
            group_counts["etc"] = group_counts.drop(columns=top_9).sum(axis=1)
            group_counts = group_counts[top_9 + ["etc"]]

        group_order = group_counts.sum().sort_values(ascending=False).index.tolist()
        group_order = [g for g in group_order if pd.notna(g)]

        if "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

        default_x = group_counts.index.strftime("%Y-%m-%d %H:%M:%S").tolist()
        traces = [
            {
                "name": col,
                "order": group_order.index(col),
                "y": group_counts[col].tolist(),
            }
            for col in group_counts.columns
            if pd.notna(col)
        ]

        return {"traces": traces, "default_x": default_x}

    async def _create_simulation_queue_chart(
        self, sim_df: pd.DataFrame, process, node, group_column, time_range
    ):
        """
        서브 함수
        explode 큐 데이터를 만드는 함수
        """

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"

        df = sim_df.loc[sim_df[f"{process}_pred"] == f"{process}_{node}"].copy()

        df_expanded = df[[start_time, end_time, group_column]].copy()
        df_expanded[start_time] = df_expanded[start_time].dt.round("10min")
        df_expanded[end_time] = df_expanded[end_time].dt.round("10min")

        df_expanded["start_int"] = (
            (
                (
                    df_expanded[start_time] - df_expanded[start_time].min()
                ).dt.total_seconds()
                // 60
            )
            .fillna(0)
            .astype(int)
        )
        df_expanded["end_int"] = (
            (
                (
                    df_expanded[end_time] - df_expanded[start_time].min()
                ).dt.total_seconds()
                // 60
            )
            .fillna(0)
            .astype(int)
        )
        df_expanded["Time"] = df_expanded.apply(
            lambda row: list(range(row["start_int"], row["end_int"], 10)),
            axis=1,  # 10분간격으로 설정
        )

        df_expanded = df_expanded.explode("Time")
        df_expanded["Time"] = df_expanded[start_time] + pd.to_timedelta(
            df_expanded["Time"] - df_expanded["start_int"], unit="m"
        )

        # 하루를 넘어가거나, 하루 이전의 값을 사전 제거
        df_expanded = df_expanded[df_expanded["Time"].isin(time_range)]

        # df_grouped 만들기
        df_grouped = (
            df_expanded.groupby(["Time", group_column])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )
        df_grouped = df_grouped.set_index("Time")
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

        df_grouped = df_grouped.reindex(time_range, fill_value=0)

        group_order = df_grouped.sum().sort_values(ascending=False).index.tolist()
        if has_etc and "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

        default_x = df_grouped.index.strftime("%Y-%m-%d %H:%M:%S").tolist()

        traces = [
            {
                "name": column,
                "order": group_order.index(column),
                "y": df_grouped[column].tolist(),
            }
            for column in df_grouped.columns
        ]

        return {"traces": traces, "default_x": default_x}

    async def _create_queue_length(
        self, sim_df: pd.DataFrame, process, node, group_column, time_range
    ):
        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"
        process_que = f"{process}_que"

        sim_df[start_time] = pd.to_datetime(sim_df[start_time])
        sim_df[end_time] = pd.to_datetime(sim_df[end_time])

        df = sim_df.loc[sim_df[f"{process}_pred"] == f"{process}_{node}"].copy()
        df = df[[start_time, end_time, group_column, process_que]].copy()

        df.loc[:, end_time] = df[end_time].dt.floor("10min")

        df_grouped = (
            df.groupby([end_time, group_column], as_index=False)
            .agg({process_que: "mean"})
            .pivot_table(
                index=end_time,
                columns=group_column,
                values=process_que,
                fill_value=0,
            )
        )

        df_grouped = df_grouped.reindex(time_range, fill_value=0)

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

        df_grouped = df_grouped.reindex(time_range, fill_value=0)

        group_order = df_grouped.sum().sort_values(ascending=False).index.tolist()
        if has_etc and "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

        default_x = df_grouped.index.strftime("%Y-%m-%d %H:%M:%S").tolist()

        traces = [
            {
                "name": column,
                "order": group_order.index(column),
                "y": df_grouped[column].map(round).tolist(),
            }
            for column in df_grouped.columns
        ]
        print(traces)

        return {"traces": traces, "default_x": default_x}

    async def _create_waiting_time(
        self, sim_df: pd.DataFrame, process, node, time_range
    ) -> pd.DataFrame:
        """
        서브 함수
        waiting time 데이터 생성

        Args:
            time_range: _create_time_range 함수에서 가져온 데이터
        """

        start_time = f"{process}_on_pred"
        end_time = f"{process}_done_pred"

        df = sim_df.loc[sim_df[f"{process}_pred"] == f"{process}_{node}"].copy()

        df[start_time] = pd.to_datetime(df[start_time])
        df[end_time] = pd.to_datetime(df[end_time])
        df = df[[start_time, end_time]]

        df["waiting_time"] = (df[end_time] - df[start_time]).dt.total_seconds()

        df.loc[:, start_time] = df[start_time].dt.floor("10min")
        df_grouped = df.groupby([start_time], as_index=start_time).agg(
            {"waiting_time": "mean"}
        )
        df_grouped["waiting_time"] = df_grouped["waiting_time"] / 60
        df_grouped = df_grouped.reindex(time_range, fill_value=None)
        wt_x_list = df_grouped.index.astype(str).tolist()
        wt_y_list = [
            int(x) if pd.notna(x) else None for x in df_grouped["waiting_time"].tolist()
        ]

        return {"y": wt_y_list, "default_x": wt_x_list}

    async def _create_simulation_flow_chart(
        self,
        sim_df: pd.DataFrame,
        flow: str,
        process: str,
        node: str,
        group_column: str,
        time_range,
    ):
        """
        서브 함수
        inflow outflow 데이터 생성

        Args:
            flow: [on, pt]로 구분되며 inflow인지 outflow인지 구분
            time_range: _create_time_range 함수에서 가져온 데이터
        """

        sim_df = sim_df.loc[sim_df[f"{process}_pred"] == f"{process}_{node}"].copy()

        sim_df.loc[:, f"{process}_{flow}_pred"] = sim_df[
            f"{process}_{flow}_pred"
        ].dt.floor("10min")

        df_grouped = (
            sim_df.groupby([f"{process}_{flow}_pred", group_column])
            .size()
            .unstack(fill_value=0)
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

        df_grouped = df_grouped.reindex(time_range, fill_value=0)

        group_order = df_grouped.sum().sort_values(ascending=False).index.tolist()
        if has_etc and "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

        default_x = df_grouped.index.strftime("%Y-%m-%d %H:%M:%S").tolist()

        traces = [
            {
                "name": column,
                "order": group_order.index(column),
                "y": df_grouped[column].tolist(),
            }
            for column in df_grouped.columns
        ]

        return {"traces": traces, "default_x": default_x}

    async def _generate_simulation_charts_node(
        self,
        process: str,
        node: str,
        sim_df: pd.DataFrame,
        date: str,
    ):
        """
        메인 함수
        각 노드 별로 차트 데이터를 생성

        Args:
            date: flight schedule에서 가져온 date
        """

        # 시간 범위 설정
        time_range = await self._create_time_range(date)

        # color criteria
        color_criteria_options = [
            "operating_carrier_name",
            "departure_terminal",
            "country_code",
            "region_name",
        ]

        # 생성될 데이터들이 담길 딕셔너리
        inbound = {}
        outbound = {}
        queing = {}

        waiting_data = await self._create_waiting_time(
            sim_df=sim_df,
            process=process,
            node=node,
            time_range=time_range,
        )

        # EX. process = checkin / node = A / flow = on
        for group_column in color_criteria_options:

            queue_data = await self._create_simulation_queue_chart(
                sim_df=sim_df,
                process=process,
                node=node,
                group_column=group_column,
                time_range=time_range,
            )
            queing[CRITERIA_MAP[group_column]] = queue_data["traces"]

            for flow in ["on", "pt"]:
                # process = checkin / node = A / flow = on / group_column = operating_carrier_name
                flow_data = await self._create_simulation_flow_chart(
                    sim_df,
                    flow=flow,
                    process=process,
                    node=node,
                    group_column=group_column,
                    time_range=time_range,
                )

                if flow == "on":
                    inbound[CRITERIA_MAP[group_column]] = flow_data["traces"]

                if flow == "pt":
                    outbound[CRITERIA_MAP[group_column]] = flow_data["traces"]

        # KPI 생성
        result = {
            "process": process,
            "node": node,
            "inbound": {
                "chart_x_data": flow_data["default_x"],
                "chart_y_data": inbound,
            },
            "outbound": {
                "chart_x_data": flow_data["default_x"],
                "chart_y_data": outbound,
            },
            "queing": {"chart_x_data": queue_data["default_x"], "chart_y_data": queing},
            "waiting": {
                "chart_x_data": waiting_data["default_x"],
                "chart_y_data": waiting_data["y"],
            },
        }

        return result

    async def _generate_simulation_metrics_kpi(
        self,
        process: str,
        node: str,
        sim_df: pd.DataFrame,
    ):
        """
        메인 함수
        각 노드별 KPI Indicator 지표를 생성
        """

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"
        process_time = f"{process}_pt"

        filtered_sim_df = sim_df.loc[
            sim_df[f"{process}_pred"] == f"{process}_{node}"
        ].copy()

        diff_arr = (
            filtered_sim_df[end_time] - filtered_sim_df[start_time]
        ).dt.total_seconds()
        throughput = int(len(filtered_sim_df)) if filtered_sim_df is not None else 0
        total_delay = int((diff_arr.sum() / 60) if diff_arr is not None else 0)
        # max_delay = int((diff_arr.max() / 60) if diff_arr is not None else 0)
        max_delay = int(
            (0 if diff_arr is None or np.isnan(diff_arr.max()) else diff_arr.max() / 60)
        )
        # 평균 지연 시간 (0으로 나눠지는 것 방지)
        if throughput:
            average_delay = int((total_delay / throughput))
        else:
            average_delay = 0

        # 평균 처리 시간
        if filtered_sim_df is not None and process_time in filtered_sim_df.columns:
            avg_time = filtered_sim_df[process_time].mean()
            average_transaction_time = int(avg_time) if pd.notna(avg_time) else 0
        else:
            average_transaction_time = 0

        kpi = [
            {
                "title": "Processed Passengers",
                "value": f"{throughput:,}",
                "unit": "pax",
            },
            {
                "title": "Maximum Wait Time",
                "value": f"{max_delay // 60:02d}:{max_delay % 60:02d}",
                "unit": None,
            },
            {
                "title": "Average Wait Time",
                "value": f"{average_delay // 60:02d}:{average_delay % 60:02d}",
                "unit": None,
            },
            {
                "title": "Average Processing Time",
                "value": f"{average_transaction_time // 60:02d}:{average_transaction_time % 60:02d}",
                "unit": None,
            },
        ]

        return {
            "process": process,
            "node": node,
            "kpi": kpi,
        }

    async def _generate_simulation_charts_total(
        self,
        sim_df: pd.DataFrame,
        total: dict,
    ):
        """
        메인 함수
        모든 노드의 최종 차트 지표를 보여주는 함수

        Args:
            total: comp_to_idx 딕셔너리 필요
            EX. comp_to_idx = {'checkin': {'A': 0, 'B': 1, 'C': 2, 'D': 3}, 'departure_gate': {'DG1': 4, 'DG2': 5}, 'security_check': {'SC1': 6, 'SC2': 7}, 'passport_check': {'PC1': 8, 'PC2': 9}}
        """

        x_data = []
        throughput_data = []
        max_delay_data = []
        average_delay_data = []
        total_delay_data = []
        for process, nodes in total.items():
            for node in nodes.keys():
                x_data.append(node)

                start_time = f"{process}_on_pred"
                end_time = f"{process}_pt_pred"

                filtered_sim_df = sim_df.loc[
                    sim_df[f"{process}_pred"] == f"{process}_{node}"
                ].copy()

                diff_arr = (
                    filtered_sim_df[end_time] - filtered_sim_df[start_time]
                ).dt.total_seconds()
                # throughput = int(len(filtered_sim_df))
                # total_delay = int(diff_arr.sum() / 60)
                # max_delay = int(diff_arr.max() / 60)
                # average_delay = int((total_delay / throughput) * 100) / 100

                throughput = (
                    int(len(filtered_sim_df)) if filtered_sim_df is not None else 0
                )
                total_delay = int((diff_arr.sum() / 60) if diff_arr is not None else 0)
                # max_delay = int((diff_arr.max() / 60) if diff_arr is not None else 0)
                max_delay = int(
                    (
                        0
                        if diff_arr is None or np.isnan(diff_arr.max())
                        else diff_arr.max() / 60
                    )
                )

                # 평균 지연 시간 (0으로 나눠지는 것 방지)
                if throughput:
                    average_delay = int((total_delay / throughput))
                else:
                    average_delay = 0

                throughput_data.append(throughput)
                max_delay_data.append(max_delay)
                average_delay_data.append(average_delay)
                total_delay_data.append(total_delay)

        result = {
            "defalut_x": x_data,
            "throughput": throughput_data,
            "max_delay": max_delay_data,
            "average_delay": average_delay_data,
            "total_delay": total_delay_data,
        }

        return result

    async def _generate_simulation_sankey(
        self, df: pd.DataFrame, component_list, suffix="_pred"
    ) -> dict:
        """
        메인 함수
        생키차트 데이터 생성
        """

        # 프로세스별 고유값과 인덱스 매핑
        nodes = []
        node_dict = {}
        idx = 0

        # 노드 인덱스 생성
        for process in component_list:
            col_name = f"{process}{suffix}"
            df = df[df[col_name] != ""].copy()
            for value in df[col_name].unique():
                if value not in node_dict and pd.notna(value):
                    node_dict[value] = idx
                    # 각 value의 길이를 label로
                    label_count = len(df[df[col_name] == value])
                    nodes.append(f"{value} ({label_count:,})")
                    idx += 1

        # source, target, value 생성
        sources, targets, values = [], [], []
        for i in range(len(component_list) - 1):
            source_col = f"{component_list[i]}{suffix}"
            target_col = f"{component_list[i+1]}{suffix}"

            flow = df.groupby([source_col, target_col]).size().reset_index()
            for _, row in flow.iterrows():
                if pd.notna(row[source_col]) and pd.notna(row[target_col]):
                    sources.append(node_dict[row[source_col]])
                    targets.append(node_dict[row[target_col]])
                    values.append(row[0])

        sankey = {
            "label": nodes,
            "link": {"source": sources, "target": targets, "value": values},
        }

        return sankey

    async def execute_simulation_by_scenario(
        self,
        schedule_date: str,
        scenario_id: str,
        components: list,
        processes: dict,
        background_tasks: BackgroundTasks,
    ):
        # ====================================================================
        # NOTE: S3에 데이터 저장
        bucket_name = "flexa-dev-ap-northeast-2-data-storage"
        object_key = f"simulations/facility-information-data/{scenario_id}.json"

        # TODO: SQS처럼 infra폴더에 별도로 모듈화
        s3 = boto3.client(
            "s3",
            config=Config(region_name="ap-northeast-2"),
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )
        s3.put_object(
            ContentType="application/json",
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps({"components": components, "processes": processes}),
        )

        # ====================================================================
        # NOTE: SQS에 메시지 전송
        background_tasks.add_task(
            send_message_to_sqs,
            queue_url=os.getenv("AWS_SQS_URL"),
            message_body={"schedule_date": schedule_date, "scenario_id": scenario_id},
        )

        return {"status": "success", "message": "Simulation started successfully."}

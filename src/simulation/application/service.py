import asyncio
from datetime import datetime, time
from typing import List, Union

import boto3
import numpy as np
import pandas as pd
from dependency_injector.wiring import inject
from fastapi import WebSocket
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncSession
from ulid import ULID

from src.common import TimeStamp
from src.constants import COL_FILTER_MAP, CRITERIA_MAP
from src.simulation.application.core.graph import DsGraph
from src.simulation.application.core.ouput_wrapper import DsOutputWrapper
from src.simulation.application.core.simulator import DsSimulator
from src.simulation.application.queries import (
    SELECT_AIRPORT_ARRIVAL,
    SELECT_AIRPORT_DEPARTURE,
)
from src.simulation.domain.repository import ISimulationRepository
from src.simulation.domain.simulation import ScenarioMetadata, SimulationScenario


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
    def __init__(
        self,
        simulation_repo: ISimulationRepository,
    ):
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

    async def create_simulation_scenario(
        self,
        db: AsyncSession,
        user_id: str,
        name: str,
        memo: str,
        terminal: str,
        editor: str,
    ):
        id = str(ULID())

        simulation_scenario: SimulationScenario = SimulationScenario(
            id=id,
            user_id=user_id,
            simulation_url=None,
            simulation_name=name,
            size=None,
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
        history: dict | None,
        flight_sch: dict | None,
        passenger_sch: dict | None,
        passenger_attr: dict | None,
        facility_conn: dict | None,
        facility_info: dict | None,
    ):

        if history:
            history["modification_date"] = self.timestamp.time_now()

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

        await self.simulation_repo.update_scenario_metadata(db, scenario_metadata)

    # =====================================
    # NOTE: 시뮬레이션 프로세스

    async def fetch_flight_schedule_data_test(
        self, db: Connection, date: str, airport: str, condition: list | None
    ):

        flight_io = "departure"
        query_map = {
            "arrival": SELECT_AIRPORT_ARRIVAL,
            "departure": SELECT_AIRPORT_DEPARTURE,
        }
        base_query = query_map.get(flight_io)

        params = {
            "airport": airport,
            "date": date,
        }

        # I/D, 항공사, 터미널
        where_conditions = []
        if condition:
            for con in condition:
                if con.criteria == "I/D":
                    where_conditions.append("FLIGHT_TYPE = :i_d")
                    params["i_d"] = con.value[0]

                if con.criteria == "Terminal":
                    where_conditions.append("F.departure_terminal = :terminal")
                    params["terminal"] = con.value[0]

                if con.criteria == "Airline":
                    where_conditions.append("F.OPERATING_CARRIER_IATA IN :airline")
                    params["airline"] = tuple(con.value)

            if where_conditions:
                base_query += " AND " + " AND ".join(where_conditions)

        stmt = text(base_query + "AND F.OPERATING_CARRIER_IATA = 'KE' LIMIT 10")

        data = await self.simulation_repo.fetch_flight_schedule_data(
            db, stmt, params, flight_io
        )

        return data

    async def fetch_flight_schedule_data(
        self, db: Connection, date: str, airport: str, condition: list | None
    ):

        flight_io = "departure"
        query_map = {
            "arrival": SELECT_AIRPORT_ARRIVAL,
            "departure": SELECT_AIRPORT_DEPARTURE,
        }
        base_query = query_map.get(flight_io)

        params = {
            "airport": airport,
            "date": date,
        }

        # I/D, 항공사, 터미널
        where_conditions = []
        if condition:
            for con in condition:
                if con.criteria == "I/D":
                    where_conditions.append("FLIGHT_TYPE = :i_d")
                    params["i_d"] = con.value[0]

                if con.criteria == "Terminal":
                    where_conditions.append("F.departure_terminal = :terminal")
                    params["terminal"] = con.value[0]

                if con.criteria == "Airline":
                    where_conditions.append("F.OPERATING_CARRIER_IATA IN :airline")
                    params["airline"] = tuple(con.value)

            if where_conditions:
                base_query += " AND " + " AND ".join(where_conditions)

        stmt = text(base_query)

        data = await self.simulation_repo.fetch_flight_schedule_data(
            db, stmt, params, flight_io
        )

        return data

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
        first_load: bool,
    ):
        """
        최초에 불러올때는 add condition에 사용될 데이터와, 그래프를 만들때 사용할 데이터를 둘다 챙겨야함
        그 이후에는 add condition이 제공이 된다면, 그래프 데이터만 제공하면 된다.
        만약 컨디션을 사용하지 않은 상태에서 진행을 한다면... first_load를 보고 구분한다.

        현재 출도착은 출발을 고정으로 가져온다.
        """

        data = await self.fetch_flight_schedule_data(db, date, airport, condition)
        flight_df = pd.DataFrame(data)

        add_conditions = None
        if first_load:
            # I/D, 항공사, 터미널
            i_d = flight_df["flight_type"].unique().tolist()
            airline = flight_df["operating_carrier_iata"].unique().tolist()
            terminal = flight_df["departure_terminal"].unique().tolist()

            if None in terminal:
                terminal = [t for t in terminal if t is not None]

            add_conditions = [
                {"name": "I/D", "operator": ["="], "value": i_d},
                {"name": "Airline", "operator": ["is in"], "value": airline},
                {"name": "Terminal", "operator": ["="], "value": terminal},
            ]

        add_priorities = None
        # ["Airline", "I/D", "Region", "Country"]
        i_d = flight_df["flight_type"].unique().tolist()
        airline = flight_df["operating_carrier_iata"].unique().tolist()
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

            chart_result[CRITERIA_MAP[group_column]] = chart_data["traces"]

        return {
            "add_conditions": add_conditions,
            "add_priorities": add_priorities,
            "total": flight_df.shape[0],
            "chart_x_data": chart_data["default_x"],
            "chart_y_data": chart_result,
        }

    # =====================================
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
                partial_df.index.repeat(partial_df["total_seat_count"])
            ]
            arrival_times = []
            for _, row in partial_df.iterrows():
                samples = np.random.normal(
                    loc=filter.mean,
                    scale=np.sqrt(filter.standard_deviation),
                    size=int(row["total_seat_count"]),
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
        pax_df["show_up_time"] = pax_df["show_up_time"].dt.floor("h")
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
            freq="h",
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
        self, db: Connection, flight_sch: dict, destribution_conditions: list
    ):

        data = await self.fetch_flight_schedule_data(
            db,
            flight_sch.date,
            flight_sch.airport,
            flight_sch.condition,
        )
        df = pd.DataFrame(data)
        total_flights = len(df)
        average_seats = df["total_seat_count"].mean()

        pax_df = await self._calculate_show_up_pattern(data, destribution_conditions)

        distribution_xy_coords = await self._create_normal_distribution(
            destribution_conditions
        )

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
            "total_sub": f"Flight({total_flights}) x Average_seats({average_seats}) x Load_factor(85.0%)",
            "dst_chart": distribution_xy_coords,
            "bar_chart_x_data": chart_data["default_x"],
            "bar_chart_y_data": chart_result,
        }

    # =====================================
    # FIXME: 운영세팅이 완료되면 변경될 코드
    async def fetch_processing_procedures(self):
        import json
        import os

        sample_data = os.path.join(
            os.getcwd(), "/code/samples/sample_processing_procedures.json"
        )

        with open(
            sample_data,
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)

        return data

    # =====================================
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
                "total": total_capa["count"].sum(),
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
                    else:
                        df_pax.loc[
                            condition, f"{horizontal_process}_component"
                        ] = df_pax.loc[
                            condition, f"{vertical_process}_component"
                        ].apply(
                            self._calculate_sample_node,
                            args=(priority_matrix_df.fillna(0),),
                        )

        return df_pax

    async def generate_facility_conn(
        self,
        db: Connection,
        flight_sch: dict,
        destribution_conditions: list,
        processes: dict,
    ):
        data = await self.fetch_flight_schedule_data(
            db, flight_sch.date, flight_sch.airport, flight_sch.condition
        )

        pax_df = await self._calculate_show_up_pattern(data, destribution_conditions)
        pax_df = await self._calculate_add_columns(processes, pax_df)

        sanky = await self._create_facility_conn_sankey(processes, pax_df)
        capacity = await self._create_capacity_chart(
            pax_df, process=processes["1"].name, node_list=processes["1"].nodes
        )

        pax_df["passenger_pattern"] = (
            pax_df["scheduled_gate_departure_local"] - pax_df["show_up_time"]
        ).dt.total_seconds() / 60
        passenger_pattern = pax_df["passenger_pattern"].mean()

        standard_deviations = []
        for condition in destribution_conditions:
            standard_deviations.append(condition.standard_deviation)
        standard_deviation = sum(standard_deviations) / len(standard_deviations)

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
                "value": f"{len(data)} flights",
            },
            {
                "name": "Passengers",
                "value": f"{len(pax_df)} pax",
            },
            {
                "name": "Passengers Pattern",
                "value": f"AVG {round(passenger_pattern)}mins Dist {round(standard_deviation)}mins",
            },
            {
                "name": "Generation Method",
                "value": "Normal Distribution",
            },
        ]

        for key in list(processes.keys())[1:]:
            name: str = processes[key].name
            name = name.replace("_", " ").title().replace("Check In", "Check-In")
            value = len(processes[key].nodes)

            matric.append({"name": name, "value": f"{value} Nodes"})

        return {"matric": matric, "sanky": sanky, "capacity": capacity}

    # =====================================
    async def _create_simulation_flow_chart(
        self,
        sim_df: pd.DataFrame,
        flow: str,
        process: str,
        node: str,
        group_column: str,
    ):

        _start = datetime.now()

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

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="10min",
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

        _end = datetime.now()
        elapsed_time = (_end - _start).total_seconds()
        # print(f"{process}_{node}_{group_column}의 소요시간 : {elapsed_time:.2f}초")
        return {"traces": traces, "default_x": default_x}

    async def _create_simulation_queue_chart_optimized(
        self, sim_df: pd.DataFrame, process, group_column
    ):

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
        self,
        sim_df: pd.DataFrame,
        process,
        group_column,
    ):
        _start = datetime.now()

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"

        df_expanded = sim_df[[start_time, end_time, group_column]].copy()
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

        start_day = df_expanded["Time"].dropna().iloc[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_expanded["Time"].dropna().iloc[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="10min",
        )
        df_expanded = df_expanded[
            df_expanded["Time"].isin(all_hours)
        ]  # 하루를 넘어가거나, 하루 이전의 값을 사전 제거

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

        _end = datetime.now()
        elapsed_time = (_end - _start).total_seconds()
        # print(f"{process}_{group_column}의 소요시간 : {elapsed_time:.2f}초")

        return {"traces": traces, "default_x": default_x}

    async def _create_waiting_time(
        self, sim_df: pd.DataFrame, process, node, group_column
    ) -> pd.DataFrame:

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"

        sim_df = sim_df.loc[sim_df[f"{process}_pred"] == f"{process}_{node}"].copy()

        sim_df[start_time] = pd.to_datetime(sim_df[start_time])
        sim_df[end_time] = pd.to_datetime(sim_df[end_time])
        df = sim_df[[start_time, end_time, group_column]]

        df["waiting_time"] = (df[end_time] - df[start_time]).dt.total_seconds()

        df.loc[:, end_time] = df[end_time].dt.floor("10min")

        df_grouped = (
            df.groupby([end_time, group_column], as_index=False)
            .agg({"waiting_time": "mean"})
            .pivot_table(
                index=end_time,
                columns=group_column,
                values="waiting_time",
                fill_value=0,
            )
        )

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="10min",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        df_grouped = df_grouped.mean(axis=1)
        wt_x_list = df_grouped.index.astype(str).tolist()
        wt_y_list = df_grouped.astype(int).values.tolist()

        return {"y": wt_y_list, "default_x": wt_x_list}

    async def _create_simulation_kpi(self, sim_df: pd.DataFrame, process, node):

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"
        process_time = f"{process}_pt"

        filtered_sim_df = sim_df.loc[
            sim_df[f"{process}_pred"] == f"{process}_{node}"
        ].copy()

        diff_arr = (
            filtered_sim_df[end_time] - filtered_sim_df[start_time]
        ).dt.total_seconds()
        throughput = int(len(filtered_sim_df))
        total_delay = int(diff_arr.sum() / 60)
        max_delay = int(diff_arr.max() / 60)
        average_delay = int((total_delay / throughput) * 100) / 100
        average_transaction_time = int(filtered_sim_df[process_time].mean() * 10) / 10

        result = {
            "Processed Passengers": throughput,
            "Maximum Wait Time": max_delay,
            "Average Wait Time": average_delay,
            "Average Processing Time": average_transaction_time,
        }

        return result

    async def generate_simulation_metrics_kpi(
        self,
        session: boto3.Session,
        user_id: str,
        process: str,
        node: str,
        scenario_id: str | None = None,
        sim_df: pd.DataFrame | None = None,
    ):
        # s3에서 시뮬레이션 데이터 프레임 가져오기
        if scenario_id:
            filename = f"{user_id}/{scenario_id}"

            sim_df = await self.simulation_repo.download_from_s3(session, filename)

        kpi = await self._create_simulation_kpi(sim_df, process, node)

        result = {
            "process": process,
            "node": node,
            "kpi": kpi,
        }
        return result

    async def generate_simulation_charts_node(
        self,
        session: boto3.Session,
        user_id: str,
        process: str,
        node: str,
        scenario_id: str | None = None,
        sim_df: pd.DataFrame | None = None,
    ):
        _start = datetime.now()

        # s3에서 시뮬레이션 데이터 프레임 가져오기
        if scenario_id:
            filename = f"{user_id}/{scenario_id}"

            sim_df = await self.simulation_repo.download_from_s3(session, filename)

            _end_ = datetime.now()
            elapsed_time_ = (_end_ - _start).total_seconds()
            # print(f"S3에서 데이터를 불러오는 소요시간 : {elapsed_time_:.2f}초")

        # 해당 데이터프레임을 선택한 process와 node에 따라 차트 생성
        inbound = {}
        outbound = {}
        queing = {}
        waiting = {}
        # process = checkin / node = A / flow = on
        for group_column in [
            "operating_carrier_name",
            "departure_terminal",
            "country_code",
            "region_name",
        ]:

            queue_data = await self._create_simulation_queue_chart_optimized(
                sim_df, process, group_column
            )
            queing[CRITERIA_MAP[group_column]] = queue_data["traces"]

            waiting_data = await self._create_waiting_time(
                sim_df=sim_df, process=process, node=node, group_column=group_column
            )

            waiting[CRITERIA_MAP[group_column]] = waiting_data["y"]

            for flow in ["on", "pt"]:
                # process = checkin / node = A / flow = on / group_column = operating_carrier_name
                flow_data = await self._create_simulation_flow_chart(
                    sim_df,
                    flow=flow,
                    process=process,
                    node=node,
                    group_column=group_column,
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
                "chart_y_data": waiting,
            },
        }

        _end = datetime.now()
        elapsed_time = (_end - _start).total_seconds()
        # print(f"전체 소요시간 : {elapsed_time:.2f}초")

        return result

    async def generate_simulation_charts_total(
        self,
        session: boto3.Session,
        user_id: str,
        scenario_id: str,
        total: list,
    ):
        filename = f"{user_id}/{scenario_id}"
        sim_df = await self.simulation_repo.download_from_s3(session, filename)

        kpi_list = []
        for li in total:
            kpi = await self._create_simulation_kpi(
                sim_df=sim_df, process=li.process, node=li.node
            )
            kpi["process"] = li.process
            kpi["node"] = li.node

            kpi_list.append(kpi)

        total_df = pd.DataFrame(kpi_list)

        x_data = [f"{row['process']}_{row['node']}" for _, row in total_df.iterrows()]
        y_data = total_df.drop(columns=["process", "node"]).to_dict(orient="list")

        return {"x": x_data, "y": y_data}

    # =====================================
    async def _create_simulation_sankey(
        self, df: pd.DataFrame, component_list, suffix="_pred"
    ) -> dict:
        # 프로세스별 고유값과 인덱스 매핑
        nodes = []
        node_dict = {}
        idx = 0

        # 노드 인덱스 생성
        for process in component_list:
            col_name = f"{process}{suffix}"
            for value in df[col_name].unique():
                if value not in node_dict and pd.notna(value):
                    node_dict[value] = idx
                    # 각 value의 길이를 label로
                    label_count = len(df[df[col_name] == value])
                    nodes.append(f"{value} ({label_count})")
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

    # TODO: 시뮬레이션 매서드 분리 or 클래스화 필요
    async def run_simulation(
        self,
        websocket: WebSocket | None,
        db: Connection,
        session: boto3.Session,
        user_id: str,
        scenario_id: str,
        flight_sch: dict,
        destribution_conditions: list,
        processes: dict,
        component_list: list,
    ):
        # NOTE: 데이터 전처리
        components = []
        component_node_pairs = []
        component_node_map = {}
        nodes_per_component = []
        max_queue_length = []
        facilities_per_node = []
        facility_schedules = []

        node_transition_graph = []  # graph_list

        for comp in component_list:
            components.append(comp.name)
            nodes_per_component.append(len(comp.nodes))

            for node in comp.nodes:
                component_node_pairs.append([comp.name, node.name])
                max_queue_length.append(node.max_queue_length)
                facilities_per_node.append(node.facility_count)
                # FIXME: 일시적으로 1440줄 만들도록 설정.
                facility_schedules.append(np.array([node.facility_schedules[0]] * 1440))

                if comp.name in component_node_map.keys():
                    component_node_map[comp.name].append(node.id)
                else:
                    component_node_map[comp.name] = [node.id]

        await websocket.send_json({"progress": "5%"})
        await asyncio.sleep(0.001)
        # ============================================================
        # NOTE: 쇼업패턴으로 생성된 여객데이터
        data = await self.fetch_flight_schedule_data(
            db, flight_sch.date, flight_sch.airport, flight_sch.condition
        )
        await websocket.send_json({"progress": "30%"})
        await asyncio.sleep(0.001)

        df_pax = await self._calculate_show_up_pattern(data, destribution_conditions)
        await websocket.send_json({"progress": "31%"})
        await asyncio.sleep(0.001)
        # ============================================================
        # NOTE: dist_key와 td_arr을 생성
        np_pax_col = df_pax.columns.to_numpy()
        np_pax = df_pax.to_numpy()

        sorted_idx = np.argsort(np_pax[:, (np_pax_col == "show_up_time")].flatten())
        mask = (np_pax_col == "show_up_time") | (np_pax_col == "operating_carrier_name")
        np_filtered_pax = np_pax[sorted_idx][:, mask]

        # 정렬된 DataFrame 재생성 -> passengers 매개변수에 사용
        sorted_np_pax = np_pax[sorted_idx]
        sorted_df_pax = pd.DataFrame(sorted_np_pax, columns=df_pax.columns)

        # dist_key
        dist_key = np_filtered_pax[:, 0].flatten()
        ck_on = np_filtered_pax[:, -1].flatten()

        # td_arr
        starting_time_stamp = ck_on[0]
        v0 = (
            starting_time_stamp.hour * 3600
            + starting_time_stamp.minute * 60
            + starting_time_stamp.second
        )

        td_arr = np.round(
            [(td.total_seconds()) + v0 for td in (ck_on - np.array(ck_on[0]))]
        )

        await websocket.send_json({"progress": "33%"})
        await asyncio.sleep(0.001)
        # ============================================================
        # NOTE: dist_map과 graph_list를 생성

        # process의 메타데이터 -> graph_list를 만들때 사용
        comp_to_idx = {}
        idx = 0
        for process_key in list(processes.keys())[1:]:
            process = processes[process_key]
            num_li = {}
            for num in range(len(process.nodes)):
                node = process.nodes[num]
                num_li[node] = idx
                idx += 1

            comp_to_idx[process.name] = num_li

        # dist_map
        process_1 = processes["1"]
        dist_map = {}

        for airline, nodes in process_1.default_matrix.items():
            indices = np.array(
                [i for i, node in enumerate(process_1.nodes) if nodes[node] > 0]
            )
            values = np.array(
                [nodes[node] for node in process_1.nodes if nodes[node] > 0]
            )
            dist_map[airline] = [indices, values]

        # graph_list
        node_transition_graph = []
        for i, key in enumerate(processes):
            if int(key) >= 2:
                default_matrix = processes[key].default_matrix
                nodes = processes[key].nodes
                dst_idx = comp_to_idx[processes[key].name]

                for destinations in default_matrix.values():
                    graph = []

                    for key, values in destinations.items():
                        if values > 0:
                            graph.append(
                                [
                                    np.int64(dst_idx[key]),
                                    np.float64(values),
                                ]
                            )

                    node_transition_graph.append(graph)

                if i == len(processes) - 1:

                    for node in range(len(nodes)):
                        node_transition_graph.append([])

        await websocket.send_json({"progress": "35%"})
        await asyncio.sleep(0.001)
        # ============================================================
        # NOTE: 메인 코드
        graph = DsGraph(
            components=components,
            component_node_pairs=component_node_pairs,
            component_node_map=component_node_map,
            nodes_per_component=nodes_per_component,
            node_transition_graph=node_transition_graph,
            max_queue_length=max_queue_length,
            facilities_per_node=facilities_per_node,
            facility_schedules=facility_schedules,
            processes=processes,  # 프로세스들의 인풋값
            comp_to_idx=comp_to_idx,  # 프로세스의 메타데이터
        )

        # comp_to_idx = {'checkin': {'A': 0, 'B': 1, 'C': 2, 'D': 3}, 'departure_gate': {'DG1': 4, 'DG2': 5}, 'security_check': {'SC1': 6, 'SC2': 7}, 'passport_check': {'PC1': 8, 'PC2': 9}}

        sim = DsSimulator(
            ds_graph=graph,
            components=components,
            showup_times=td_arr,
            source_per_passengers=dist_key,
            source_transition_graph=dist_map,
            passengers=sorted_df_pax,  # <-- SHOW-UP 로직을 돌린다.
        )

        SECONDS_IN_THREE_DAYS = 3600 * 24 * 3
        last_passenger_arrival_time = td_arr[-1]

        await sim.run(
            start_time=0,
            end_time=max(SECONDS_IN_THREE_DAYS, last_passenger_arrival_time),
            websocket=websocket,
        )

        ow = DsOutputWrapper(
            passengers=sorted_df_pax,
            components=components,
            nodes=graph.nodes,
            starting_time=[starting_time_stamp, v0],
        )
        ow.write_pred()

        await websocket.send_json({"progress": "95%"})
        await asyncio.sleep(0.001)

        # =====================================
        # NOTE: 시뮬레이션 결과 데이터 s3 저장
        # print(ow.passengers)
        # ow.passengers.to_csv("sim_pax_test.csv", encoding="utf-8-sig", index=False)

        filename = f"{user_id}/{scenario_id}.parquet"
        await self.simulation_repo.upload_to_s3(session, ow.passengers, filename)

        await websocket.send_json({"progress": "97%"})
        await asyncio.sleep(0.001)

        # =====================================
        # NOTE: 시뮬레이션 결과 데이터를 차트로 변환
        sankey = await self._create_simulation_sankey(
            df=ow.passengers, component_list=components
        )
        await websocket.send_json({"progress": "98%"})
        await asyncio.sleep(0.001)

        first_process = next(iter(comp_to_idx), None)
        node_list = sorted(ow.passengers[f"{first_process}_pred"].unique().tolist())
        first_node = node_list[0].replace(f"{first_process}_", "")

        chart = await self.generate_simulation_charts_node(
            session=session,
            user_id=None,
            scenario_id=None,
            sim_df=ow.passengers,
            process=first_process,
            node=first_node,
        )

        kpi = await self.generate_simulation_metrics_kpi(
            session=session,
            user_id=None,
            scenario_id=None,
            sim_df=ow.passengers,
            process=first_process,
            node=first_node,
        )

        await websocket.send_json({"progress": "99%"})
        await asyncio.sleep(0.001)

        return {"sankey": sankey, "kpi": kpi, "chart": chart}

    async def run_simulation_test(
        self,
        db: Connection,
        session: boto3.Session,
        user_id: str,
        scenario_id: str,
        flight_sch: dict,
        destribution_conditions: list,
        processes: dict,
        component_list: list,
    ):
        # FIXME: 테스트용 확인 코드
        # print(f"시작시간 : {datetime.now()}")
        # ============================================================
        # NOTE: 데이터 전처리
        components = []
        component_node_pairs = []
        component_node_map = {}
        nodes_per_component = []
        max_queue_length = []
        facilities_per_node = []
        facility_schedules = []

        node_transition_graph = []  # graph_list

        for comp in component_list:
            components.append(comp.name)
            nodes_per_component.append(len(comp.nodes))

            for node in comp.nodes:
                component_node_pairs.append([comp.name, node.name])
                max_queue_length.append(node.max_queue_length)
                facilities_per_node.append(node.facility_count)
                # FIXME: 일시적으로 1440줄 만들도록 설정.
                facility_schedules.append(np.array([node.facility_schedules[0]] * 1440))

                if comp.name in component_node_map.keys():
                    component_node_map[comp.name].append(node.id)
                else:
                    component_node_map[comp.name] = [node.id]

        await asyncio.sleep(0.001)
        # ============================================================
        # NOTE: 쇼업패턴으로 생성된 여객데이터
        data = await self.fetch_flight_schedule_data_test(
            db, flight_sch.date, flight_sch.airport, flight_sch.condition
        )

        await asyncio.sleep(0.001)

        df_pax = await self._calculate_show_up_pattern(data, destribution_conditions)
        await asyncio.sleep(0.001)
        # ============================================================
        # NOTE: dist_key와 td_arr을 생성
        np_pax_col = df_pax.columns.to_numpy()
        np_pax = df_pax.to_numpy()

        sorted_idx = np.argsort(np_pax[:, (np_pax_col == "show_up_time")].flatten())
        mask = (np_pax_col == "show_up_time") | (np_pax_col == "operating_carrier_name")
        np_filtered_pax = np_pax[sorted_idx][:, mask]

        # 정렬된 DataFrame 재생성 -> passengers 매개변수에 사용
        sorted_np_pax = np_pax[sorted_idx]
        sorted_df_pax = pd.DataFrame(sorted_np_pax, columns=df_pax.columns)

        # dist_key
        dist_key = np_filtered_pax[:, 0].flatten()
        ck_on = np_filtered_pax[:, -1].flatten()

        # td_arr
        starting_time_stamp = ck_on[0]
        v0 = (
            starting_time_stamp.hour * 3600
            + starting_time_stamp.minute * 60
            + starting_time_stamp.second
        )

        td_arr = np.round(
            [(td.total_seconds()) + v0 for td in (ck_on - np.array(ck_on[0]))]
        )

        await asyncio.sleep(0.001)
        # ============================================================
        # NOTE: dist_map과 graph_list를 생성

        # process의 메타데이터 -> graph_list를 만들때 사용
        comp_to_idx = {}
        idx = 0
        for process_key in list(processes.keys())[1:]:
            process = processes[process_key]
            num_li = {}
            for num in range(len(process.nodes)):
                node = process.nodes[num]
                num_li[node] = idx
                idx += 1

            comp_to_idx[process.name] = num_li

        # dist_map
        process_1 = processes["1"]
        dist_map = {}

        for airline, nodes in process_1.default_matrix.items():
            indices = np.array(
                [i for i, node in enumerate(process_1.nodes) if nodes[node] > 0]
            )
            values = np.array(
                [nodes[node] for node in process_1.nodes if nodes[node] > 0]
            )
            dist_map[airline] = [indices, values]

        # graph_list
        node_transition_graph = []
        for i, key in enumerate(processes):
            if int(key) >= 2:
                default_matrix = processes[key].default_matrix
                nodes = processes[key].nodes
                dst_idx = comp_to_idx[processes[key].name]

                for destinations in default_matrix.values():
                    graph = []

                    for key, values in destinations.items():
                        if values > 0:
                            graph.append(
                                [
                                    np.int64(dst_idx[key]),
                                    np.float64(values),
                                ]
                            )

                    node_transition_graph.append(graph)

                if i == len(processes) - 1:

                    for node in range(len(nodes)):
                        node_transition_graph.append([])

        await asyncio.sleep(0.001)
        # ============================================================
        # NOTE: 메인 코드
        try:
            graph = DsGraph(
                components=components,
                component_node_pairs=component_node_pairs,
                component_node_map=component_node_map,
                nodes_per_component=nodes_per_component,
                node_transition_graph=node_transition_graph,
                max_queue_length=max_queue_length,
                facilities_per_node=facilities_per_node,
                facility_schedules=facility_schedules,
                processes=processes,  # 프로세스들의 인풋값
                comp_to_idx=comp_to_idx,  # 프로세스의 메타데이터
            )
        except Exception as e:
            raise

        # comp_to_idx = {'checkin': {'A': 0, 'B': 1, 'C': 2, 'D': 3}, 'departure_gate': {'DG1': 4, 'DG2': 5}, 'security_check': {'SC1': 6, 'SC2': 7}, 'passport_check': {'PC1': 8, 'PC2': 9}}

        sim = DsSimulator(
            ds_graph=graph,
            components=components,
            showup_times=td_arr,
            source_per_passengers=dist_key,
            source_transition_graph=dist_map,
            passengers=sorted_df_pax,  # <-- SHOW-UP 로직을 돌린다.
        )

        SECONDS_IN_THREE_DAYS = 3600 * 24 * 3
        last_passenger_arrival_time = td_arr[-1]

        await sim.run_test(
            start_time=0,
            end_time=max(SECONDS_IN_THREE_DAYS, last_passenger_arrival_time),
        )

        ow = DsOutputWrapper(
            passengers=sorted_df_pax,
            components=components,
            nodes=graph.nodes,
            starting_time=[starting_time_stamp, v0],
        )
        ow.write_pred()

        await asyncio.sleep(0.001)

        # =====================================
        # NOTE: 시뮬레이션 결과 데이터 s3 저장
        # print(ow.passengers)
        ow.passengers.to_csv("sim_pax_test.csv", encoding="utf-8-sig", index=False)

        # filename = f"{user_id}/{scenario_id}.parquet"
        # await self.simulation_repo.upload_to_s3(session, ow.passengers, filename)

        await asyncio.sleep(0.001)

        # =====================================
        # NOTE: 시뮬레이션 결과 데이터를 차트로 변환
        sankey = await self._create_simulation_sankey(
            df=ow.passengers, component_list=components
        )

        await asyncio.sleep(0.001)

        first_process = next(iter(comp_to_idx), None)
        node_list = sorted(ow.passengers[f"{first_process}_pred"].unique().tolist())
        first_node = node_list[0].replace(f"{first_process}_", "")

        chart = await self.generate_simulation_charts_node(
            session=session,
            user_id=None,
            scenario_id=None,
            sim_df=ow.passengers,
            process=first_process,
            node=first_node,
        )

        kpi = await self.generate_simulation_metrics_kpi(
            session=session,
            user_id=None,
            scenario_id=None,
            sim_df=ow.passengers,
            process=first_process,
            node=first_node,
        )

        await asyncio.sleep(0.001)

        # FIXME: 테스트용 확인코드
        # print(f"완료시간 : {datetime.now()}")
        return {"sankey": sankey, "kpi": kpi, "chart": chart}

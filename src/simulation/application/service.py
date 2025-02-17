import os
from datetime import datetime, time

import numpy as np
import pandas as pd
from dependency_injector.wiring import inject
from sqlalchemy import Connection, text
from sqlalchemy.ext.asyncio import AsyncSession
from ulid import ULID

from src.common import TimeStamp
from src.constants import COL_FILTER_MAP
from src.simulation.application.core.graph import DsGraph
from src.simulation.application.core.ouput_wrapper import DsOutputWrapper
from src.simulation.application.core.simulator import DsSimulator
from src.simulation.application.queries import (
    SELECT_AIRPORT_ARRIVAL,
    SELECT_AIRPORT_DEPARTURE,
)
from src.simulation.domain.repository import ISimulationRepository
from src.simulation.domain.simulation import ScenarioMetadata, SimulationScenario


# FIXME: 스노우플레이크 정상작동시 삭제할 os 경로 코드
# NOTE: samples 폴더 안에 sample_ICN_data.json 파일 필요
SAMPLE_DATA = os.path.join(os.getcwd(), "code/samples/sample_ICN_data.json")


class SimulationService:
    """
    //매서드 정의//
    generate : 각 controller의 최상위 매서드 (하위 매서드를 포함)
    run_simulation : 최종 시뮬레이션 코드 (현재 분할 필요)

    _calculate: 새로운 데이터 생성 (내장함수)
    _create : 차트 생성 (내장함수)

    fetch : get
    create : post
    update : put
    """

    # TODO: 그래프 만드는 코드들이 서로 비슷한데 합칠수는 없을까?

    @inject
    def __init__(
        self,
        simulation_repo: ISimulationRepository,
    ):
        self.simulation_repo = simulation_repo
        self.timestamp = TimeStamp()

    async def fetch_simulation_scenario(self, db: AsyncSession, user_id: str):

        scenario = await self.simulation_repo.fetch_simulation_scenario(db, user_id)

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
            simulation_id=id,
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

        return id

    async def fetch_scenario_metadata(self, db: AsyncSession, simulation_id: str):

        metadata = await self.simulation_repo.fetch_scenario_metadata(db, simulation_id)

        return metadata

    async def update_scenario_metadata(
        self,
        db: AsyncSession,
        simulation_id: str,
        overview: dict | None,
        history: dict | None,
        flight_sch: dict | None,
        passenger_sch: dict | None,
        passenger_attr: dict | None,
        facility_conn: dict | None,
        facility_info: dict | None,
    ):

        scenario_metadata: ScenarioMetadata = ScenarioMetadata(
            simulation_id=simulation_id,
            overview=overview,
            history=history,
            flight_sch=flight_sch,
            passenger_sch=passenger_sch,
            passenger_attr=passenger_attr,
            facility_conn=facility_conn,
            facility_info=facility_info,
        )

        await self.simulation_repo.update_scenario_metadata(db, scenario_metadata)

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
                    where_conditions.append("FLIGHT_TYPE = :i/d")
                    params["i/d"] = con.value

                if con.criteria == "Terminal":
                    where_conditions.append("F.departure_terminal = :terminal")
                    params["terminal"] = con.value

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

    async def update_simulation_scenario(
        self, db: AsyncSession, user_id: str, target_date: str
    ):

        target_datetime = datetime.strptime(target_date, "%Y-%m-%d")

        await self.simulation_repo.update_simulation_scenario(
            db, user_id, target_datetime
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

        day = df_grouped.index[0].date()
        all_hours = pd.date_range(
            start=pd.Timestamp(day),
            end=pd.Timestamp(day) + pd.Timedelta(hours=23),
            freq="h",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        default_x = df_grouped.index.strftime("%H:%M").tolist()
        traces = [
            {"name": column, "y": df_grouped[column].tolist()}
            for column in df_grouped.columns
        ]

        result = {
            "default_x": default_x,
            "traces": traces,
        }

        return result

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

        # data = await self.fetch_flight_schedule_data(db, date, airport, condition)
        # =====================
        # FIXME: 스노우플레이크 정상화되면 삭제할 코드
        import json

        with open(
            SAMPLE_DATA,
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)
        # ======================
        flight_df = pd.DataFrame(data)

        add_condition = None
        if first_load:
            # I/D, 항공사, 터미널
            i_d = flight_df["flight_type"].unique().tolist()
            airline = flight_df["operating_carrier_iata"].unique().tolist()
            terminal = flight_df["departure_terminal"].unique().tolist()

            add_condition = {"I/D": i_d, "Airline": airline, "Terminal": terminal}

        chart_result = {}
        for group_column in [
            "operating_carrier_iata",
            "departure_terminal",
            "flight_type",
        ]:
            chart_data = await self._create_flight_schedule_chart(
                flight_df, group_column
            )

            chart_result[f"{group_column}_chart_data"] = chart_data

        return {"condition": add_condition, "chart_data": chart_result}

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
            pax_df = pd.concat([pax_df, partial_pax_df], ignore_index=True)
            df.drop(index=partial_df.index, inplace=True)
        # pax_df.to_csv(".idea/pax_df.csv", index=False) # 분산처리 데이터검증목적
        return pax_df

    async def _create_normal_distribution(self, destribution_conditions: list):
        distribution_xy_coords = {}
        for condition in destribution_conditions:
            index = condition.index
            mean = condition.mean
            std_dev = condition.standard_deviation
            x = np.linspace(mean - 4 * std_dev, mean + 4 * std_dev, 1000)
            y = (1 / (std_dev * np.sqrt(2 * np.pi))) * np.exp(
                -0.5 * ((x - mean) / std_dev) ** 2
            )
            distribution_xy_coords[index] = {"x": x.tolist(), "y": y.tolist()}
        return distribution_xy_coords

    async def _create_show_up_summary(self, pax_df: pd.DataFrame, group_column: str):
        pax_df["show_up_time"] = pax_df["show_up_time"].dt.floor("h")
        df_grouped = (
            pax_df.groupby(["show_up_time", group_column]).size().unstack(fill_value=0)
        )
        df_grouped = df_grouped.sort_index()

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="h",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        default_x = df_grouped.index.strftime("%Y-%m-%d %H:%M:%S").tolist()
        traces = [
            {"name": column, "y": df_grouped[column].tolist()}
            for column in df_grouped.columns
        ]

        summary = {
            "default_x": default_x,
            "traces": traces,
        }
        return summary

    async def generate_passenger_schedule(
        self, db: Connection, flight_sch: dict, destribution_conditions: list
    ):

        # data = await self.fetch_flight_schedule_data(
        #     db,
        #     flight_sch.date,
        #     flight_sch.airport,
        #     flight_sch.condition,
        # )
        # =====================
        # FIXME: 스노우플레이크 정상화되면 삭제할 코드
        import json

        with open(
            SAMPLE_DATA,
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)
        # =====================

        pax_df = await self._calculate_show_up_pattern(data, destribution_conditions)

        distribution_xy_coords = await self._create_normal_distribution(
            destribution_conditions
        )

        chart_result = {}
        for group_column in [
            "operating_carrier_iata",
            "departure_terminal",
            "country_code",
            "region_name",
        ]:
            chart_data = await self._create_show_up_summary(pax_df, group_column)
            chart_result[f"{group_column}_chart_data"] = chart_data

        return {"dst_chart": distribution_xy_coords, "bar_chart": chart_result}

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

    async def _create_capacity_chart(self, df_pax, node_list):
        # 10분 단위의 시간 리스트 생성
        times = [
            time(hour=hour, minute=minute)
            for hour in range(24)
            for minute in range(0, 60, 10)
        ]
        time_df = pd.DataFrame({"index": times})
        time_df["index"] = time_df["index"].astype(str)

        fin_dict = {}

        for node in node_list:
            # 노드별 데이터 필터링 후 복사본 생성
            df_pax_filtered = df_pax[df_pax["checkin_component"] == node].copy()
            # '10T' 대신 '10min' 사용
            df_pax_filtered.loc[:, "show_up_time"] = df_pax_filtered[
                "show_up_time"
            ].dt.floor("10min")

            grouped = df_pax_filtered["show_up_time"].value_counts().reset_index()
            grouped["index"] = grouped["show_up_time"].astype(str).str[-8:]

            total_capa = pd.merge(time_df, grouped, on="index", how="left")
            total_capa["count"] = total_capa["count"].fillna(0)
            total_capa = total_capa.drop("show_up_time", axis=1)

            total_dict = {
                "x": total_capa["index"].tolist(),  # 시간 리스트
                "y": total_capa["count"].tolist(),  # count 리스트
            }

            fin_dict[node] = total_dict

        return fin_dict

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

            for priority_matrix in facility_detail[facility].priority_matrix:
                priority_matrix_df = pd.DataFrame.from_dict(
                    priority_matrix.matrix, orient="index"
                )

                condition = pd.Series(True, index=df_pax.index)
                for con in priority_matrix.condition:
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
                    ] = df_pax.loc[condition, f"{vertical_process}_component"].apply(
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
        # data = await self.fetch_flight_schedule_data(
        #     db, flight_sch.date, flight_sch.airport, flight_sch.condition
        # )
        # =====================
        # FIXME: 스노우플레이크 정상화되면 삭제할 코드
        import json

        with open(
            SAMPLE_DATA,
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)
        # =====================

        pax_df = await self._calculate_show_up_pattern(data, destribution_conditions)
        pax_df = await self._calculate_add_columns(processes, pax_df)

        sanky = await self._create_facility_conn_sankey(processes, pax_df)
        capacity = await self._create_capacity_chart(
            pax_df, node_list=processes["1"].nodes
        )

        return {"sanky": sanky, "capacity": capacity}

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

    # TODO: 시뮬레이션으로 나오는 데이터 처리하는데, 양이 너무 많음.....
    async def _create_simulation_kpi_chart(
        self,
        pax_df: pd.DataFrame,
        on_done: str,
        process_name: str,
        process_node: str,
        group_column: str,
    ):
        pax_df = pax_df.loc[
            pax_df[f"{process_name}_pred"] == f"{process_name}_{process_node}"
        ].copy()

        pax_df.loc[:, f"{process_name}_{on_done}_pred"] = pax_df[
            f"{process_name}_{on_done}_pred"
        ].dt.floor("10min")

        df_grouped = (
            pax_df.groupby([f"{process_name}_{on_done}_pred", group_column])
            .size()
            .unstack(fill_value=0)
        )
        df_grouped = df_grouped.sort_index()

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="10min",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        default_x = df_grouped.index.strftime("%Y-%m-%d %H:%M:%S").tolist()
        traces = [
            {"name": column, "y": df_grouped[column].tolist()}
            for column in df_grouped.columns
        ]

        summary = {
            "default_x": default_x,
            "traces": traces,
        }
        return summary

    # TODO: 시뮬레이션 매서드 분리 or 클래스화 필요
    async def run_simulation(
        self,
        db: Connection,
        flight_sch: dict,
        destribution_conditions: list,
        processes: dict,
        component_list: list,
    ):
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

        # ============================================================
        # NOTE: 쇼업패턴으로 생성된 여객데이터
        # data = await self.fetch_flight_schedule_data(
        #     db, flight_sch.date, flight_sch.airport, flight_sch.condition
        # )
        # =====================
        # FIXME: 스노우플레이크 정상화되면 삭제할 코드
        import json

        with open(
            SAMPLE_DATA,
            "r",
            encoding="utf-8",
        ) as file:
            data = json.load(file)
        # =====================

        df_pax = await self._calculate_show_up_pattern(data, destribution_conditions)

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

        # ============================================================
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

        sim.run(
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

        # print(ow.passengers)
        # ow.passengers.to_csv("sim_pax_test.csv", encoding="utf-8-sig", index=False)

        sankey = await self._create_simulation_sankey(
            df=ow.passengers, component_list=components
        )

        # TODO: 차트만드는 코드는 현재 너무 길어서 고민
        # chart_result = {}
        # for process, nodes in comp_to_idx.items():
        #     # process = checkin / nodes = A, B, C, D
        #     for node in list(nodes.keys()):
        #         # process = checkin / node = A
        #         for on_done in ["on", "done"]:
        #             # process = checkin / node = A / on_done = on
        #             for group_column in [
        #                 "operating_carrier_iata",
        #                 "departure_terminal",
        #                 "country_code",
        #                 "region_name",
        #             ]:
        #                 # process = checkin / node = A / on_done = on / group_column = operating_carrier_iata
        #                 chart_data = await self._create_simulation_kpi_chart(
        #                     ow.passengers,
        #                     on_done=on_done,
        #                     process_name=process,
        #                     process_node=node,
        #                     group_column=group_column,
        #                 )
        #                 chart_result[
        #                     f"{process}_{node}_{on_done}_{group_column}_chart_data"
        #                 ] = chart_data
        # output_file = "output.json"  # 저장할 JSON 파일명
        # with open(output_file, "w", encoding="utf-8") as json_file:
        #     json.dump(chart_result, json_file, ensure_ascii=False, indent=4)
        return {"sankey": sankey, "chart": "not yet"}
        # return "simulation success!!"

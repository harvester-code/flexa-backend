from src.simulation.application.core.graph import DsGraph
from src.simulation.application.core.simulator import DsSimulator
from src.simulation.application.core.ouput_wrapper import DsOutputWrapper
from src.simulation.schema import SimulationBody
from src.airports.service import AirportService
import numpy as np
import pandas as pd

airport_service = AirportService()


class SimulationService:
    # @staticmethod
    def run_simulation(self, item: SimulationBody):
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

        for comp in item.components:
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
        df_pax = airport_service.show_up_pattern(
            data=item.data,
            destribution_conditions=item.destribution_conditions,
        )

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
        for process_key in list(item.processes.keys())[1:]:
            process = item.processes[process_key]
            num_li = {}
            for num in range(len(process.nodes)):
                node = process.nodes[num]
                num_li[node] = idx
                idx += 1

            comp_to_idx[process.name] = num_li

        # dist_map
        process_1 = item.processes["1"]
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
        for i, key in enumerate(item.processes):
            if int(key) >= 2:
                default_matrix = item.processes[key].default_matrix
                nodes = item.processes[key].nodes
                dst_idx = comp_to_idx[item.processes[key].name]

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

                if i == len(item.processes) - 1:

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
            processes=item.processes,  # 프로세스들의 인풋값
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
        # ow.passengers.to_csv("sim_pax_0_capa250.csv", encoding="utf-8-sig", index=False)

        sankey = self.sankey(df=ow.passengers, component_list=components)

        return sankey
        # return "simulation success!!"

    def sankey(self, df, component_list, suffix="_pred") -> dict:
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
            "node": nodes,
            "sources": sources,
            "targets": targets,
            "values": values,
        }

        return sankey

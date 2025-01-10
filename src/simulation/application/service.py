from src.simulation.application.core.graph import DsGraph
from src.simulation.application.core.simulator import DsSimulator
from src.simulation.schema import SimulationBody
from src.airports.service import AirportService
import numpy as np

airport_service = AirportService()


class SimulationService:
    @staticmethod
    def run_simulation(item: SimulationBody):
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
                facility_schedules.append(np.array(node.facility_schedules))

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

        # dist_key
        dist_key = np_filtered_pax[:, 0].flatten()
        ck_on = np_filtered_pax[:, -1].flatten()

        starting_time_stamp = ck_on[0]
        v0 = (
            starting_time_stamp.hour * 3600
            + starting_time_stamp.minute * 60
            + starting_time_stamp.second
        )

        # td_arr
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

        for airline, nodes in process_1.default_matricx.items():
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
                default_matrix = item.processes[key].default_matricx
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
        )

        sim = DsSimulator(
            ds_graph=graph,
            components=components,
            showup_times=td_arr,
            source_per_passengers=dist_key,
            source_transition_graph=dist_map,
            passengers=df_pax,  # <-- SHOW-UP 로직을 돌린다.
        )

        SECONDS_IN_THREE_DAYS = 3600 * 24 * 3
        last_passenger_arrival_time = td_arr[-1]

        sim.run(
            start_time=0,
            end_time=max(SECONDS_IN_THREE_DAYS, last_passenger_arrival_time),
        )

        return "simulation success!!"

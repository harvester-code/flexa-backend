from src.simulation.application.core.graph import DsGraph
from src.simulation.application.core.simulator import DsSimulator
from src.simulation.schema import SimulationBody
from src.airports.service import AirportService

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
                facility_schedules.append(node.facility_schedules)

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

        # graph = DsGraph(
        #     components=components,
        #     component_node_pairs=component_node_pairs,
        #     component_node_map=component_node_map,
        #     nodes_per_component=nodes_per_component,
        #     node_transition_graph=node_transition_graph,
        #     max_queue_length=max_queue_length,
        #     facilities_per_node=facilities_per_node,
        #     facility_schedules=facility_schedules,
        # )

        # sim = DsSimulator(
        #     ds_graph=graph,
        #     components=params["comp"],
        #     showup_times=params["td_arr"],
        #     source_per_passengers=params["dist_key"],
        #     source_transition_graph=params["dist_map"],
        #     passengers=params["df_pax"],  # <-- SHOW-UP 로직을 돌린다.
        # )

        # SECONDS_IN_THREE_DAYS = 3600 * 24 * 3
        # last_passenger_arrival_time = params["td_arr"][-1]

        # sim.run(
        #     start_time=0,
        #     end_time=max(SECONDS_IN_THREE_DAYS, last_passenger_arrival_time),
        # )

        return "simulation success!!"

from typing import List

from src.simulation.application.core.node import DsNode


class DsGraph:
    def __init__(
        self,
        components,
        component_node_map,
        component_node_pairs,
        nodes_per_component,
        node_transition_graph,
        max_queue_length,
        facilities_per_node,
        facility_schedules,
    ):
        self.components = components
        self.component_node_map = component_node_map
        self.component_node_pairs = component_node_pairs
        self.nodes_per_component = nodes_per_component
        self.node_transition_graph = node_transition_graph
        self.facilities_per_node = facilities_per_node
        self.facility_schedules = facility_schedules
        self.max_queue_length = max_queue_length

        self.nodes = self.__build_nodes()

    def __build_nodes(self) -> List[DsNode]:
        nodes: DsNode = []

        def pad_components_with_none(components, component_node_map, idx):
            components = components + [None] * 100

            # TODO: 개선 가능성 있어 보임
            current_component = [
                key for key, value in component_node_map.items() if idx in value
            ][0]
            current_idx = components.index(current_component)

            padded_components = components[current_idx:]
            return padded_components

        for i in range(sum(self.nodes_per_component)):
            padded_components = pad_components_with_none(
                idx=i,
                components=self.components,
                component_node_map=self.component_node_map,
            )

            target_transition = self.node_transition_graph[i]
            destinations = (
                [dst[0] for dst in target_transition] if target_transition else None
            )
            destination_choices = (
                [dst[1] for dst in target_transition] if target_transition else []
            )

            node_label = (
                f"{self.component_node_pairs[i][0]}_{self.component_node_pairs[i][1]}"
            )

            node = DsNode(
                node_id=i,
                node_label=node_label,
                components=padded_components,
                destinations=destinations,
                destination_choices=destination_choices,
                facility_schedule=self.facility_schedules[i],
                max_capacity=self.max_queue_length[i],
                num_facilities=self.facilities_per_node[i],
                is_deterministic=True,
            )
            nodes.append(node)

        # ==============================================================
        # NOTE: Replace indices (e.g., [4, 5] or [8]) to DsNode instances
        for node in nodes:
            if node.destinations:
                node.destinations = [nodes[i] for i in node.destinations]

        return nodes

    # FIXME: 굳이 여기서 prod 메서드를 또 만들 필요가 없다. simulator에서 바로 ds_graph.nodes를 for loop 돌아도 된다.
    def prod(self, second, minute, passengers):
        for node in self.nodes:
            node.prod(
                nodes=self.nodes,
                passengers=passengers,
                second=second,
                minute=minute,
            )

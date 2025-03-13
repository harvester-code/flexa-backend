import heapq
import pandas as pd
import numpy as np
from numpy import ndarray
from src.constants import COL_FILTER_MAP


class DsNode:
    def __init__(
        self,
        node_id,
        node_label: str,
        components,
        destination_choices,
        destinations,
        facility_schedule: ndarray,
        max_capacity,
        num_facilities,
        processes,
        comp_to_idx,
        num_passengers: int = 500_000,
        bypass: bool = False,
        is_deterministic: bool = False,
        selection_config=None,
        random_seed=42,
    ):
        # if random_seed:
        #     np.random.seed(random_seed)
        self.node_id = node_id
        self.node_label = node_label
        self.components = components
        self.is_deterministic = is_deterministic
        self.max_capacity = max_capacity
        self.num_passengers = num_passengers
        self.destinations = destinations
        self.destination_choices = destination_choices
        self.bypass = bypass
        self.processes = processes
        self.comp_to_idx = comp_to_idx
        # ==========
        self.occupied_facilities = []
        self.passenger_ids = []
        self.passenger_node_id = 0
        self.passenger_queues = []
        self.que_history = np.zeros(num_passengers, dtype=int) - 1
        self.processing_time = np.zeros(num_passengers, dtype=int)
        self.unoccupied_facilities = np.ones(num_facilities, dtype=int)
        self.on_time = np.zeros(num_passengers, dtype=int)
        self.done_time = np.zeros(num_passengers, dtype=int)
        if facility_schedule.size:
            self.processing_config = np.array(facility_schedule)
        else:
            self.processing_config = 183 * np.ones((60 * 24, num_facilities), dtype=int)
        if selection_config:
            self.selection_config = np.array(selection_config)
        else:
            self.selection_config = 1 * (self.processing_config > 0)

    def normalize(self, arr):
        return arr / sum(arr)

    def prod(self, second, minute, nodes, passengers):
        self._prod_counter(second, nodes, passengers)
        self._prod_que(second, minute, nodes, passengers)

    def _prod_counter(self, second, nodes, passengers):
        while self.occupied_facilities and self.occupied_facilities[0][0] <= second:
            # 이전 프로세스에서 넘어온 값
            _, passenger_node_id, facility_number = heapq.heappop(
                self.occupied_facilities
            )

            self.done_time[passenger_node_id] = second
            self.unoccupied_facilities[facility_number] = 1

            if self.destinations is None:
                break

            # 전체 프로세스에서의 승객 ID
            passenger_id = self.passenger_ids[passenger_node_id]

            destination = self.select_destination(nodes, passengers, passenger_id)
            destination.passenger_ids.append(passenger_id)

            # NOTE: destination의 승객 노드 ID
            _passenger_node_id = destination.passenger_node_id

            # TODO: 아래 코드 위치를 수정해보기
            destination.passenger_node_id += 1

            # movement_time = destination.test_pt(3600)
            movement_time = 0
            destination.on_time[_passenger_node_id] = (
                self.done_time[passenger_node_id] + movement_time
            )  # TODO: 검증과정이 필요함. 시그마를 0으로 두고 10분씩 뒤로 밀리는지 체크가 1번.

            destination.que_history[_passenger_node_id] = len(
                destination.passenger_queues
            )

            minute_of_day = min(
                1439, (destination.on_time[_passenger_node_id] % 86400) // 60
            )

            destination_facility_number = destination.select_facility(
                minute=minute_of_day
            )

            # dod: destination of destination
            dod_component = destination.components[1]
            # 기존 도착 컴포넌트
            destination_component = destination.components[0]

            priority_dod_node_indices = None
            if dod_component:
                passenger = passengers.loc[_passenger_node_id]
                # 기존 코드와 동일
                for process in self.processes.values():
                    if process.name == dod_component:
                        priority_matrix = process.priority_matrix
                        break

                edited_df = None
                if priority_matrix:
                    for priority in priority_matrix:

                        conditions = priority.condition

                        for condition in conditions:
                            criteria = condition.criteria
                            criteria = COL_FILTER_MAP[criteria]
                            value = condition.value

                            check = passenger[criteria] in value
                            if not check:
                                break  # check = False로 내보낸다.

                        # check가 false이면 이건 돌지 않기 때문에 다음 컨디션을 확인해야함.
                        if check:
                            edited_df = priority.matrix
                            break

                # 기존 코드와 동일
                if edited_df:
                    priority_dod_node_indices = (
                        [
                            self.comp_to_idx[dod_component][idx]
                            for idx in list(list(edited_df.values())[0].keys())
                        ]
                        if destination.node_id
                        in [
                            self.comp_to_idx[destination_component][key]
                            for key in list(edited_df.keys())
                        ]
                        else None
                    )

            priority_dod_nodes = (
                [nodes[idx] for idx in priority_dod_node_indices]
                if priority_dod_node_indices
                else None
            )
            dod_nodes = priority_dod_nodes or destination.destinations

            if destination_facility_number is None:
                heapq.heappush(
                    destination.passenger_queues,
                    (destination.on_time[_passenger_node_id], _passenger_node_id),
                )
            elif dod_nodes is not None and all(
                [
                    ddst.max_capacity - len(ddst.passenger_queues) <= 0
                    for ddst in dod_nodes
                ]
            ):
                destination.unoccupied_facilities[destination_facility_number - 1] = 1
                heapq.heappush(
                    destination.passenger_queues,
                    (destination.on_time[_passenger_node_id], _passenger_node_id),
                )
            else:
                adjusted_processing_time = destination.adjust_processing_time(
                    destination.processing_config[minute_of_day][
                        destination_facility_number - 1
                    ]
                )
                heapq.heappush(
                    destination.occupied_facilities,
                    (
                        destination.on_time[_passenger_node_id]
                        + adjusted_processing_time,
                        _passenger_node_id,
                        destination_facility_number - 1,
                    ),
                )
                destination.processing_time[_passenger_node_id] = (
                    adjusted_processing_time
                )

    def select_destination(self, nodes, df_pax, pax_idx):

        # edited_df가 컬럼에 없기때문에 시작 컴포넌트가 필요
        start_component = self.components[0]
        destination_component = self.components[1]

        priority_destination_node_indices = None
        if destination_component:
            passenger = df_pax.loc[pax_idx]
            # 기존과 같은 방식으로 matrix을 가져온다.
            for process in self.processes.values():
                if process.name == destination_component:
                    priority_matrix = process.priority_matrix
                    break

            edited_df = None
            if priority_matrix:
                for priority in priority_matrix:

                    conditions = priority.condition

                    for condition in conditions:
                        criteria = condition.criteria
                        criteria = COL_FILTER_MAP[criteria]
                        value = condition.value

                        check = passenger[criteria] in value
                        if not check:
                            break  # check = False로 내보낸다.

                    # check가 false이면 이건 돌지 않기 때문에 다음 컨디션을 확인해야함.
                    if check:
                        edited_df = priority.matrix
                        break

            # 해당 edited_df의 키값 = 시작컴포넌트를 맞춰주고, 맞다면 해당 매트릭의 키값 = 도착컴포넌트를 맞춰준다.
            if edited_df:
                priority_destination_node_indices = (
                    [
                        self.comp_to_idx[destination_component][idx]
                        for idx in list(list(edited_df.values())[0].keys())
                    ]
                    if self.node_id
                    in [
                        self.comp_to_idx[start_component][key]
                        for key in list(edited_df.keys())
                    ]
                    else None
                )

        # 해당 컴포넌트로 dst_nodes를 만들어준다.
        priority_destination_nodes = (
            [nodes[idx] for idx in priority_destination_node_indices]
            if priority_destination_node_indices
            else None
        )

        destination_nodes = priority_destination_nodes or self.destinations

        available_destination_nodes = [
            i
            for i, node in enumerate(destination_nodes)
            if len(node.passenger_queues) < node.max_capacity
        ]

        if len(available_destination_nodes) == 0:
            return min(destination_nodes, key=lambda x: len(x.passenger_queues))

        # ==================================================
        if edited_df:
            # 키(시작컴포넌트)의 메타 인덱스가 node_id와 같다면 해당 키의 벨류(도착 컴포넌트의 확률)을 전달
            for key in list(edited_df.keys()):
                if self.node_id == self.comp_to_idx[start_component][key]:
                    prob = np.array(
                        list(edited_df[key].values()),
                        dtype=float,
                    )
                else:
                    prob = np.array(
                        [
                            self.destination_choices[i]
                            for i in available_destination_nodes
                        ],
                        dtype=float,
                    )
        else:
            prob = np.array(
                [self.destination_choices[i] for i in available_destination_nodes],
                dtype=float,
            )

        # FIXME: destination_nodes가 아니라 available_destination_nodes가 아닐까?
        return destination_nodes[np.random.choice(len(prob), p=self.normalize(prob))]

    def _prod_que(self, second, minute, nodes, passengers):
        while self.passenger_queues and self.passenger_queues[0][0] <= second:
            counter_num = self.select_facility(minute)

            if counter_num is None:
                break

            passenger_node_id = self.passenger_queues[0][1]
            passenger_id = self.passenger_ids[passenger_node_id]

            priority_destination_node_indices = None
            start_component = self.components[0]
            destination_component = self.components[1]

            # 기존 코드와 동일
            if destination_component:
                passenger = passengers.loc[passenger_id]
                for process in self.processes.values():
                    if process.name == destination_component:
                        priority_matrix = process.priority_matrix
                        break

                edited_df = None
                if priority_matrix:
                    for priority in priority_matrix:

                        conditions = priority.condition

                        for condition in conditions:
                            criteria = condition.criteria
                            criteria = COL_FILTER_MAP[criteria]
                            value = condition.value

                            check = passenger[criteria] in value
                            if not check:
                                break  # check = False로 내보낸다.

                        # check가 false이면 이건 돌지 않기 때문에 다음 컨디션을 확인해야함.
                        if check:
                            edited_df = priority.matrix
                            break

                if edited_df:
                    priority_destination_node_indices = (
                        [
                            self.comp_to_idx[destination_component][idx]
                            for idx in list(list(edited_df.values())[0].keys())
                        ]
                        if self.node_id
                        in [
                            self.comp_to_idx[start_component][key]
                            for key in list(edited_df.keys())
                        ]
                        else None
                    )

            priority_destination_nodes = (
                [nodes[idx] for idx in priority_destination_node_indices]
                if priority_destination_node_indices
                else None
            )
            destination_nodes = priority_destination_nodes or self.destinations

            if destination_nodes is not None:
                # NOTE: 현재 목적지의 모든 노드가 꽉 찬 상태
                if all(
                    len(node.passenger_queues) >= node.max_capacity
                    for node in destination_nodes
                ):
                    self.unoccupied_facilities[counter_num - 1] = 1
                    break

            # ==================================================
            heapq.heappop(self.passenger_queues)

            # ==================================================
            adjusted_processing_time = self.adjust_processing_time(
                processing_time=self.processing_config[minute][counter_num - 1]
            )
            self.processing_time[passenger_node_id] = adjusted_processing_time

            heapq.heappush(
                self.occupied_facilities,
                (second + adjusted_processing_time, passenger_node_id, counter_num - 1),
            )

    def select_facility(self, minute) -> int | None:
        current_selection = self.selection_config[minute]

        available_facility_indices = np.where(
            np.multiply(current_selection, self.unoccupied_facilities) > 0
        )[0]

        if len(available_facility_indices) < 1:
            return None

        if self.is_deterministic:
            index = 0
        else:
            probabilities = self.normalize(
                current_selection[available_facility_indices]
            )
            index = np.random.choice(len(available_facility_indices), p=probabilities)

        # NOTE: 선택된 시설(facility)의 가용 여부를 변경한다.
        self.unoccupied_facilities[available_facility_indices[index]] = 0

        facility_index = available_facility_indices[index] + 1
        return facility_index

    def adjust_processing_time(self, processing_time, sigma_multiple=1):
        if processing_time is None or processing_time == 0:
            return processing_time

        sigma = np.sqrt(processing_time)

        if sigma < 1:
            return processing_time

        adjusted_processing_time = int(
            round(np.random.normal(processing_time, sigma * sigma_multiple))
        )
        # return max(adjusted_processing_time, processing_time // 2)
        return processing_time

    def test_pt(self, pt_orig, sigma_multiple=0):
        sigma = np.sqrt(pt_orig)
        pt = int(round(np.random.normal(pt_orig, sigma * sigma_multiple)))
        pt = max(pt, pt_orig // 2)
        return pt

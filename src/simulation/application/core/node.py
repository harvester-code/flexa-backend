import heapq
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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
        self.facility_numbers = np.zeros(num_passengers, dtype=int) - 1
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

            #######################################################################################
            done_time, passenger_node_id, facility_number = heapq.heappop(
                self.occupied_facilities
            )
            self.done_time[passenger_node_id] = second
            self.processing_time[passenger_node_id] += (
                second - done_time
            )  ## 박경훈 추가 ##

            self.unoccupied_facilities[facility_number] = 1
            if self.destinations is None:
                # break
                continue  ## 박경훈 추가 : break가 아니라 continue로 해야한다. ##
            #######################################################################################

            # 전체 프로세스에서의 승객 ID
            passenger_id = self.passenger_ids[passenger_node_id]
            destination = self.select_destination(
                nodes, passengers, passenger_id, second
            )

            if destination:
                destination.passenger_ids.append(passenger_id)

                # NOTE: destination의 승객 노드 ID
                _passenger_node_id = destination.passenger_node_id

                # TODO: 아래 코드 위치를 수정해보기
                destination.passenger_node_id += 1

                destination.on_time[_passenger_node_id] = self.done_time[
                    passenger_node_id
                ]

                minute_of_day = min(
                    1439, (destination.on_time[_passenger_node_id] % 86400) // 60
                )
                destination_passenger_id = destination.passenger_ids[_passenger_node_id]
                destination_facility_number = destination.select_facility(
                    minute=minute_of_day
                )

                destination.facility_numbers[_passenger_node_id] = (
                    destination_facility_number
                )

                # dod: destination of destination
                dod_component = destination.components[1]
                # 기존 도착 컴포넌트
                destination_component = destination.components[0]
                priority_dod_node_indices = None
                if dod_component:
                    priority_matrix = None
                    passenger = passengers.loc[destination_passenger_id]
                    # 기존 코드와 동일
                    for process in self.processes.values():
                        if process.name == dod_component:
                            priority_matrix = process.priority_matrix
                            break

                    edited_df = None
                    # NOTE: 상위 매트릭스부터 확인하면서 모든 condition을 만족할 시 해당 매트릭스의 값을 가져옴.
                    if priority_matrix:
                        for priority in priority_matrix:

                            conditions = priority.condition
                            check = all(
                                self.check_condition(
                                    passenger, condition, destination_component, second
                                )
                                for condition in conditions
                            )

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

                # ======================================================
                # NOTE: 도착지의 가용가능한 기기가 없을 경우(= 줄이서있는 상황) que_history 넣기
                if destination.unoccupied_facilities.sum() > 0:
                    destination.que_history[_passenger_node_id] = len(
                        destination.passenger_queues
                    )
                # ======================================================
                if destination_facility_number == 0:
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
                    destination.unoccupied_facilities[
                        destination_facility_number - 1
                    ] = 1
                    heapq.heappush(
                        destination.passenger_queues,
                        (destination.on_time[_passenger_node_id], _passenger_node_id),
                    )
                    destination.que_history[_passenger_node_id] = len(
                        destination.passenger_queues
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

                # ======================================================
                if destination.unoccupied_facilities.sum() == 0:
                    destination.que_history[_passenger_node_id] = len(
                        destination.passenger_queues
                    )

    def select_destination(self, nodes, df_pax, pax_idx, second):

        # edited_df가 컬럼에 없기때문에 시작 컴포넌트가 필요
        start_component = self.components[0]
        destination_component = self.components[1]

        priority_destination_node_indices = None
        if destination_component:
            priority_matrix = None
            passenger = df_pax.loc[pax_idx]
            # 기존과 같은 방식으로 matrix을 가져온다.
            for process in self.processes.values():
                if process.name == destination_component:
                    priority_matrix = process.priority_matrix
                    break

            edited_df = None
            # NOTE: 상위 매트릭스부터 확인하면서 모든 condition을 만족할 시 해당 매트릭스의 값을 가져옴.
            if priority_matrix:
                for priority in priority_matrix:

                    conditions = priority.condition
                    check = all(
                        self.check_condition(
                            passenger, condition, start_component, second
                        )
                        for condition in conditions
                    )

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

        # available_destination_node_ids = [
        #     i
        #     for i, node in enumerate(destination_nodes)
        #     if len(node.passenger_queues) < node.max_capacity
        # ]

        # available_destination_nodes = [
        #     node
        #     for i, node in enumerate(destination_nodes)
        #     if len(node.passenger_queues) < node.max_capacity
        # ]
        available_destination_node_ids = []
        available_destination_nodes = []
        for i, node in enumerate(destination_nodes):
            if len(node.passenger_queues) < node.max_capacity:
                available_destination_node_ids.append(i)  # [0,2,3]
                available_destination_nodes.append(
                    node
                )  # [object(0), object(2), object(3)]

        # for i, node in enumerate(destination_nodes):
        #     print(
        #         "nodenum",
        #         node.node_label,
        #         "/pax_queue",
        #         len(node.passenger_queues),
        #         "/max_queue",
        #         node.max_capacity,
        #         "/selected_node",
        #     )
        #     if len(node.passenger_queues) < node.max_capacity:
        #         print("요놈1", node.node_label)

        if len(available_destination_node_ids) == 0:

            # NOTE : available_dstination_node_ids가 0이어도 목적지에 보내야 한다
            # 그 이유는 만약 가면 안되는 것이 있다면 prod_que에서 걸러졌을 것이다.
            # 즉 가면 안되는 것은 prod_que에서 facility를 아예 안보낼 것인데,
            # 만약 prod_que에서 heapq.heappush가 되었다면 뭐라고 뽑아주는 것이 맞다.
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
                            for i in available_destination_node_ids
                        ],
                        dtype=float,
                    )
        else:
            prob = np.array(
                [self.destination_choices[i] for i in available_destination_node_ids],
                dtype=float,
            )
            # available_destination_node_ids = [0,2,3]
            # self.destination_choices = [0.1,0.15,0.2,0.25,0.3]
            # prob = [0.1, 0.2, 0.25]

        # print(
        #     "요놈2",
        #     available_destination_nodes[
        #         np.random.choice(len(prob), p=self.normalize(prob))
        #     ].node_label,
        # )

        return available_destination_nodes[
            np.random.choice(len(prob), p=self.normalize(prob))
        ]

    def _prod_que(self, second, minute, nodes, passengers):
        while self.passenger_queues and self.passenger_queues[0][0] <= second:
            counter_num = self.select_facility(minute)

            if counter_num == 0:
                break  # prod_que는 prod_counter와는 달리 continue를 안해도 된다. 왜냐하면 그당시 줄서있는 사람이 너무나 많기 때문이다. 그당시 카운터가 없었으면 그냥 그시간대는 넘어가는 것으로로

            passenger_node_id = self.passenger_queues[0][1]
            passenger_id = self.passenger_ids[passenger_node_id]
            self.facility_numbers[passenger_node_id] = counter_num
            priority_destination_node_indices = None
            start_component = self.components[0]
            destination_component = self.components[1]
            adjusted_processing_time = self.adjust_processing_time(
                processing_time=self.processing_config[minute][counter_num - 1]
            )

            # 기존 코드와 동일
            if destination_component:
                priority_matrix = None
                passenger = passengers.loc[passenger_id]
                for process in self.processes.values():
                    if process.name == destination_component:
                        priority_matrix = process.priority_matrix
                        break

                edited_df = None
                # NOTE: 상위 매트릭스부터 확인하면서 모든 condition을 만족할 시 해당 매트릭스의 값을 가져옴.
                if priority_matrix:
                    for priority in priority_matrix:

                        conditions = priority.condition

                        check = all(
                            self.check_condition(
                                passenger,
                                condition,
                                start_component,
                                second + adjusted_processing_time,
                            )
                            for condition in conditions
                        )

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
                    len(node.passenger_queues) >= (node.max_capacity - 3)
                    for node in destination_nodes
                ):
                    self.unoccupied_facilities[counter_num - 1] = 1
                    break  # prod_que는 prod_counter와는 달리 continue를 안해도 된다. 왜냐하면 그당시 줄서있는 사람이 너무나 많기 때문이다. 그당시 카운터가 없었으면 그냥 그시간대는 넘어가는 것으로로

            # ==================================================
            heapq.heappop(self.passenger_queues)

            # ==================================================
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

        if len(available_facility_indices) == 0:
            return 0

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

    def get_movement_time(self, processes: dict, destination_component):

        for process in processes.values():
            if process.name == destination_component:

                movement_time = int(process.wait_time) * 60
                movement_time = self.adjust_processing_time(movement_time)

        return movement_time

    def check_condition(self, passenger, condition, component, check_time):
        criteria = condition.criteria

        if criteria == component:
            return True

        elif criteria == "time":
            check_time = (datetime.min + timedelta(seconds=int(check_time))).time()
            operator = condition.operator
            condition_time = datetime.strptime(condition.value[0], "%H:%M")

            if operator == "start":
                return check_time >= condition_time.time()
            if operator == "end":
                return check_time <= condition_time.time()

        else:
            criteria_col = COL_FILTER_MAP.get(criteria, None)
            if not criteria_col:
                return False

            return passenger[criteria_col] in condition.value

import asyncio
import heapq
import math
import time
from datetime import datetime

import numpy as np
from fastapi import WebSocket
from loguru import logger

from src.constants import COL_FILTER_MAP
from src.simulation.application.core.graph import DsGraph


class DsSimulator:
    def __init__(
        self,
        components,
        ds_graph: DsGraph,
        passengers,
        showup_times,
        source_per_passengers,
        source_transition_graph,
    ):
        if not isinstance(ds_graph, DsGraph):
            raise TypeError("Expected an instance of DsGraph.")

        self.components = components
        self.ds_graph = ds_graph
        self.passengers = passengers
        self.showup_times = showup_times
        self.source_per_passengers = source_per_passengers
        self.source_transition_graph = source_transition_graph
        # ==========
        self.num_passengers = len(showup_times)
        self.passenger_id = 0
        self.processes = ds_graph.processes
        self.comp_to_idx = ds_graph.comp_to_idx

    def check_condition(self, passenger, condition):
        criteria = condition.criteria

        if criteria == "Time":
            criteria_col = "show_up_time"
            operator = condition.operator
            condition_time = datetime.strptime(condition.value, "%H:%M")
            if operator == "start":
                return passenger[criteria_col].time() >= condition_time.time()
            if operator == "end":
                return passenger[criteria_col].time() <= condition_time.time()

        else:
            criteria_col = COL_FILTER_MAP.get(criteria, None)
            if not criteria_col:
                return False

            return passenger[criteria_col] in condition.value

    # FIXME: 해당 메서드는 DsSimulator에 위치하는게 아니라 DsGraph에 있어야하는게 아닐까?
    def add_flow(self, current_second: int, greedy: bool = False):
        first_component = self.components[0]
        comp_to_idx = self.comp_to_idx[first_component]

        # 해당 프로세스의 priority_matrix를 찾는 작업
        for process in self.processes.values():
            if process.name == first_component:
                priority_matrix = process.priority_matrix
                break

        while (
            self.passenger_id < self.num_passengers
            and self.showup_times[self.passenger_id] <= current_second
        ):

            target_source_key = self.source_per_passengers[self.passenger_id]
            passenger = self.passengers.loc[self.passenger_id]

            edited_df = None
            # NOTE: 상위 매트릭스부터 확인하면서 모든 condition을 만족할 시 해당 매트릭스의 값을 가져옴.
            if priority_matrix:
                for priority in priority_matrix:

                    conditions = priority.condition

                    check = all(
                        self.check_condition(passenger, condition)
                        for condition in conditions
                    )

                    if check:
                        edited_df = priority.matrix
                        break

            if not edited_df:
                destinations = self.source_transition_graph[target_source_key][0]
                probabilities = self.source_transition_graph[target_source_key][1]
            else:
                destinations = []
                probabilities = []

                # 위에 edited_df가 없는 경우의 destinations probabilities 값과 같은 형식으로 배출되도록 변경
                for key, value in edited_df[target_source_key].items():
                    if value > 0:
                        destinations.append(key)
                        probabilities.append(value)

                destinations = np.array([comp_to_idx[key] for key in destinations])
                probabilities = np.array(probabilities)

            if greedy:
                destination_nodes = [self.ds_graph.nodes[d] for d in destinations]
                node = min(
                    destination_nodes, key=lambda node: len(node.passenger_queues)
                )

            else:
                node = self.ds_graph.nodes[
                    destinations[np.random.choice(len(probabilities), p=probabilities)]
                ]

            # ============================================================
            if node.unoccupied_facilities.sum() == 0:
                heapq.heappush(
                    node.passenger_queues,
                    (self.showup_times[self.passenger_id], node.passenger_node_id),
                )
                node.que_history[node.passenger_node_id] = len(node.passenger_queues)
            else:
                node.que_history[node.passenger_node_id] = len(node.passenger_queues)
                heapq.heappush(
                    node.passenger_queues,
                    (self.showup_times[self.passenger_id], node.passenger_node_id),
                )

            node.passenger_ids.append(self.passenger_id)
            node.on_time[node.passenger_node_id] = current_second

            # ============================================================
            self.passenger_id += 1
            node.passenger_node_id += 1

    async def run(self, websocket: WebSocket, start_time, end_time, unit=10):
        logger.info("시뮬레이션을 시작합니다.")
        start_at = time.time()
        previous_progress = 35
        start_progress = 35
        end_progress = 94

        # 매 초마다 소스 데이터를 시작으로 마지막 컴포넌트까지 돌고 오는 방식이다.
        for current_second in range(start_time, end_time + 1, unit):
            minute_of_day = (current_second % 86400) // 60

            # 1. 소스 데이터에서 첫번째 컴포넌트까지
            self.add_flow(current_second=current_second)

            # 2. 첫번째 컴포넌트부터 마지막 컴포넌트까지
            self.ds_graph.prod(
                second=current_second,
                minute=minute_of_day,
                passengers=self.passengers,
            )

            progress_time = start_progress + (current_second / end_time) * (
                end_progress - start_progress
            )
            progress = math.floor(progress_time)
            if progress > previous_progress:
                await websocket.send_json({"progress": f"{progress}%"})
                await asyncio.sleep(0.001)
                previous_progress = progress

        logger.info(
            f"시뮬레이션을 종료합니다. (소요 시간: {round(time.time() - start_at)}초)"
        )

    async def run_test(self, start_time, end_time, unit=1):
        logger.info("시뮬레이션을 시작합니다.")
        start_at = time.time()

        # 매 초마다 소스 데이터를 시작으로 마지막 컴포넌트까지 돌고 오는 방식이다.
        for current_second in range(start_time, end_time + 1, unit):
            minute_of_day = (current_second % 86400) // 60

            # 1. 소스 데이터에서 첫번째 컴포넌트까지
            self.add_flow(current_second=current_second)

            # 2. 첫번째 컴포넌트부터 마지막 컴포넌트까지
            self.ds_graph.prod(
                second=current_second,
                minute=minute_of_day,
                passengers=self.passengers,
            )

        logger.info(
            f"시뮬레이션을 종료합니다. (소요 시간: {round(time.time() - start_at)}초)"
        )

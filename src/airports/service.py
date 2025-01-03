from datetime import datetime, timedelta
from json import loads
from typing import Annotated

import numpy as np
import pandas as pd
from fastapi import Depends
from sqlalchemy import Engine

from src.airports.queries import SELECT_AIRPORT_ARRIVAL, SELECT_AIRPORT_DEPARTURE
from src.database import get_snowflake_session

SessionDep = Annotated[Engine, Depends(get_snowflake_session)]


class AirportService:
    @staticmethod
    def fetch_general_declarations(date, airport, flight_io, session: SessionDep):
        with session.connect() as connection:
            if flight_io == "arrival":
                df = pd.read_sql(
                    SELECT_AIRPORT_ARRIVAL.format(airport=airport, date=date),
                    connection,
                )

            if flight_io == "departure":
                df = pd.read_sql(
                    SELECT_AIRPORT_DEPARTURE.format(airport=airport, date=date),
                    connection,
                )

        result = loads(df.to_json(orient="records"))
        return result

    @staticmethod
    def show_up(inputs):
        df = pd.DataFrame([dict(row) for row in inputs.inputs["data"]])

        col_map = {
            "Airline": "operating_carrier_iata",
            "Region": "df_region",
            "Country": "df_country",
            "Airport": "df_airport",
            "Flight_number": "flight_number",
        }

        pax_df = pd.DataFrame()
        for filter in inputs.inputs["filters"]:
            filtered_df = df.copy()

            for condition in filter["conditions"]:
                filtered_df = filtered_df[
                    filtered_df[col_map[condition["criteria"]]].isin(condition["value"])
                ]

            # ========================
            # filtered_df 뻥튀기 코드 (input: 평균 / 분산)
            seats_80_percent = (
                filtered_df["total_seat_count"].fillna(0).astype(int) * 0.8
            )
            partial_pax_df = filtered_df.loc[
                filtered_df.index.repeat(seats_80_percent)
            ].reset_index(drop=True)

            departure_times = pd.to_datetime(
                partial_pax_df["scheduled_gate_departure_local"]
            )
            random_minutes = np.random.normal(
                loc=-90, scale=30, size=len(partial_pax_df)
            )
            partial_pax_df["showup_time"] = departure_times + pd.to_timedelta(
                random_minutes, unit="m"
            )
            # ========================

            pax_df = pd.concat([pax_df, partial_pax_df], ignore_index=True)
            df.drop(index=filtered_df.index, inplace=True)

        # short_df = pax_df[["FLIGHT_ID", "showup_time"]]
        # return loads(short_df.to_json(orient="records"))

        ## 그래프용 코드 ##

        def count_rows_by_time(df, time_column):
            # 00:00부터 23:59까지의 모든 시간을 생성
            times = []
            counts = []

            start_time = datetime.strptime("00:00", "%H:%M")
            end_time = datetime.strptime("23:59", "%H:%M")

            current_time = start_time
            while current_time <= end_time:
                time_str = current_time.strftime("%H:%M")
                count = len(df[df[time_column].dt.strftime("%H:%M") == time_str])

                times.append(time_str)
                counts.append(count)

                current_time += timedelta(minutes=1)

            # 결과 데이터프레임 생성
            # result_df = pd.DataFrame({"인원수": counts}, index=times)
            result_df = pd.DataFrame({"시간": times, "인원수": counts})
            return result_df

        # 시간 열('time')을 기준으로 행 개수 계산
        result_df = count_rows_by_time(pax_df.head(500), "showup_time")

        return loads(result_df.to_json(orient="records"))

    ##########################################
    # NOTE: 이 아래로 매서드 구분을 해놨습니다

    def create_choice_matrix(self, inputs):

        # show-up에서 만드는 코드를 사용해서 데이터 프레임 반환
        df_pax = self._show_up(inputs)

        # 초이스 매트릭스 부분만 가져오기
        facility_detail = inputs.inputs["facility_detail"]

        # 초이스 매트릭스 run
        df_pax = self._add_columns(facility_detail, df_pax)

        sanky = self._create_sankey_data(facility_detail, df_pax)
        capacity = self._capacity_chart(df_pax, node_list=facility_detail["1"]["nodes"])

        return {"sanky": sanky, "capacity": capacity}

    def _show_up(self, inputs):
        df = pd.DataFrame([dict(row) for row in inputs.inputs["data"]])

        col_map = {
            "Airline": "operating_carrier_iata",
            "Region": "df_region",
            "Country": "df_country",
            "Airport": "df_airport",
            "Flight_number": "flight_number",
        }

        pax_df = pd.DataFrame()
        # df = df[df["operating_carrier_iata"].isin(["KE", "OZ"])]
        for filter in inputs.inputs["filters"]:
            filtered_df = df.copy()

            for condition in filter["conditions"]:
                filtered_df = filtered_df[
                    filtered_df[col_map[condition["criteria"]]].isin(condition["value"])
                ]

            # ========================
            # filtered_df 뻥튀기 코드 (input: 평균 / 분산)
            seats_80_percent = (
                filtered_df["total_seat_count"].fillna(0).astype(int) * 0.8
            )
            partial_pax_df = filtered_df.loc[
                filtered_df.index.repeat(seats_80_percent)
            ].reset_index(drop=True)

            departure_times = pd.to_datetime(
                partial_pax_df["scheduled_gate_departure_local"]
            )
            random_minutes = np.random.normal(
                loc=-90, scale=30, size=len(partial_pax_df)
            )
            partial_pax_df["showup_time"] = departure_times + pd.to_timedelta(
                random_minutes, unit="m"
            )
            # ========================

            pax_df = pd.concat([pax_df, partial_pax_df], ignore_index=True)
            df.drop(index=filtered_df.index, inplace=True)

        return pax_df

    def _sample_node(self, row, edited_df):
        """
        Sample a node based on the probabilities from the choice matrix.
        :param row: The row of the DataFrame to sample from
        :param edited_df: The choice matrix DataFrame with probabilities
        :return: The sampled node
        """
        probabilities = edited_df.loc[row]
        return np.random.choice(probabilities.index, p=probabilities.values)

    def _create_sankey_data(self, facility_detail, pax_df):
        sankey_data = {"nodes": [], "links": []}
        node_ids = set()  # 중복 제거를 위한 세트

        for key, value in list(facility_detail.items())[1:]:
            # 노드 추가
            for node in value["nodes"]:
                if node not in node_ids:
                    sankey_data["nodes"].append({"id": node, "name": node})
                    node_ids.add(node)

            # 링크 추가
            source_nodes = facility_detail[key]["nodes"]
            destination = facility_detail[key]["destination"]
            if destination:
                source_name = facility_detail[key]["name"]
                destination_name = facility_detail[destination]["name"]

                target_nodes = facility_detail[destination]["nodes"]
                for source_node in source_nodes:
                    for target_node in target_nodes:

                        value = pax_df[
                            (pax_df[f"{source_name}_component"] == source_node)
                            & (pax_df[f"{destination_name}_component"] == target_node)
                        ]

                        sankey_data["links"].append(
                            {
                                "source": source_node,
                                "target": target_node,
                                "value": len(value),
                            }
                        )

        return sankey_data

    def _capacity_chart(self, df_pax, node_list):
        from datetime import time

        times = [
            time(hour=hour, minute=minute)
            for hour in range(24)
            for minute in range(0, 60, 10)
        ]
        time_df = pd.DataFrame({"index": times})
        time_df["index"] = time_df["index"].astype(str)

        fin_dict = {}

        for node in node_list:
            df_pax_filtered = df_pax[df_pax["checkin_component"] == node]
            df_pax_filtered["showup_time"] = df_pax_filtered["showup_time"].dt.floor(
                "10T"
            )

            grouped = df_pax_filtered["showup_time"].value_counts().reset_index()
            grouped["index"] = grouped["showup_time"].astype(str).str[-8:]

            total_capa = pd.merge(time_df, grouped, on="index", how="left")
            total_capa["count"] = total_capa["count"].fillna(0)
            total_capa = total_capa.drop("showup_time", axis=1)

            total_dict = loads(total_capa.to_json(orient="records"))

            fin_dict[node] = total_dict

        return fin_dict

    def _add_columns(self, facility_detail, df_pax):
        """
        Add columns to the DataFrame based on the choice matrices for each process transition.
        :param return_dict: Dictionary containing the choice matrices and the passenger DataFrame
        :return: Updated return_dict with added columns to the passenger DataFrame
        """

        # Iterate over each process transition in the choice matrix
        for facility in list(facility_detail.keys())[1:]:

            source_num = facility_detail[facility]["source"]

            vertical_process = facility_detail[source_num][
                "name"
            ]  # 시작점 ex. operating_carrier_iata
            horizontal_process = facility_detail[facility][
                "name"
            ]  # 도착점 ex. check-in

            # 매트릭스 테이블 df 만들기
            edited_df = pd.DataFrame.from_dict(
                facility_detail[facility]["filters"][-1]["matricx"], orient="index"
            )

            # 매트릭스로 새로운 컬럼 생성
            if facility == "1":  # is root
                df_pax[f"{horizontal_process}_component"] = df_pax[
                    vertical_process
                ].apply(self._sample_node, args=(edited_df.fillna(0),))
            else:
                df_pax[f"{horizontal_process}_component"] = df_pax[
                    f"{vertical_process}_component"
                ].apply(self._sample_node, args=(edited_df.fillna(0),))

            df_pax[f"{horizontal_process}_edited_df"] = None

        return df_pax

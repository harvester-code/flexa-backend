from datetime import datetime, timedelta
from json import loads

import numpy as np
import pandas as pd
from sqlalchemy import Connection, text
from sqlalchemy.exc import SQLAlchemyError

from src.airports.schema import (
    GeneralDeclarationArrival,
    GeneralDeclarationDeparture,
    ChoiceMatricxBody,
    ShowupBody,
)
from src.airports.queries import SELECT_AIRPORT_ARRIVAL, SELECT_AIRPORT_DEPARTURE


class AirportService:
    @staticmethod
    async def fetch_general_declarations(date, airport, flight_io, conn: Connection):
        try:
            query_map = {
                "arrival": SELECT_AIRPORT_ARRIVAL,
                "departure": SELECT_AIRPORT_DEPARTURE,
            }
            schema_map = {
                "arrival": GeneralDeclarationArrival,
                "departure": GeneralDeclarationDeparture,
            }

            stmt = text(query_map.get(flight_io))
            params = {
                "airport": airport,
                "date": date,
            }

            result = conn.execute(stmt, params)

            rows = [schema_map.get(flight_io)(**row._mapping) for row in result]
            return rows
        # TODO: 에러핸들링 개선
        except SQLAlchemyError as err:
            raise err
        except Exception as err:
            raise err
        finally:
            result.close()

    def create_show_up(self, item: ShowupBody):
        pax_df = self.show_up_pattern(
            data=item.data,
            destribution_conditions=item.destribution_conditions,
        )

        # Condition 에 따른 Distribution 좌표 생성
        distribution_xy_coords = self._create_normal_distribution(
            item.destribution_conditions
        )

        # Show up 그래프용 데이터
        show_up_summary = self._create_show_up_summary(pax_df, interval_minutes=60)

        # 시간 포맷 변경
        pax_df["show_up_time"] = pax_df["show_up_time"].dt.strftime("%Y-%m-%dT%H:%M:%S")

        return (
            loads(pax_df.head().to_json(orient="records")),
            distribution_xy_coords,
            show_up_summary,
        )

    def create_choice_matrix(self, item: ChoiceMatricxBody):

        # show-up에서 만드는 코드를 사용해서 데이터 프레임 반환
        pax_df = self.show_up_pattern(
            data=item.data,
            destribution_conditions=item.destribution_conditions,
        )
        # 초이스 매트릭스 부분만 가져오기
        facility_detail = item.processes

        # 초이스 매트릭스 run
        pax_df = self._add_columns(facility_detail, pax_df)

        sanky = self._create_sankey_data(facility_detail, pax_df)
        capacity = self._capacity_chart(pax_df, node_list=facility_detail["1"]["nodes"])

        return {"sanky": sanky, "capacity": capacity}

    def show_up_pattern(self, data: list, destribution_conditions: list):
        df = pd.DataFrame(data)
        col_map = {
            "International/Domestic": "flight_type",
            "Airline": "operating_carrier_iata",
            "Region": "region_name",
            "Country": "country_code",
        }

        pax_df = pd.DataFrame()
        for filter in destribution_conditions:
            partial_df = df.copy()
            for condition in filter.conditions:
                partial_df = partial_df[
                    partial_df[col_map[condition.criteria]].isin(condition.value)
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

    def _create_normal_distribution(self, destribution_conditions: list):
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

    def _create_show_up_summary(self, pax_df, interval_minutes=60):
        pax_df["time_group"] = pax_df["show_up_time"].dt.floor(f"{interval_minutes}min")
        counts_df = pax_df.groupby("time_group").size().reset_index()
        summary = {
            "times": counts_df["time_group"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist(),
            "counts": counts_df[0].tolist(),
        }
        return summary

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

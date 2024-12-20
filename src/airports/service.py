from datetime import datetime, timedelta
from json import loads

import numpy as np
import pandas as pd
from sqlalchemy import Connection, text
from sqlalchemy.exc import SQLAlchemyError

from src.airports.schema import GeneralDeclarationArrival, GeneralDeclarationDeparture
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
            print(err)
            raise err
        except Exception as err:
            print(err)
            raise err
        finally:
            result.close()

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

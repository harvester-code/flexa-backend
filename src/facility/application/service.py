from dependency_injector.wiring import inject
from src.facility.domain.repository import IFacilityRepository
import boto3
import pandas as pd
import numpy as np


class FacilityService:
    """
    //매서드 정의//

    """

    @inject
    def __init__(
        self,
        facility_repo: IFacilityRepository,
    ):
        self.facility_repo = facility_repo

    async def fetch_process_list(
        self,
        session: boto3.Session,
        user_id: str | None = None,
        scenario_id: str | None = None,
    ):

        # s3에서 시뮬레이션 데이터 프레임 가져오기
        # if scenario_id:
        #     filename = f"{user_id}/{scenario_id}"
        #     sim_df: pd.DataFrame = await self.facility_repo.download_from_s3(
        #         session, filename
        #     )

        # FIXME: 이후에 실제 시뮬레이션 데이터로 붙을 수 있도록 컨트롤러와 함께 수정
        sim_df = pd.read_csv("samples/test_sample.csv")

        process_columns = sim_df.columns[
            sim_df.columns.str.contains("pt_pred", case=False)
        ]

        result = []
        for process in process_columns:
            value = process.replace("_pt_pred", "")
            label = value.replace("_", " ").capitalize().replace("Checkin", "Check-in")

            process_result = {"label": label, "value": value}
            result.append(process_result)

        return result

    # ==============================================================
    # NOTE: KPI Summary

    # NOTE: 공용로직들
    async def _create_throughput(
        self, sim_df: pd.DataFrame, group_column, process
    ) -> pd.DataFrame:
        end_time = f"{process}_pt_pred"

        df = sim_df.dropna(subset=[end_time]).copy()

        df[end_time] = pd.to_datetime(df[end_time])
        df.loc[:, end_time] = df[end_time].dt.floor("10min")

        df_grouped = df.groupby([end_time, group_column]).size().unstack(fill_value=0)
        df_grouped = df_grouped.sort_index()

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="10min",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)
        # print(df_grouped)  # 히트맵
        # print(df_grouped.sum(axis=1))  # 차트
        # print(df_grouped.sum())  # 테이블
        # print(df_grouped.sum().mean())  # 테이블에서 평균(ALL)

        return df_grouped

    async def _create_queue_length(
        self, sim_df: pd.DataFrame, process, group_column, func: str = "mean"
    ) -> pd.DataFrame:
        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"
        process_que = f"{process}_que"

        sim_df[start_time] = pd.to_datetime(sim_df[start_time])
        sim_df[end_time] = pd.to_datetime(sim_df[end_time])
        df = sim_df[[start_time, end_time, group_column, process_que]].copy()

        df.loc[:, end_time] = df[end_time].dt.floor("10min")

        if func == "top5":
            func = lambda x: np.percentile(x, 95)

        if func == "bottom5":
            func = lambda x: np.percentile(x, 5)

        df_grouped = (
            df.groupby([end_time, group_column], as_index=False)
            .agg({process_que: func})
            .pivot_table(
                index=end_time,
                columns=group_column,
                values=process_que,
                fill_value=0,
            )
        )

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="10min",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        # print("=================")
        # print(df_grouped)  # 히트맵
        # print(df_grouped.mean(axis=1))  # 차트
        # print(df_grouped.mean())  # 테이블
        # print(df_grouped.mean().mean())  # 테이블에서 평균(ALL)

        return df_grouped

    async def _create_waiting_time(
        self, sim_df: pd.DataFrame, process, group_column, func: str = "mean"
    ) -> pd.DataFrame:
        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"

        sim_df[start_time] = pd.to_datetime(sim_df[start_time])
        sim_df[end_time] = pd.to_datetime(sim_df[end_time])
        df = sim_df[[start_time, end_time, group_column]].copy()

        df["waiting_time"] = (df[end_time] - df[start_time]).dt.total_seconds()

        df.loc[:, end_time] = df[end_time].dt.floor("10min")

        if func == "top5":
            func = lambda x: np.percentile(x, 95)  # 지금은 단일 지정값
            # lambda x: np.sum(x[x >= np.percentile(x, 95)]) 상위의 평균

        if func == "bottom5":
            func = lambda x: np.percentile(x, 5)
            # lambda x: np.sum(x[x <= np.percentile(x, 5)]) 하위의 평균

        df_grouped = (
            df.groupby([end_time, group_column], as_index=False)
            .agg({"waiting_time": func})
            .pivot_table(
                index=end_time,
                columns=group_column,
                values="waiting_time",
                fill_value=0,
            )
        )

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="10min",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        # print("=================")
        # print(df_grouped)  # 히트맵
        # print(df_grouped.mean(axis=1))  # 차트
        # print(df_grouped.mean())  # 테이블
        # print(df_grouped.mean().mean())  # 테이블에서 평균(ALL)

        return df_grouped

    async def _create_facility_efficiency(
        self, sim_df: pd.DataFrame, process
    ) -> pd.DataFrame:
        end_time = f"{process}_pt_pred"
        group_column = f"{process}_pred"

        sim_df[end_time] = pd.to_datetime(sim_df[end_time])
        sim_df.loc[:, end_time] = sim_df[end_time].dt.floor("10min")
        df = sim_df[[end_time, group_column]].copy()

        df_grouped = df.groupby([end_time, group_column]).size().unstack(fill_value=0)
        df_grouped[df_grouped > 0] = 1
        df_grouped = df_grouped.sort_index()

        start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
        end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
        all_hours = pd.date_range(
            start=pd.Timestamp(start_day),
            end=pd.Timestamp(end_day),
            freq="10min",
        )
        df_grouped = df_grouped.reindex(all_hours, fill_value=0)

        return df_grouped

    # NOTE: KPI
    async def generate_kpi(
        self,
        session: boto3.Session,
        process: str,
        func: str = "mean",
        user_id: str | None = None,
        scenario_id: str | None = None,
    ):

        # s3에서 시뮬레이션 데이터 프레임 가져오기
        # if scenario_id:
        #     filename = f"{user_id}/{scenario_id}"
        #     sim_df: pd.DataFrame = await self.facility_repo.download_from_s3(
        #         session, filename
        #     )

        kpi_result = {"header": {"columns": [], "subColumns": []}, "body": []}
        # FIXME: 이후에 실제 시뮬레이션 데이터로 붙을 수 있도록 컨트롤러와 함께 수정
        sim_df = pd.read_csv("samples/test_sample.csv")
        node_list: list = sim_df[f"{process}_pred"].unique().tolist()

        kpi_result["header"]["columns"].append({"label": "KPI"})
        kpi_result["header"]["columns"].append({"label": "AVERAGE"})
        kpi_result["header"]["columns"].append(
            {"label": process, "colSpan": len(node_list)}
        )

        for node in node_list:
            kpi_result["header"]["subColumns"].append({"label": node})

        # TP
        tp_data = await self._create_throughput(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        tp_all = round(tp_data.sum().mean())
        tp_list = tp_data.sum().astype(int).values.tolist()
        tp_list.insert(0, tp_all)

        kpi_result["body"].append(
            {"label": "Throughput", "unit": "pax", "values": tp_list}
        )

        # QL
        ql_data = await self._create_queue_length(
            sim_df=sim_df, process=process, group_column=f"{process}_pred", func=func
        )
        ql_all = round(ql_data.mean().mean())
        ql_list = ql_data.mean().astype(int).values.tolist()
        ql_list.insert(0, ql_all)

        kpi_result["body"].append(
            {"label": "Queue Length", "unit": "pax", "values": ql_list}
        )

        # WT
        wt_data = await self._create_waiting_time(
            sim_df=sim_df, process=process, group_column=f"{process}_pred", func=func
        )
        wt_all = round(wt_data.mean().mean())
        wt_list = wt_data.mean().astype(int).values.tolist()
        wt_list.insert(0, wt_all)

        kpi_result["body"].append(
            {"label": "Waiting Time", "unit": "sec", "values": wt_list}
        )

        # FE
        fe_data = await self._create_facility_efficiency(sim_df, process)
        fe_all = round(((fe_data.sum(axis=1) / 4) * 100).mean())
        column_list = fe_data.columns.tolist()
        fe_list = []
        for column in column_list:
            num_ones = (fe_data[column] == 1).sum()

            total_rows = len(fe_data)
            ratio = round(num_ones / total_rows * 100)
            fe_list.append(ratio)

        fe_list.insert(0, fe_all)

        kpi_result["body"].append(
            {"label": "Facility Efficiency", "unit": "%", "values": fe_list}
        )

        return kpi_result

    # NOTE: KPI Summary Chart
    async def generate_ks_chart(
        self,
        session: boto3.Session,
        process: str,
        user_id: str | None = None,
        scenario_id: str | None = None,
    ):

        # s3에서 시뮬레이션 데이터 프레임 가져오기
        # if scenario_id:
        #     filename = f"{user_id}/{scenario_id}"
        #     sim_df: pd.DataFrame = await self.facility_repo.download_from_s3(
        #         session, filename
        #     )
        chart_result = {}

        # FIXME: 이후에 실제 시뮬레이션 데이터로 붙을 수 있도록 컨트롤러와 함께 수정
        sim_df = pd.read_csv("samples/test_sample.csv")

        # TP
        tp_data = await self._create_throughput(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        tp = tp_data.sum(axis=1)
        tp_x_list = tp.index.astype(str).tolist()
        tp_y_list = tp.astype(int).values.tolist()

        chart_result["troughput"] = {"x": tp_x_list, "y": tp_y_list}

        # QL
        ql_data = await self._create_queue_length(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        ql = ql_data.mean(axis=1)
        ql_x_list = ql.index.astype(str).tolist()
        ql_y_list = ql.astype(int).values.tolist()

        chart_result["queue_length"] = {"x": ql_x_list, "y": ql_y_list}

        # WT
        wt_data = await self._create_waiting_time(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        wt = wt_data.mean(axis=1)
        wt_x_list = wt.index.astype(str).tolist()
        wt_y_list = wt.astype(int).values.tolist()

        chart_result["waiting_time"] = {"x": wt_x_list, "y": wt_y_list}

        # # FE
        # fe_data = await self._create_facility_efficiency(sim_df, process)
        # fe_data["fe"] = (fe_data.sum(axis=1) / 4) * 100
        # fe_x_list = fe_data.index.astype(str).tolist()
        # fe_y_list = fe_data["fe"].astype(int).values.tolist()

        # chart_result["facility_efficiency"] = {"x": fe_x_list, "y": fe_y_list}

        return chart_result

    # NOTE: HeatMap
    async def generate_heatmap(
        self,
        session: boto3.Session,
        process: str,
        user_id: str | None = None,
        scenario_id: str | None = None,
    ):

        # s3에서 시뮬레이션 데이터 프레임 가져오기
        # if scenario_id:
        #     filename = f"{user_id}/{scenario_id}"
        #     sim_df: pd.DataFrame = await self.facility_repo.download_from_s3(
        #         session, filename
        #     )
        heatmap_result = {}

        # FIXME: 이후에 실제 시뮬레이션 데이터로 붙을 수 있도록 컨트롤러와 함께 수정
        sim_df = pd.read_csv("samples/test_sample.csv")

        # TP
        tp = await self._create_throughput(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        tp_x_list = tp.index.astype(str).tolist()
        tp_y_list = []
        tp_z_list = []
        tp_column_list = tp.columns.tolist()
        for column in tp_column_list:
            tp_y_list.append(column)
            tp_z_list.append(tp[column].astype(int).values.tolist())

        heatmap_result["troughput"] = {"x": tp_x_list, "y": tp_y_list, "z": tp_z_list}

        # QL
        ql = await self._create_queue_length(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        ql_x_list = ql.index.astype(str).tolist()
        ql_y_list = []
        ql_z_list = []
        ql_column_list = ql.columns.tolist()
        for column in ql_column_list:
            ql_y_list.append(column)
            ql_z_list.append(ql[column].astype(int).values.tolist())

        heatmap_result["queue_length"] = {
            "x": ql_x_list,
            "y": ql_y_list,
            "z": ql_z_list,
        }

        # WT
        wt = await self._create_waiting_time(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        wt_x_list = wt.index.astype(str).tolist()
        wt_y_list = []
        wt_z_list = []
        wt_column_list = wt.columns.tolist()
        for column in wt_column_list:
            wt_y_list.append(column)
            wt_z_list.append(wt[column].astype(int).values.tolist())

        heatmap_result["waiting_time"] = {
            "x": wt_x_list,
            "y": wt_y_list,
            "z": wt_z_list,
        }

        return heatmap_result

    # ============================================================
    # NOTE: Passenger Analysis

    # NOTE: Pie Chart
    async def generate_pie_chart(self, session, process):

        # s3에서 시뮬레이션 데이터 프레임 가져오기
        # if scenario_id:
        #     filename = f"{user_id}/{scenario_id}"
        #     sim_df: pd.DataFrame = await self.facility_repo.download_from_s3(
        #         session, filename
        #     )

        # FIXME: 이후에 실제 시뮬레이션 데이터로 붙을 수 있도록 컨트롤러와 함께 수정
        sim_df = pd.read_csv("samples/test_sample.csv")

        group_mapping = {
            "Airline": "operating_carrier_name",
            "Destination": "country_code",
            "Flight Number": "flight_number",
            f"{process} Counter": f"{process}_pred",
        }

        queue_length = f"{process}_que"

        pie_result = {}
        table_result = {}
        total_queue_length_result = {}
        for group_name, group_column in group_mapping.items():
            group_max_queue = sim_df.groupby(group_column)[queue_length].max()
            top5_groups = group_max_queue.nlargest(5).index
            top5_df = sim_df[sim_df[group_column].isin(top5_groups)].copy()

            group_df = await self._create_queue_length(
                sim_df=top5_df, process=process, group_column=group_column
            )

            df = group_df.mean().sort_values(ascending=False)

            labels = []
            values = []
            table_values = []
            total_queue_length = 0
            for row in range(len(df)):

                que_mean = round(df.iloc[row])
                total_queue_length += que_mean
                column_name = df.index[row]

                values.append(que_mean)
                labels.append(column_name)

                table_values.append(
                    {"label": row + 1, "values": [f"{column_name} {que_mean}pax"]}
                )

            header = {
                "columns": [
                    {"label": f"{group_name} Top{len(table_values)}", "rowSpan": 2}
                ]
            }
            table_result[group_name] = {"header": header, "body": table_values}
            pie_result[group_name] = {"labels": labels, "values": values}
            total_queue_length_result[group_name] = total_queue_length

        return {
            "total_queue_length": total_queue_length_result,
            "pie_result": pie_result,
            "table_result": table_result,
        }

        # labels = []
        # values = []
        # table_values = []
        # for label, column_name in enumerate(
        #     top5_df[group_column].unique().tolist(), start=1
        # ):
        #     df = top5_df[top5_df[group_column] == column_name].copy()

        #     max_que = df[queue_length].max()
        #     max_time = (
        #         df.loc[df[queue_length] == max_que, start_time]
        #         .iloc[0]
        #         .floor("10min")
        #     )
        #     max_time_plus_10 = max_time + pd.Timedelta(minutes=10)
        #     max_time_minus_10 = max_time - pd.Timedelta(minutes=10)

        #     df.loc[:, start_time] = df[start_time].dt.floor("10min")
        #     filtered_df = df[
        #         (df[start_time] >= max_time_minus_10)
        #         & (df[start_time] <= max_time_plus_10)
        #     ]

        #     que_mean = round(filtered_df[queue_length].mean())

        #     values.append(que_mean)
        #     labels.append(column_name)

        #     table_values.append(
        #         {"label": label, "values": [f"{column_name} {que_mean} {max_time}"]}
        #     )

        # return

    # NOTE: Chart 공용로직
    async def _create_top5_chart(
        self, sim_df: pd.DataFrame, group_result: dict, kpi_name: str
    ):

        total_groups = sim_df.shape[1]
        has_etc = total_groups > 5
        if has_etc:
            top_5_columns = sim_df.sum().nlargest(5).index.tolist()
            sim_df = sim_df[top_5_columns]

        sim_df_x_list = sim_df.index.astype(str).tolist()
        traces = []
        group_order = sim_df.sum().sort_values(ascending=False).index.tolist()

        for column in sim_df.columns.unique().tolist():
            traces.append(
                {
                    "name": column,
                    "order": group_order.index(column),
                    "y": sim_df[column].astype(int).values.tolist(),
                }
            )
        group_result[kpi_name] = {"default_x": sim_df_x_list, "traces": traces}

        return group_result

    # NOTE: Passenger Analysis Chart
    async def generate_pa_chart(self, session, process):
        # s3에서 시뮬레이션 데이터 프레임 가져오기
        # if scenario_id:
        #     filename = f"{user_id}/{scenario_id}"
        #     sim_df: pd.DataFrame = await self.facility_repo.download_from_s3(
        #         session, filename
        #     )

        # FIXME: 이후에 실제 시뮬레이션 데이터로 붙을 수 있도록 컨트롤러와 함께 수정
        sim_df = pd.read_csv("samples/test_sample.csv")

        group_mapping = {
            "Airline": "operating_carrier_name",
            "Destination": "country_code",
            "Flight Number": "flight_number",
            f"{process} Counter": f"{process}_pred",
        }

        chart_result = {}
        for group_name, group_column in group_mapping.items():
            group_result = {}
            # TP
            tp = await self._create_throughput(
                sim_df=sim_df, process=process, group_column=group_column
            )

            group_result = await self._create_top5_chart(tp, group_result, "troughput")

            # QL
            ql = await self._create_queue_length(
                sim_df=sim_df, process=process, group_column=group_column
            )

            group_result = await self._create_top5_chart(
                ql, group_result, "queue_length"
            )

            # WT
            wt = await self._create_waiting_time(
                sim_df=sim_df, process=process, group_column=group_column
            )
            group_result = await self._create_top5_chart(
                wt, group_result, "waiting_time"
            )

            chart_result[group_name] = group_result

        return chart_result

    # ============================================================
    async def test(self, session: boto3.Session, process):

        data = pd.read_csv("samples/test_sample.csv")

        await self.create_pie_chart(data, process)

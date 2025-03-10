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

    # ==============================================================
    # NOTE: 공용 로직

    async def _create_throughput(self, sim_df: pd.DataFrame, process) -> pd.DataFrame:
        end_time = f"{process}_pt_pred"
        group_column = f"{process}_pred"

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

    async def _create_queue_length(self, sim_df: pd.DataFrame, process) -> pd.DataFrame:

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"
        group_column = f"{process}_pred"

        df = sim_df[[start_time, end_time, group_column]].copy()

        df[end_time] = pd.to_datetime(df[end_time])
        df[start_time] = pd.to_datetime(df[start_time])

        df[start_time] = df[start_time].dt.round("10min")
        df[end_time] = df[end_time].dt.round("10min")

        global_start = df[start_time].min()
        global_end = df[end_time].max()
        all_times = pd.date_range(start=global_start, end=global_end, freq="10min")

        group_list = df[group_column].unique()
        group_counts = pd.DataFrame(0, index=all_times, columns=group_list)

        for group, sub_df in df.groupby(group_column):

            start_idx = np.searchsorted(all_times, sub_df[start_time].values)
            end_idx = np.searchsorted(all_times, sub_df[end_time].values, side="right")

            diff_array = np.zeros(len(all_times) + 1, dtype=int)
            np.add.at(diff_array, start_idx, 1)
            np.add.at(diff_array, end_idx, -1)

            counts = np.cumsum(diff_array)[:-1]
            group_counts[group] = counts

        # print(df)
        # row_sums = df.sum(axis=1)
        # print(row_sums)
        # print(df.sum())
        # print(df.sum().mean())

        return group_counts

    async def _create_waiting_time(
        self, sim_df: pd.DataFrame, process, func: str = "mean"
    ) -> pd.DataFrame:
        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"
        group_column = f"{process}_pred"

        sim_df[start_time] = pd.to_datetime(sim_df[start_time])
        sim_df[end_time] = pd.to_datetime(sim_df[end_time])

        sim_df["waiting_time"] = (
            sim_df[end_time] - sim_df[start_time]
        ).dt.total_seconds()

        sim_df.loc[:, end_time] = sim_df[end_time].dt.floor("10min")

        if func == "top5":
            func = lambda x: np.percentile(x, 95)

        if func == "bottom5":
            func = lambda x: np.percentile(x, 5)

        df_grouped = (
            sim_df.groupby([end_time, group_column], as_index=False)
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

        df_grouped = (
            sim_df.groupby([end_time, group_column]).size().unstack(fill_value=0)
        )
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

    # ==============================================================
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
        sim_df = pd.read_csv("samples/sim_pax.csv")
        node_list: list = sim_df[f"{process}_pred"].unique().tolist()

        kpi_result["header"]["columns"].append({"label": "KPI"})
        kpi_result["header"]["columns"].append({"label": "AVERAGE"})
        kpi_result["header"]["columns"].append(
            {"label": process, "colSpan": len(node_list)}
        )

        for node in node_list:
            kpi_result["header"]["subColumns"].append({"label": node})

        # TP
        tp_data = await self._create_throughput(sim_df, process)
        tp_all = round(tp_data.sum().mean())
        tp_list = tp_data.sum().astype(int).values.tolist()
        tp_list.insert(0, tp_all)

        kpi_result["body"].append(
            {"label": "Throughput", "unit": "pax", "values": tp_list}
        )

        # QL
        ql_data = await self._create_queue_length(sim_df, process)
        ql_all = round(ql_data.sum().mean())
        ql_list = ql_data.sum().astype(int).values.tolist()
        ql_list.insert(0, ql_all)

        kpi_result["body"].append(
            {"label": "Queue Length", "unit": "pax", "values": ql_list}
        )

        # WT
        wt_data = await self._create_waiting_time(sim_df, process, func)
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

        # # Result
        # kpi_result["values"].append([tp_all, ql_all, wt_all, fe_all])
        # for tp, ql, wt, fe in zip(tp_list, ql_list, wt_list, fe_list):

        #     kpi_result["values"].append([tp, ql, wt, fe])

        return kpi_result

    # ==============================================================
    # NOTE: Chart

    async def generate_chart(
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
        sim_df = pd.read_csv("samples/sim_pax.csv")

        # TP
        tp_data = await self._create_throughput(sim_df, process)
        tp = tp_data.sum(axis=1)
        tp_x_list = tp.index.astype(str).tolist()
        tp_y_list = tp.astype(int).values.tolist()

        chart_result["troughput"] = {"x": tp_x_list, "y": tp_y_list}

        # QL
        ql_data = await self._create_queue_length(sim_df, process)
        ql = ql_data.sum(axis=1)
        ql_x_list = ql.index.astype(str).tolist()
        ql_y_list = ql.astype(int).values.tolist()

        chart_result["queue_length"] = {"x": ql_x_list, "y": ql_y_list}

        # WT
        wt_data = await self._create_waiting_time(sim_df, process)
        wt = wt_data.mean(axis=1)
        wt_x_list = wt.index.astype(str).tolist()
        wt_y_list = wt.astype(int).values.tolist()

        chart_result["waiting_time"] = {"x": wt_x_list, "y": wt_y_list}

        # FE
        fe_data = await self._create_facility_efficiency(sim_df, process, "mean")
        fe_data["fe"] = (fe_data.sum(axis=1) / 4) * 100
        fe_x_list = fe_data.index.astype(str).tolist()
        fe_y_list = fe_data["fe"].astype(int).values.tolist()

        chart_result["facility_efficiency"] = {"x": fe_x_list, "y": fe_y_list}

        return chart_result

    # ==============================================================
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
        sim_df = pd.read_csv("samples/sim_pax.csv")

        # TP
        tp = await self._create_throughput(sim_df, process)
        tp_x_list = tp.index.astype(str).tolist()
        tp_y_list = []
        tp_z_list = []
        tp_column_list = tp.columns.tolist()
        for column in tp_column_list:
            tp_y_list.append(column)
            tp_z_list.append(tp[column].astype(int).values.tolist())

        heatmap_result["troughput"] = {"x": tp_x_list, "y": tp_y_list, "z": tp_z_list}

        # QL
        ql = await self._create_queue_length(sim_df, process)
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
        wt = await self._create_waiting_time(sim_df, process)
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
    async def test(self, session: boto3.Session, process):

        # data = pd.read_csv(".idea/etc/samples/test.csv")
        data = pd.read_csv("samples/sim_pax.csv")
        # user_id = "6c377bfd-6679-48e5-ab27-49ed3ca4611c"
        # scenario_id = "01JM14SGPX0ZREQMGE3NRRKTWQ"
        # filename = f"{user_id}/{scenario_id}"
        # data: pd.DataFrame = await self.facility_repo.download_from_s3(
        #     session, filename
        # )
        # node_list: list = data[f"{process}_pred"].unique().tolist()

        # node_list.sort()
        # node_list.insert(0, "kpi")

        # print(node_list)

        await self._create_facility_efficiency(data, process)
        # await self.create_max_queue_chart(data, process=process)
        # print(data)

    # ==============================================================
    # NOTE: KPI 테이블에서 func 사용하는 로직

    async def set_groupby_function(
        self, df: pd.DataFrame, end_time, group_column, all_hours: bool
    ):

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
        print(df_grouped)

        print(sum(df_grouped))

    async def _create_simulation_kpi(self, sim_df: pd.DataFrame, process, node):

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"
        process_time = f"{process}_pt"

        sim_df[end_time] = pd.to_datetime(sim_df[end_time])
        sim_df[start_time] = pd.to_datetime(sim_df[start_time])

        filtered_sim_df = sim_df.loc[
            sim_df[f"{process}_pred"] == f"{process}_{node}"
        ].copy()

        diff_arr = (
            filtered_sim_df[end_time] - filtered_sim_df[start_time]
        ).dt.total_seconds()
        throughput = int(len(filtered_sim_df))

        total_delay = int(diff_arr.sum() / 60)
        max_delay = int(diff_arr.max() / 60)
        average_delay = int((total_delay / throughput) * 100) / 100
        average_transaction_time = int(filtered_sim_df[process_time].mean() * 10) / 10

        result = [throughput, max_delay, average_delay, average_transaction_time]

        return result

    async def _create_simulation_throughput(self, sim_df: pd.DataFrame, process):

        end_time = f"{process}_pt_pred"
        group_column = f"{process}_pred"

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
        print(df_grouped)

    async def _create_simulation_waiting_time(self, sim_df: pd.DataFrame, process):

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"

        node_list = sim_df[f"{process}_pred"].unique().tolist()

        sim_df[start_time] = pd.to_datetime(sim_df[start_time])
        sim_df[end_time] = pd.to_datetime(sim_df[end_time])

        for node in node_list:
            df = sim_df.loc[sim_df[f"{process}_pred"] == f"{node}"].copy()

            df["waiting_time"] = (df[end_time] - df[start_time]).dt.total_seconds()

            df.loc[:, end_time] = df[end_time].dt.floor("10min")
            df_grouped = (
                df.groupby([end_time], as_index=False)
                .agg({"waiting_time": "mean"})
                .set_index("checkin_pt_pred")
            )

            start_day = df_grouped.index[0].strftime("%Y-%m-%d %H:%M:%S")
            end_day = df_grouped.index[-1].strftime("%Y-%m-%d %H:%M:%S")
            all_hours = pd.date_range(
                start=pd.Timestamp(start_day),
                end=pd.Timestamp(end_day),
                freq="10min",
            )
            df_grouped = df_grouped.reindex(all_hours, fill_value=0)

            print("=================")
            print(node)
            print(df_grouped)

    # ==============================================================
    # NOTE: KPI past

    async def create_throughput_kpi(self, sim_df: pd.DataFrame, process):

        end_time = f"{process}_pt_pred"
        counter = f"{process}_pred"

        df = sim_df.dropna(subset=[end_time]).copy()
        counts = df[counter].value_counts()

        counts = counts.astype(int).tolist()
        counts.insert(0, sum(counts))

        return counts

    async def create_max_queue_kpi(self, sim_df: pd.DataFrame, process):

        df: pd.DataFrame = await self._create_simulation_queue(sim_df, process)

        counts = df.sum()
        counts = counts.astype(int).tolist()
        counts.insert(0, sum(counts))

        return counts

    async def create_waiting_time_kpi(self, sim_df: pd.DataFrame, process, func):

        start_time = f"{process}_on_pred"
        end_time = f"{process}_pt_pred"

        sim_df[start_time] = pd.to_datetime(sim_df[start_time])
        sim_df[end_time] = pd.to_datetime(sim_df[end_time])

        sim_df["waiting_time"] = (
            sim_df[end_time] - sim_df[start_time]
        ).dt.total_seconds()

        df_grouped = sim_df.groupby([f"{process}_pred"], as_index=False).agg(
            {"waiting_time": f"{func}"}
        )
        wt_list = df_grouped["waiting_time"].round().astype(int).tolist()
        wt_list.insert(0, sum(wt_list))

        return wt_list

    async def create_facility_efficiency_kpi(self, sim_df: pd.DataFrame, process, func):

        df_grouped = sim_df.groupby([f"{process}_pred"], as_index=False).agg(
            {f"{process}_pt": f"{func}"}
        )

        fe_list = df_grouped[f"{process}_pt"].round().astype(int).tolist()
        fe_list.insert(0, sum(fe_list))

        return fe_list


kpi_result = {
    "counter_A": [
        "throughput",
        "max_delay",
        "average_delay",
        "average_transaction_time",
    ],
    "counter_B": [
        "throughput",
        "max_delay",
        "average_delay",
        "average_transaction_time",
    ],
}

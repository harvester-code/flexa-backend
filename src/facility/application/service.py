from dependency_injector.wiring import inject
from src.facility.domain.repository import IFacilityRepository
import boto3
import pandas as pd
import numpy as np
from datetime import timedelta, datetime


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

        sim_df = await self.facility_repo.download_from_s3(session, scenario_id)

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

    async def _create_time_range(self, date):
        """
        서브 함수
        시뮬레이션 차트에 나올 시간 범위 생성

        Args:
            date: flight schedule에서 가져온 date

        Return:
            pd.date_range
        """

        date_obj = datetime.strptime(date, "%Y-%m-%d")

        day_before = date_obj - timedelta(days=1)
        day_after = date_obj + timedelta(days=1)

        day_before_str = day_before.strftime("%Y-%m-%d")
        day_after_str = day_after.strftime("%Y-%m-%d")

        time_range = pd.date_range(
            start=pd.Timestamp(f"{day_before_str} 23:00:00"),
            end=pd.Timestamp(f"{day_after_str} 01:00:00"),
            freq="10min",
        )

        return time_range

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

        start_day = df_grouped.index[0].strftime("%Y-%m-%d")
        time_range = await self._create_time_range(start_day)
        df_grouped = df_grouped.reindex(time_range, fill_value=0)
        # print(df_grouped["checkin_A"])
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

        start_day = df_grouped.index[0].strftime("%Y-%m-%d")
        time_range = await self._create_time_range(start_day)
        df_grouped = df_grouped.reindex(time_range, fill_value=0)

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

        start_day = df_grouped.index[0].strftime("%Y-%m-%d")
        time_range = await self._create_time_range(start_day)
        df_grouped = df_grouped.reindex(time_range, fill_value=0)

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

        start_day = df_grouped.index[0].strftime("%Y-%m-%d")
        time_range = await self._create_time_range(start_day)
        df_grouped = df_grouped.reindex(time_range, fill_value=0)

        return df_grouped

    async def get_average_time_string(self, time_list):
        """시간 문자열 리스트의 평균을 다시 시간 문자열로 반환합니다 (형식: HH:MM:SS)."""
        # 1. 모든 시간 문자열을 초 단위로 변환
        seconds_list = [pd.Timedelta(t).total_seconds() for t in time_list]

        # 2. 평균 초 계산
        average_seconds = sum(seconds_list) / len(seconds_list)

        # 3. 초를 다시 timedelta로 변환
        average_time = timedelta(seconds=average_seconds)

        # 4. 문자열로 변환 (HH:MM:SS 형식)
        average_time_str = str(average_time)

        # 4. 시간, 분, 초 추출
        hours, remainder = divmod(average_time.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)

        # 5. 원하는 포맷으로 변환 (00:00:00)
        average_time_str = f"{int(hours):02}:{int(minutes):02}:{int(seconds):02}"

        return average_time_str

    # NOTE: KPI
    async def generate_kpi(
        self,
        session: boto3.Session,
        process: str,
        stats: str = "mean",
        user_id: str | None = None,
        scenario_id: str | None = None,
    ):

        sim_df = await self.facility_repo.download_from_s3(session, scenario_id)

        kpi_result = {"header": {"columns": [], "subColumns": []}, "body": []}
        node_list: list = sim_df[f"{process}_pred"].unique().tolist()

        description = "A weighted average is a calculation that assigns varying degrees of importance to the numbers in a particular data set."
        kpi_result["header"]["columns"].append(
            {"label": "KPI", "description": description, "rowSpan": 2}
        )
        kpi_result["header"]["columns"].append(
            {"label": "Weighted Average", "rowSpan": 2}
        )
        kpi_result["header"]["columns"].append(
            {"label": process, "colSpan": len(node_list)}
        )

        for node in node_list:
            kpi_result["header"]["subColumns"].append({"label": node})

        quantile_options = {
            "max": 1,
            "min": 0,
            "median": 0.5,
            "top5": 0.95,
            "bottom5": 0.05,
            "mean": "mean",
        }
        quantile = quantile_options.get(stats, None)

        cols_needed = [
            f"{process}_pred",
            f"{process}_que",
            f"{process}_pt",
            f"{process}_on_pred",
            f"{process}_pt_pred",
        ]
        process_df = sim_df[cols_needed].copy()
        # # TP
        # tp_data = await self._create_throughput(
        #     sim_df=sim_df, process=process, group_column=f"{process}_pred"
        # )
        # tp_all = round(tp_data.sum().mean())
        # tp_list = tp_data.sum().astype(int).values.tolist()
        # tp_list.insert(0, tp_all)

        # kpi_result["body"].append(
        #     {"label": "Throughput", "unit": "pax", "values": tp_list}
        # )

        # NEW TroughPut
        tp_result = []
        for node in node_list:
            troughput = len(process_df[process_df[f"{process}_pred"] == node])
            tp_result.append(troughput)

        tp_mean = np.mean(tp_result)
        tp_result.insert(0, int(tp_mean))

        tp_result = [f"{tp:,}" for tp in tp_result]

        kpi_result["body"].append(
            {"label": "Throughput", "unit": "pax", "values": tp_result}
        )

        # ===============================
        # # QL
        # ql_data = await self._create_queue_length(
        #     sim_df=sim_df, process=process, group_column=f"{process}_pred", func=stats
        # )
        # # print(ql_data)
        # ql_all = round(ql_data.max().mean())
        # ql_list = ql_data.max().astype(int).values.tolist()
        # ql_list.insert(0, ql_all)

        # kpi_result["body"].append(
        #     {"label": "Queue Length", "unit": "pax", "values": ql_list}
        # )

        # NEW Queue Length
        ql_result = []
        for node in node_list:
            filtered_df = process_df[process_df[f"{process}_pred"] == node]

            if quantile == "mean":
                queue_length = filtered_df[f"{process}_que"].mean()
            else:
                queue_length = filtered_df[f"{process}_que"].quantile(quantile)

            ql_result.append(int(queue_length))

        ql_mean = np.mean(ql_result)
        ql_result.insert(0, int(ql_mean))

        ql_result = [f"{ql:,}" for ql in ql_result]
        kpi_result["body"].append(
            {"label": "Queue Length", "unit": "pax", "values": ql_result}
        )

        # ================================
        # WT
        # wt_data = await self._create_waiting_time(
        #     sim_df=sim_df, process=process, group_column=f"{process}_pred", func=stats
        # )
        # wt_all = round(wt_data.mean().mean())
        # wt_list = wt_data.mean().astype(int).values.tolist()
        # wt_list.insert(0, wt_all)

        # kpi_result["body"].append(
        #     {"label": "Waiting Time", "unit": "sec", "values": wt_list}
        # )

        # NEW WT
        wt_result = []
        for node in node_list:
            filtered_df = process_df[process_df[f"{process}_pred"] == node]
            wt_df = (
                filtered_df[f"{process}_pt_pred"] - filtered_df[f"{process}_on_pred"]
            )
            if quantile == "mean":
                waiting_time = wt_df.mean()
            else:
                waiting_time = wt_df.quantile(quantile)

            # waiting_time = pd.Timedelta(waiting_time).total_seconds()
            wt_result.append(str(waiting_time).split()[-1].split(".")[0])

        wt_mean = await self.get_average_time_string(wt_result)
        wt_result.insert(0, wt_mean)
        kpi_result["body"].append(
            {"label": "Waiting Time", "unit": None, "values": wt_result}
        )
        # ====================================
        # FE
        fe_data = await self._create_facility_efficiency(sim_df, process)
        fe_all = round(((fe_data.sum(axis=1) / 4) * 100).mean(), 1)
        column_list = fe_data.columns.tolist()
        fe_list = []
        for column in column_list:
            num_ones = (fe_data[column] == 1).sum()

            total_rows = len(fe_data)
            ratio = round(num_ones / total_rows * 100)
            fe_list.append(ratio)

        fe_mean = np.mean(fe_list)
        fe_list.insert(0, int(fe_mean))

        kpi_result["body"].append(
            {"label": "Facility Efficiency", "unit": "%", "values": fe_list}
        )

        # cal = HomeCalculator(pax_df=sim_df, calculate_type=stats)
        # result = cal.get_facility_details()
        # print(result)
        return kpi_result

    # NOTE: KPI Summary Chart
    async def generate_ks_chart(
        self,
        session: boto3.Session,
        process: str,
        user_id: str | None = None,
        scenario_id: str | None = None,
    ):

        chart_result = {}

        sim_df = await self.facility_repo.download_from_s3(session, scenario_id)

        node_list = [
            val
            for val in sim_df[f"{process}_pred"].unique()
            if isinstance(val, str) and val.strip() != ""
        ]
        tp_data = await self._create_throughput(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        ql_data = await self._create_queue_length(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )
        wt_data = await self._create_waiting_time(
            sim_df=sim_df, process=process, group_column=f"{process}_pred"
        )

        for node in node_list:
            node_result = {}

            # TP
            tp = tp_data[node]
            tp_x_list = tp.index.astype(str).tolist()
            tp_y_list = tp.astype(int).values.tolist()

            node_result["throughput"] = {"x": tp_x_list, "y": tp_y_list}

            # QL
            ql = ql_data[node]
            ql_x_list = ql.index.astype(str).tolist()
            ql_y_list = ql.astype(int).values.tolist()

            node_result["queue_length"] = {"x": ql_x_list, "y": ql_y_list}

            # WT
            wt = wt_data[node]
            wt_x_list = wt.index.astype(str).tolist()
            wt_y_list = (wt.astype(int) // 60).tolist()

            node_result["waiting_time"] = {"x": wt_x_list, "y": wt_y_list}

            # result
            chart_result[node] = node_result

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

        heatmap_result = {}

        sim_df = await self.facility_repo.download_from_s3(session, scenario_id)

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

        heatmap_result["throughput"] = {"x": tp_x_list, "y": tp_y_list, "z": tp_z_list}

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
            wt_z_list.append((wt[column].astype(int) // 60).tolist())

        heatmap_result["waiting_time"] = {
            "x": wt_x_list,
            "y": wt_y_list,
            "z": wt_z_list,
        }

        return heatmap_result

    # ============================================================
    # NOTE: Passenger Analysis

    def get_criteria_options(self, process) -> dict:
        criteria_options = {
            "airline": "operating_carrier_name",
            "destination": "country_code",
            "flight_number": "flight_number",
        }
        return criteria_options

    async def _create_top5(self, sim_df: pd.DataFrame):

        total_groups = sim_df.shape[1]
        has_etc = total_groups > 5
        if has_etc:
            top_5_columns = sim_df.sum().nlargest(5).index.tolist()
            sim_df["etc"] = sim_df.drop(columns=top_5_columns, errors="ignore").sum(
                axis=1
            )
            sim_df = sim_df[top_5_columns + ["etc"]]

        group_order = sim_df.sum().sort_values(ascending=False).index.tolist()
        if has_etc and "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

        return sim_df, group_order

    # NOTE: Pie Chart
    async def generate_pie_chart(
        self,
        session: boto3.Session,
        process: str,
        user_id: str | None = None,
        scenario_id: str | None = None,
    ):

        sim_df = await self.facility_repo.download_from_s3(session, scenario_id)

        pie_result = {}
        table_result = {}
        total_queue_length_result = {}
        for group_name, group_column in self.get_criteria_options(process).items():
            group_df = await self._create_queue_length(
                sim_df=sim_df, process=process, group_column=group_column
            )
            group_df, group_order = await self._create_top5(sim_df=group_df)
            df = group_df.mean().sort_values(ascending=False)

            labels = []
            values = []
            table_values = []
            total_queue_length = 0
            for i, row in enumerate(group_order):

                que_mean = round(df.loc[row])
                total_queue_length += que_mean
                column_name = row

                values.append(que_mean)
                labels.append(column_name)

                table_values.append(
                    {"rank": i + 1, "title": column_name, "value": f"{que_mean:,}"}
                )

            table_result[group_name] = table_values
            pie_result[group_name] = {"labels": labels, "values": values}
            total_queue_length_result[group_name] = f"{total_queue_length:,}"

        return {
            "total_queue_length": total_queue_length_result,
            "pie_result": pie_result,
            "table_result": table_result,
        }

    # NOTE: Chart 공용로직
    async def _create_all_chart(
        self, sim_df: pd.DataFrame, group_result: dict, kpi_name: str
    ):

        sim_df_x_list = sim_df.index.astype(str).tolist()

        total_groups = sim_df.shape[1]
        has_etc = total_groups > 9

        if has_etc:
            top_9_columns = sim_df.sum().nlargest(9).index.tolist()
            sim_df["etc"] = sim_df.drop(columns=top_9_columns, errors="ignore").sum(
                axis=1
            )
            sim_df = sim_df[top_9_columns + ["etc"]]
        else:
            top_9_columns = sim_df.columns.tolist()

        group_order = sim_df.sum().sort_values(ascending=False).index.tolist()
        if has_etc and "etc" in group_order:
            group_order.remove("etc")
            group_order.append("etc")

        traces = []
        for column in sim_df.columns.unique().tolist():
            traces.append(
                {
                    "name": column,
                    "order": group_order.index(column),
                    "y": (sim_df[column].astype(int) // 60).tolist(),
                }
            )
        group_result[kpi_name] = {"default_x": sim_df_x_list, "traces": traces}

        return group_result

    # NOTE: Passenger Analysis Chart
    async def generate_pa_chart(
        self,
        session: boto3.Session,
        process: str,
        user_id: str | None = None,
        scenario_id: str | None = None,
    ):

        sim_df = await self.facility_repo.download_from_s3(session, scenario_id)

        chart_result = {}
        for group_name, group_column in self.get_criteria_options(process).items():
            group_result = {}
            # TP
            tp = await self._create_throughput(
                sim_df=sim_df, process=process, group_column=group_column
            )

            group_result = await self._create_all_chart(tp, group_result, "throughput")

            # QL
            ql = await self._create_queue_length(
                sim_df=sim_df, process=process, group_column=group_column
            )

            group_result = await self._create_all_chart(
                ql, group_result, "queue_length"
            )

            # WT
            wt = await self._create_waiting_time(
                sim_df=sim_df, process=process, group_column=group_column
            )
            group_result = await self._create_all_chart(
                wt, group_result, "waiting_time"
            )

            chart_result[group_name] = group_result

        return chart_result

    # ============================================================
    async def test(self, session: boto3.Session, process):

        data = pd.read_parquet("samples/sim_pax.parquet")

        await self.create_pie_chart(data, process)

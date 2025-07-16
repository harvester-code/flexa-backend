import pandas as pd
import numpy as np
from typing import Dict, Any, Optional
import math


class Calculator:
    def __init__(
        self,
        pax_df: pd.DataFrame,
        facility_info: Optional[Dict[str, Any]] = None,
        calculate_type: str = "mean",
        percentile: int | None = None,
    ):
        # 1. on, done 값이 없는 경우, 처리 안된 여객이므로 제외하고 시작함
        self.pax_df = pax_df.copy()
        for process in [col.replace("_on_pred", "") for col in self.pax_df.columns if "on_pred" in col]:
            cols_to_check = [
                f"{process}_on_pred",
                f"{process}_done_pred"
            ]
            self.pax_df = self.pax_df.dropna(subset=cols_to_check)
        # 2. 처리 완료 시간이 예정 출발 시간보다 늦은 경우, 제외하고 시작함
        # last_done_col = f"{process_list[-1]}_done_pred"
        # if last_done_col in pax_df.columns and 'scheduled_gate_departure_local' in pax_df.columns:
        #     pax_df = pax_df[pax_df[last_done_col] < pax_df['scheduled_gate_departure_local']]

        self.facility_info = facility_info
        self.calculate_type = calculate_type
        self.percentile = percentile
        self.time_unit = "15min" # for aemos template time unit arrange
        self.process_list = self._get_process_list()
        self.facility_ratio = self.make_facility_ratio() if self.facility_info is not None else {}
    # ===============================
    # 메인 함수들
    # ===============================
        
    def get_summary(self):
        """요약 데이터 생성"""
        # 기본 데이터 계산
        throughput = int(self.pax_df[f"{self.process_list[-1]}_pt_pred"].notna().sum())
        
        waiting_times_df = pd.DataFrame({
            process: self.pax_df[f"{process}_pt_pred"] - self.pax_df[f"{process}_on_pred"]
            for process in self.process_list
        }).dropna()
        waiting_time_data = self._get_pax_experience_data(waiting_times_df, 'time', self.calculate_type, self.percentile)

        queue_lengths_df = pd.DataFrame({
            process: self.pax_df[f"{process}_que"] for process in self.process_list
        }).dropna()
        queue_data = self._get_pax_experience_data(queue_lengths_df, 'count', self.calculate_type, self.percentile)

        # 시설 관련 데이터 - 안전한 접근
        facility_data = self.facility_ratio.get("all_facility", {}) if self.facility_ratio else {}
        
        # 응답 데이터 구성
        data = {
            "throughput": throughput,
            "waiting_time": waiting_time_data['total'],
            "queue_length": queue_data['total'],
            "facility_utilization": facility_data.get("activated_per_installed"),
            "processed_per_activated": facility_data.get("processed_per_activated"),
            "processed_per_installed": facility_data.get("processed_per_installed"),
            "pax_experience": {
                "waiting_time": {
                    process: waiting_time_data[process] 
                    for process in self.process_list
                },
                "queue_length": {
                    process: queue_data[process] 
                    for process in self.process_list
                }
            }
        }
        return data
    
    def get_alert_issues(self, top_n: int = 8, time_interval: str = "30min"):
        """알림 및 이슈 데이터 생성"""
        result_df = pd.concat([
            self._create_process_dataframe(process, time_interval)
            for process in self.process_list
        ], ignore_index=True)

        # 데이터 정렬 및 중복 제거
        result_df = result_df.sort_values(
            "waiting_time", ascending=False
        ).drop_duplicates(subset=["datetime", "process_name"]).reset_index(drop=True)

        # 전체 시설 데이터
        data = {
            "all_facilities": [
                self._to_alert_format(row) 
                for _, row in result_df.head(top_n).iterrows()
            ]
        }

        # 각 프로세스별 데이터 추가
        for process_name in self.process_list:
            filtered_data = result_df[result_df["process"] == process_name].head(top_n)
            data[process_name] = [
                self._to_alert_format(row) 
                for _, row in filtered_data.iterrows()
            ]
        return data

    def get_aemos_template(self, time_interval_min=15, dep_arr="departure"):
        """AEMOS 템플릿 데이터 생성 (최적화된 버전)"""
        df = self.pax_df.copy()  # 한 번만 복사
        component_list = self.process_list
        
        ###### SURVEY TEMPLATE ######
        # 모든 컴포넌트의 데이터를 한 번에 처리
        template_parts = []
        
        for component in component_list:
            # 시간 플로어링을 한 번에 계산
            df[f"{component}_time_floored"] = df[f"{component}_on_pred"].dt.floor(f"{time_interval_min}T")
            
            # 모든 노드의 데이터를 한 번에 그룹화
            grouped = df.groupby([f"{component}_time_floored", f"{component}_pred"]).size().reset_index(name="Exp pax")
            
            # Service Point 컬럼 추가
            grouped["Service Point"] = grouped[f"{component}_pred"].str.replace(f"{component}_", "", regex=False)
            grouped["Touch Point"] = component
            
            # 컬럼 순서 조정
            grouped = grouped[["Touch Point", "Service Point", f"{component}_time_floored", "Exp pax"]]
            grouped = grouped.rename(columns={f"{component}_time_floored": "Measurement Time"})
            
            # 빈 컬럼들 추가
            empty_cols = ["Queue Start", "Sample Appearance", "Queue Pax", "Open Resources", "Open Detail", "Queue End", "Comment"]
            for col in empty_cols:
                grouped[col] = ""
            
            template_parts.append(grouped)
        
        # 모든 템플릿 데이터 합치기
        template = pd.concat(template_parts, ignore_index=True)
        
        # 샘플 데이터 설정 (첫 번째 행만)
        if len(template) > 0:
            template.loc[0, "Queue Start"] = template.loc[0, "Measurement Time"] + pd.Timedelta(12, unit="S")
            template.loc[0, "Sample Appearance"] = "Blue T-shirt"
            template.loc[0, "Queue Pax"] = 32
            template.loc[0, "Open Resources"] = 7
            template.loc[0, "Open Detail"] = "Desk 01~07"
            template.loc[0, "Queue End"] = template.loc[0, "Measurement Time"] + pd.Timedelta(702, unit="S")

        ###### SERVICE POINT INFO ######
        # 서비스 포인트 정보를 효율적으로 생성
        service_points = template[["Touch Point", "Service Point"]].drop_duplicates().reset_index(drop=True)
        
        # 하드코딩된 데이터를 사전으로 관리
        service_point_defaults = {
            "Unit(Desk / Lane..)": ["Desk", "Desk"],
            "Installed(total) Facility": ["11 Desks", "15 Desks"],
            "Manned / Self-service": ["Manned", "Self"],
            "e-Gate Support": ["No", "Yes"],
            "e-Gate Detail": ["-", "SITA SmartPath(v2.1)"],
            "Biometric Support": ["No", "Yes"],
            "Biometric Detail": ["-", "Face recognition(One ID, IDEMIA)"],
            "Dedicated Lane": ["No", "Yes"],
            "Dedicated Detail": ["No", "For PRM"]
        }
        
        # 컬럼명 변경
        service_points = service_points.rename(columns={
            "Touch Point": "Touch-Point",
            "Service Point": "Service-Point"
        })
        
        # 기본값 설정
        for col, values in service_point_defaults.items():
            service_points[col] = ""
            for i, value in enumerate(values):
                if i < len(service_points):
                    service_points.loc[i, col] = value

        ###### METRIC VALUE ######
        # 메트릭 계산을 벡터화
        metric_dict = {
            "origin_airport_code": df[f"{dep_arr}_airport_iata"].iloc[0] if len(df) > 0 else "",
            "terminal_name": df[f"{dep_arr}_terminal"].iloc[0] if len(df) > 0 else "",
            "num_of_flights": df["flight_number"].nunique(),
            "num_of_passengers": len(df),
            "num_of_touch_point": len(component_list),
            "num_of_service_point": service_points["Service-Point"].nunique(),
            "num_of_samples": len(template),
            "sample_ratio": f"{int(len(template)/(len(df)*len(component_list))*1000)/10}%" if len(df) > 0 and len(component_list) > 0 else "0%"
        }

        # 날짜/시간 컬럼을 문자열로 변환 (벡터화)
        datetime_columns = template.select_dtypes(include=['datetime64']).columns
        for col in datetime_columns:
            template[col] = template[col].astype(str)
        
        datetime_columns = service_points.select_dtypes(include=['datetime64']).columns
        for col in datetime_columns:
            service_points[col] = service_points[col].astype(str)

        return {
            "template_dict": template.to_dict(orient="records"),
            "service_point_info_dict": service_points.to_dict(orient="records"),
            "metric_dict": metric_dict
        }





    def get_flow_chart_data(self, time_unit: str = None):
        """플로우 차트 데이터 생성"""
        time_unit = time_unit or self.time_unit
        time_df = self._create_time_df_index(time_unit)
        data = {"times": time_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()}

        for process in self.process_list:
            facilities = sorted(self.pax_df[f"{process}_pred"].dropna().unique())
            if not facilities:
                data[process] = {}
                continue

            process_data = self.pax_df[self.pax_df[f"{process}_pred"].notna()].copy()
            process_data[f"{process}_waiting"] = (process_data[f"{process}_pt_pred"] - process_data[f"{process}_on_pred"]).dt.total_seconds()

            # 시간 플로어링을 복사본에서 계산
            process_data[f"{process}_on_floored"] = process_data[f"{process}_on_pred"].dt.floor(time_unit)
            process_data[f"{process}_done_floored"] = process_data[f"{process}_done_pred"].dt.floor(time_unit)

            # 한번에 모든 메트릭 계산
            metrics = {
                'inflow': process_data.groupby([f"{process}_on_floored", f"{process}_pred"]).size(),
                'outflow': process_data.groupby([f"{process}_done_floored", f"{process}_pred"]).size(),
                'queue_length': process_data.groupby([f"{process}_on_floored", f"{process}_pred"])[f"{process}_que"].mean(),
                'waiting_time': process_data.groupby([f"{process}_on_floored", f"{process}_pred"])[f"{process}_waiting"].mean()
            }

            # unstack하고 reindex 한번에
            pivoted = {k: v.unstack(fill_value=0).reindex(time_df.index, fill_value=0) for k, v in metrics.items()}

            # 결과 구성
            process_facility_data = {}
            aggregated = {k: pd.Series(0, index=time_df.index, dtype=float) for k in metrics.keys()}
            aggregated['capacity'] = pd.Series(0, index=time_df.index, dtype=float)

            for facility_name in facilities:
                node_name = facility_name.split("_")[-1]
                capacity_per_unit_time = self._calculate_node_capacity_list(process, node_name, time_unit) or [0] * len(time_df.index)
                
                facility_data = {k: pivoted[k].get(facility_name, pd.Series(0, index=time_df.index)) for k in metrics.keys()}
                facility_data['capacity'] = pd.Series(capacity_per_unit_time, index=time_df.index)
                
                # 집계
                for k in facility_data.keys():
                    aggregated[k] += facility_data[k]
                
                # 저장 (타입 변환)
                process_facility_data[node_name] = {
                    k: (facility_data[k].round() if k in ['queue_length', 'waiting_time'] else facility_data[k]).astype(int).tolist()
                    for k in facility_data.keys()
                }

            # all_zones
            facility_count = len(facilities)
            all_zones_data = {
                'inflow': aggregated['inflow'].astype(int).tolist(),
                'outflow': aggregated['outflow'].astype(int).tolist(), 
                'queue_length': (aggregated['queue_length'] / facility_count).round().astype(int).tolist(),
                'waiting_time': (aggregated['waiting_time'] / facility_count).round().astype(int).tolist(),
                'capacity': aggregated['capacity'].astype(int).tolist()
            }

            data[process] = {"all_zones": all_zones_data, **process_facility_data}
        return data

    def get_flow_chart_aemos_data(self, time_unit: str = None, dep_arr="departure"):
        """플로우 차트와 AEMOS 템플릿 데이터를 함께 생성 (최적화된 통합 함수)"""
        time_unit = time_unit or self.time_unit
        time_df = self._create_time_df_index(time_unit)
        flow_data = {"times": time_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()}
        aemos_template_parts = []

        for process in self.process_list:
            facilities = sorted(self.pax_df[f"{process}_pred"].dropna().unique())
            if not facilities:
                flow_data[process] = {}
                continue
       
            process_data = self.pax_df[self.pax_df[f"{process}_pred"].notna()].copy()
            process_data[f"{process}_waiting"] = (process_data[f"{process}_pt_pred"] - process_data[f"{process}_on_pred"]).dt.total_seconds()

            # 시간 플로어링을 복사본에서 계산
            process_data[f"{process}_on_floored"] = process_data[f"{process}_on_pred"].dt.floor(time_unit)
            process_data[f"{process}_done_floored"] = process_data[f"{process}_done_pred"].dt.floor(time_unit)

            # 한번에 모든 메트릭 계산
            metrics = {
                'inflow': process_data.groupby([f"{process}_on_floored", f"{process}_pred"]).size(),
                'outflow': process_data.groupby([f"{process}_done_floored", f"{process}_pred"]).size(),
                'queue_length': process_data.groupby([f"{process}_on_floored", f"{process}_pred"])[f"{process}_que"].mean(),
                'waiting_time': process_data.groupby([f"{process}_on_floored", f"{process}_pred"])[f"{process}_waiting"].mean()
            }

            # unstack하고 reindex 한번에
            pivoted = {k: v.unstack(fill_value=0).reindex(time_df.index, fill_value=0) for k, v in metrics.items()}

            # AEMOS 템플릿 데이터 생성
            aemos_part = self._create_aemos_template_part(process, pivoted)
            if aemos_part is not None:
                aemos_template_parts.append(aemos_part)

            # 플로우 차트 데이터 구성
            process_facility_data = {}
            aggregated = {k: pd.Series(0, index=time_df.index, dtype=float) for k in metrics.keys()}
            aggregated['capacity'] = pd.Series(0, index=time_df.index, dtype=float)

            for facility_name in facilities:
                node_name = facility_name.split("_")[-1]
                capacity_per_unit_time = self._calculate_node_capacity_list(process, node_name, time_unit) or [0] * len(time_df.index)
                
                facility_data = {k: pivoted[k].get(facility_name, pd.Series(0, index=time_df.index)) for k in metrics.keys()}
                facility_data['capacity'] = pd.Series(capacity_per_unit_time, index=time_df.index)
                
                # 집계
                for k in facility_data.keys():
                    aggregated[k] += facility_data[k]
                
                # 저장 (타입 변환)
                process_facility_data[node_name] = {
                    k: (facility_data[k].round() if k in ['queue_length', 'waiting_time'] else facility_data[k]).astype(int).tolist()
                    for k in facility_data.keys()
                }

            # all_zones
            facility_count = len(facilities)
            all_zones_data = {
                'inflow': aggregated['inflow'].astype(int).tolist(),
                'outflow': aggregated['outflow'].astype(int).tolist(), 
                'queue_length': (aggregated['queue_length'] / facility_count).round().astype(int).tolist(),
                'waiting_time': (aggregated['waiting_time'] / facility_count).round().astype(int).tolist(),
                'capacity': aggregated['capacity'].astype(int).tolist()
            }

            flow_data[process] = {"all_zones": all_zones_data, **process_facility_data}
        
        # AEMOS 데이터 생성
        aemos_data = self._build_aemos_data(aemos_template_parts, dep_arr)
        
        return {
            "flow_chart_data": flow_data,
            "aemos_template_data": aemos_data
        }

    def get_facility_details(self):
        """시설 세부 정보 생성"""
        
        if self.calculate_type != "mean" and self.percentile is None:
            raise ValueError("percentile 방식을 사용하려면 percentile 값을 제공해야 합니다.")

        data = []
        for process in self.process_list:
            cols = [f"{process}_{x}" for x in ["pred", "facility_number", "que", "pt", "on_pred", "pt_pred"]]
            process_df = self.pax_df[cols].copy()
            
            # Overview 계산
            waiting_time = self._calculate_waiting_time(process_df, process)
            ai, pa, pi = self._get_process_ratios(process)
            
            overview = {
                "opened": self._get_opened_count(process),
                "throughput": len(process_df),
                "queuePax": int(process_df[f"{process}_que"].quantile(1 - self.percentile / 100) if self.calculate_type == "top" else process_df[f"{process}_que"].mean()),
                "waitTime": self._format_waiting_time(waiting_time.quantile(1 - self.percentile / 100) if self.calculate_type == "top" else waiting_time.mean()),
                "ai_ratio": ai, "pa_ratio": pa, "pi_ratio": pi,
            }
            
            # Components 계산
            components = []
            for facility in sorted(process_df[f"{process}_pred"].unique()):
                facility_df = process_df[process_df[f"{process}_pred"] == facility]
                waiting_time = self._calculate_waiting_time(facility_df, process)
                
                ai = pa = None
                if self.facility_ratio and facility in self.facility_ratio:
                    ai = self.facility_ratio[facility].get("activated_per_installed")
                    pa = self.facility_ratio[facility].get("processed_per_activated")
                
                components.append({
                    "title": facility,
                    "opened": self._get_opened_count(process, facility),
                    "throughput": len(facility_df),
                    "queuePax": int(facility_df[f"{process}_que"].quantile(1 - self.percentile / 100) if self.calculate_type == "top" else facility_df[f"{process}_que"].mean()),
                    "waitTime": self._format_waiting_time(waiting_time.quantile(1 - self.percentile / 100) if self.calculate_type == "top" else waiting_time.mean()),
                    "ai_ratio": ai, "pa_ratio": pa,
                })
            
            data.append({"category": process, "overview": overview, "components": components})
        return data

    def make_facility_ratio(self, slot_seconds: int = 600):
        """시설 정보를 계층적으로 집계하여 하나의 딕셔너리에 통합"""
        if not self.facility_info:
            return {}
            
        # 상수
        SLOTS_PER_DAY = 86400 // slot_seconds
        
        # 1. 개별 시설 데이터 생성
        facility_summary = {}
        
        for component in self.facility_info["components"]:
            process = component["name"]
            
            for node in component["nodes"]:
                node_name = node["name"]
                facility_count = node["facility_count"]
                facility_schedules = node["facility_schedules"]
                
                # 용량 메트릭 계산
                installed_capacity, activated_capacity = self._calculate_capacity_metrics(
                    facility_schedules, slot_seconds
                )
                
                # 각 기기별 데이터 생성
                for device_idx in range(facility_count):
                    facility_key = f"{process}_{node_name}_{device_idx + 1}"
                    
                    # 기본값
                    installed = installed_capacity[device_idx] if device_idx < len(installed_capacity) else None
                    activated = activated_capacity[device_idx] if device_idx < len(activated_capacity) else None
                    processed = self._get_processed_pax_count(process, node_name, device_idx + 1)
                    
                    facility_summary[facility_key] = {
                        "installed_capacity": installed,
                        "activated_capacity": activated,
                        "processed_pax": processed,
                        "activated_per_installed": self._calculate_ratios(activated, installed),
                        "processed_per_activated": self._calculate_ratios(processed, activated),
                        "processed_per_installed": self._calculate_ratios(processed, installed)
                    }
        
        # 2. 계층적 집계
        facility_ratio = {}
        
        # 전체 시설 집계
        facility_ratio["all_facility"] = self._aggregate_facilities(list(facility_summary.values()))
        
        # 프로세스별 집계
        for process in self.process_list:
            process_facilities = [
                data for key, data in facility_summary.items() 
                if key.startswith(f"{process}_")
            ]
            
            if process_facilities:
                facility_ratio[process] = self._aggregate_facilities(process_facilities)
                
                # 노드별 집계 - facility_info에서 노드 정보 가져오기
                process_component = next(
                    (comp for comp in self.facility_info["components"] if comp["name"] == process), 
                    None
                )
                
                if process_component:
                    for node_info in process_component["nodes"]:
                        node = node_info["name"]
                        node_key = f"{process}_{node}"
                        node_facilities = [
                            data for key, data in facility_summary.items()
                            if key.startswith(f"{process}_{node}_")
                        ]
                        
                        if node_facilities:
                            facility_ratio[node_key] = self._aggregate_facilities(node_facilities)
                            
                            # 개별 시설 추가
                            for facility_key in sorted(facility_summary.keys()):
                                if facility_key.startswith(f"{process}_{node}_"):
                                    facility_ratio[facility_key] = facility_summary[facility_key]
        
        return facility_ratio

    def get_histogram_data(self):
        """시설별, 그리고 그 안의 구역별 통계 데이터 생성 (all_zones 포함)"""
        # 상수
        WT_BINS = [0, 15, 30, 45, 60, float("inf")]
        WT_LABELS = ["00:00-15:00", "15:00-30:00", "30:00-45:00", "45:00-60:00", "60:00-"]
        QL_BINS = [0, 50, 100, 150, 200, 250, float("inf")]
        QL_LABELS = ["0-50", "50-100", "100-150", "150-200", "200-250", "250+"]
        
        data = {}
        
        for process in self.process_list:
            facilities = sorted(self.pax_df[f"{process}_pred"].dropna().unique())
            wt_collection, ql_collection = [], []
            facility_data = {}
            
            for facility in facilities:
                df = self.pax_df[self.pax_df[f"{process}_pred"] == facility].copy()
                
                # 대기시간 분포
                wt_mins = (df[f"{process}_pt_pred"] - df[f"{process}_on_pred"]).dt.total_seconds()
                wt_bins = self._get_distribution(wt_mins, WT_BINS, WT_LABELS)
                
                # 대기열 분포
                ql_bins = []
                if f"{process}_que" in df.columns and not df[f"{process}_que"].empty:
                    ql_bins = self._get_distribution(df[f"{process}_que"], QL_BINS, QL_LABELS)
                
                # 데이터 저장
                short_name = facility.split("_")[-1]
                facility_data[short_name] = {
                    "waiting_time": self._create_bins_data(wt_bins, "min", True),
                    "queue_length": self._create_bins_data(ql_bins, "pax", False)
                }
                
                if wt_bins:
                    wt_collection.append(wt_bins)
                if ql_bins:
                    ql_collection.append(ql_bins)
            
            # all_zones 생성
            if wt_collection and ql_collection:
                all_zones = {
                    "waiting_time": self._create_bins_data(self._calc_avg_bins(wt_collection), "min", True),
                    "queue_length": self._create_bins_data(self._calc_avg_bins(ql_collection), "pax", False)
                }
                data[process] = {"all_zones": all_zones, **facility_data}
            else:
                data[process] = facility_data
        
        return data

    def get_sankey_diagram_data(self):
        """산키 다이어그램 데이터 생성"""
        # 메인 로직
        target_columns = [
            col
            for col in self.pax_df.columns
            if col.endswith("_pred") and not any(x in col for x in ["on", "done", "pt"])
        ]
        
        flow_df = self.pax_df.groupby(target_columns).size().reset_index(name="count")
        
        # 동일한 결과를 보장하기 위해 각 컬럼의 고유값을 정렬
        unique_values = {}
        current_index = 0
        for col in target_columns:
            # 고유값을 정렬하여 일관된 순서 보장
            sorted_unique_vals = sorted(flow_df[col].unique())
            unique_values[col] = {
                val: i + current_index for i, val in enumerate(sorted_unique_vals)
            }
            current_index += len(sorted_unique_vals)
        
        sources, targets, values = [], [], []
        for i in range(len(target_columns) - 1):
            col1, col2 = target_columns[i], target_columns[i + 1]
            grouped = flow_df.groupby([col1, col2])["count"].sum().reset_index()
            
            # 일관된 순서를 위해 그룹화된 결과도 정렬
            grouped = grouped.sort_values([col1, col2]).reset_index(drop=True)
            
            for _, row in grouped.iterrows():
                sources.append(unique_values[col1][row[col1]])
                targets.append(unique_values[col2][row[col2]])
                values.append(int(row["count"]))
        
        # 라벨도 정렬된 순서로 생성
        labels = []
        for col in target_columns:
            labels.extend(sorted(flow_df[col].unique()))
        
        return {
            "label": labels,
            "link": {"source": sources, "target": targets, "value": values},
        }

    # ===============================
    # 서브 함수들 (헬퍼 메소드)
    # ===============================

    def _get_process_list(self):
        """프로세스 리스트 추출"""
        return [col.replace("_on_pred", "") for col in self.pax_df.columns if "on_pred" in col]

    def _format_waiting_time(self, time_value):
        """대기 시간을 hour, minute, second로 분리하여 딕셔너리로 반환"""
        try:
            # timedelta 객체인 경우
            total_seconds = int(time_value.total_seconds())
        except AttributeError:
            # 정수(초)인 경우
            total_seconds = int(time_value)
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        return {"hour": hours, "minute": minutes, "second": seconds}

    def _get_pax_experience_data(self, df, data_type, calculate_type, percentile):
        """승객 경험 데이터 계산"""
        df['total'] = df.sum(axis=1)
        if calculate_type == 'mean':
            target_value = df['total'].mean()
        else:
            target_value = np.percentile(df['total'], 100 - percentile)
        
        closest_idx = (df['total'] - target_value).abs().idxmin()
        result_row = df.loc[closest_idx]
        
        result_dict = {}
        for col in df.columns:
            if data_type == 'time':
                result_dict[col] = self._format_waiting_time(result_row[col])
            else:
                result_dict[col] = int(result_row[col])
        
        return result_dict

    def _create_process_dataframe(self, process, time_interval):
        """각 프로세스별 데이터프레임 생성"""
        return pd.DataFrame(
            {
                "process": process,
                "datetime": self.pax_df[f"{process}_on_pred"].dt.floor(time_interval),
                "waiting_time": self.pax_df[f"{process}_pt_pred"] - self.pax_df[f"{process}_on_pred"],
                "queue_length": self.pax_df[f"{process}_que"],
                "process_name": self.pax_df[f"{process}_pred"],
            }
        )

    def _to_alert_format(self, row):
        """행을 알림 형식으로 변환"""
        return {
            "time": row["datetime"].strftime("%H:%M:%S"),
            "waiting_time": self._format_waiting_time(row["waiting_time"]),
            "queue_length": row["queue_length"],
            "node": row["process_name"],
        }

    def _create_time_df_index(self, time_unit):
        """시간별 데이터프레임 생성"""
        last_date = self.pax_df["show_up_time"].dt.date.unique()[-1]
        time_index = pd.date_range(
            start=f"{last_date} 00:00:00", 
            end=f"{last_date} 23:59:59", 
            freq=time_unit
        )
        return pd.DataFrame(index=time_index)

    def _calculate_node_capacity_list(self, process_name, node_name, time_unit):
        """노드 용량 리스트 계산"""
        if not self.facility_info:
            return []    
        unit_seconds = int(pd.Timedelta(time_unit).total_seconds())
        for comp in self.facility_info.get("components", []):
            if comp["name"] == process_name:
                for node in comp.get("nodes", []):
                    if node["name"] == node_name:
                        schedules = node.get("facility_schedules", [])
                        if not schedules:
                            return []
                        
                        # 10분 슬롯을 time_unit으로 분할
                        slots_per_10min = 600 // unit_seconds  # 10분(600초)을 unit_seconds로 나눔
                        
                        capacity_list = []
                        for slot in schedules:  # 144개 슬롯
                            slot_capacity = sum(unit_seconds / t for t in slot if t and t > 0)
                            slot_capacity_int = int(slot_capacity)
                            
                            # 각 10분 슬롯을 time_unit 크기로 분할해서 추가
                            capacity_list.extend([slot_capacity_int] * slots_per_10min)
                        
                        return capacity_list
        return []

    def _calculate_waiting_time(self, process_df, process):
        """대기 시간 계산"""
        return process_df[f"{process}_pt_pred"] - process_df[f"{process}_on_pred"]

    def _get_opened_count(self, process, facility=None):
        """열린 시설 개수 계산"""
        if not self.facility_info or "components" not in self.facility_info:
            return [0, 0]
        
        for comp in self.facility_info["components"]:
            if comp["name"] != process:
                continue
                
            if facility:  # 특정 시설
                for node in comp["nodes"]:
                    if f"{process}_{node['name']}" == facility:
                        schedules = node.get("facility_schedules", [])
                        per_facility = list(zip(*schedules)) if schedules else []
                        opened = sum(1 for col in per_facility if any(v and v > 0 for v in col))
                        total = node.get("facility_count", len(per_facility))
                        return [opened, total]
            else:  # 전체 프로세스
                opened = total = 0
                for node in comp["nodes"]:
                    schedules = node.get("facility_schedules", [])
                    per_facility = list(zip(*schedules)) if schedules else []
                    opened += sum(1 for col in per_facility if any(v and v > 0 for v in col))
                    total += node.get("facility_count", len(per_facility))
                return [opened, total]
        return [0, 0]

    def _get_process_ratios(self, process):
        """프로세스 비율 계산"""
        if not self.facility_ratio:
            return None, None, None
        
        keys = [k for k in self.facility_ratio.keys() if k.startswith(f"{process}_") and "_" not in k[len(process)+1:]]
        installed = sum(self.facility_ratio[k].get("installed_capacity", 0) or 0 for k in keys)
        activated = sum(self.facility_ratio[k].get("activated_capacity", 0) or 0 for k in keys)
        processed = sum(self.facility_ratio[k].get("processed_pax", 0) or 0 for k in keys)
        
        ai = round(activated / installed * 100, 2) if installed else None
        pa = round(processed / activated * 100, 2) if activated else None
        pi = round(processed / installed * 100, 2) if installed else None
        return ai, pa, pi

    def _calculate_capacity_metrics(self, facility_schedules, slot_seconds):
        """시설 스케줄로부터 용량 메트릭 계산"""
        SLOTS_PER_DAY = 86400 // slot_seconds
        num_devices = len(facility_schedules[0])
        
        # 최소 처리시간 추출
        min_per_device = []
        for col_idx in range(num_devices):
            col_values = [facility_schedules[row][col_idx] for row in range(len(facility_schedules))]
            significant_values = [v for v in col_values if v > 1e-9]
            min_per_device.append(min(significant_values) if significant_values else None)
        
        # 설치 용량 계산
        installed_capacity = []
        for min_time in min_per_device:
            if min_time and min_time > 0:
                capacity_per_slot = math.floor(slot_seconds / min_time)
                installed_capacity.append(capacity_per_slot * SLOTS_PER_DAY)
            else:
                installed_capacity.append(None)
        
        # 활성화 용량 계산
        activated_capacity = [0] * num_devices
        for slot in facility_schedules:
            for idx, time_val in enumerate(slot):
                if time_val and time_val > 1e-9:
                    activated_capacity[idx] += math.floor(slot_seconds / time_val)
        
        # 유효하지 않은 값 처리
        activated_capacity = [v if 0 < v < 1e9 else None for v in activated_capacity]
        
        return installed_capacity, activated_capacity

    def _calculate_ratios(self, activated, installed):
        """비율 계산"""
        if activated is not None and installed is not None and installed != 0:
            return round((activated / installed) * 100, 2)
        return None

    def _get_processed_pax_count(self, process, node, facility_idx):
        """처리된 승객 수 가져오기"""
        if f"{process}_pred" not in self.pax_df.columns:
            return None
            
        facility_name = f"{process}_{node}"
        facility_df = self.pax_df[self.pax_df[f"{process}_pred"] == facility_name]
        
        if f"{process}_facility_number" not in facility_df.columns:
            return None
            
        facility_counts = facility_df.groupby(f"{process}_facility_number").size()
        return int(facility_counts.get(facility_idx, 0))

    def _aggregate_facilities(self, facilities_data):
        """시설 데이터 집계"""
        if not facilities_data:
            return None
        
        aggregated = {
            "installed_capacity": sum(f["installed_capacity"] or 0 for f in facilities_data),
            "activated_capacity": sum(f["activated_capacity"] or 0 for f in facilities_data), 
            "processed_pax": sum(f["processed_pax"] or 0 for f in facilities_data)
        }
        
        # 비율 계산
        aggregated["activated_per_installed"] = self._calculate_ratios(
            aggregated["activated_capacity"], aggregated["installed_capacity"]
        )
        aggregated["processed_per_activated"] = self._calculate_ratios(
            aggregated["processed_pax"], aggregated["activated_capacity"]
        )
        aggregated["processed_per_installed"] = self._calculate_ratios(
            aggregated["processed_pax"], aggregated["installed_capacity"]
        )
        
        return aggregated

    def _get_distribution(self, values, bins, labels):
        """값들의 분포를 백분율로 계산"""
        if values.empty:
            return []
        groups = pd.cut(values, bins=bins, labels=labels, right=False)
        counts = groups.value_counts().reindex(labels, fill_value=0)
        total = counts.sum()
        percentages = ((counts / total) * 100).round(0) if total > 0 else counts
        return [{"title": label, "value": int(percentages[label]), "unit": "%"} for label in labels]

    def _parse_range(self, title, is_time=True):
        """범위 문자열 파싱"""
        if is_time:
            if ":" not in title:
                return [0, None]
            start, rest = title.split("-")
            start_min = int(start.split(":")[0])
            return [start_min, int(rest.split(":")[0]) if rest else None]
        else:
            if "+" in title:
                return [int(title.replace("+", "")), None]
            start, end = title.split("-") if "-" in title else (0, None)
            return [int(start), int(end) if end else None]

    def _create_bins_data(self, bins_list, range_unit, is_time):
        """bins 데이터 생성"""
        return {
            "range_unit": range_unit,
            "value_unit": "%",
            "bins": [{"range": self._parse_range(item["title"], is_time), "value": item["value"]} for item in bins_list]
        }

    def _calc_avg_bins(self, bins_collection):
        """여러 시설의 평균 계산"""
        if not bins_collection:
            return []
        agg = {}
        for bin_list in bins_collection:
            for item in bin_list:
                agg.setdefault(item["title"], []).append(item["value"])
        return [{"title": item["title"], "value": int(round(np.mean(agg.get(item["title"], [0]))))} 
                for item in bins_collection[0]]

    def _create_aemos_template_part(self, component, pivoted_data):
        """개별 컴포넌트의 AEMOS 템플릿 파트 생성 (실제 데이터 포함)"""
        try:
            # inflow 데이터를 기반으로 템플릿 생성
            grouped = pivoted_data['inflow'].stack().reset_index()
            grouped.columns = ['Measurement Time', f'{component}_pred', 'Exp pax']
            grouped = grouped[grouped['Exp pax'] > 0].copy()  # 실제 승객이 있는 시간대만
            
            if grouped.empty:
                return None
            
            # Service Point 컬럼 추가
            grouped["Service Point"] = grouped[f"{component}_pred"].str.replace(f"{component}_", "", regex=False)
            grouped["Touch Point"] = component
            grouped = grouped[["Touch Point", "Service Point", "Measurement Time", "Exp pax"]]
            
            # 실제 데이터로 빈 컬럼들 채우기
            empty_cols = ["Queue Start", "Sample Appearance", "Queue Pax", "Open Resources", "Open Detail", "Queue End", "Comment"]
            for col in empty_cols:
                grouped[col] = ""
            
            # 첫 번째 행에 실제 데이터 기반 값 설정
            if len(grouped) > 0:
                first_idx = grouped.index[0]
                measurement_time = grouped.loc[first_idx, "Measurement Time"]
                facility_name = grouped.loc[first_idx, f"{component}_pred"]
                
                # 실제 큐 길이와 대기시간 활용
                queue_data = pivoted_data.get('queue_length')
                waiting_data = pivoted_data.get('waiting_time')
                
                try:
                    avg_queue = int(queue_data[facility_name].mean()) if queue_data is not None and facility_name in queue_data.columns else 25
                    grouped.loc[first_idx, "Queue Pax"] = max(avg_queue, 1)
                    
                    avg_waiting = int(waiting_data[facility_name].mean()) if waiting_data is not None and facility_name in waiting_data.columns else 702
                    grouped.loc[first_idx, "Queue Start"] = measurement_time + pd.Timedelta(10, unit="S")
                    grouped.loc[first_idx, "Queue End"] = measurement_time + pd.Timedelta(avg_waiting, unit="S")
                except:
                    grouped.loc[first_idx, "Queue Pax"] = 25
                    grouped.loc[first_idx, "Queue Start"] = measurement_time + pd.Timedelta(12, unit="S")
                    grouped.loc[first_idx, "Queue End"] = measurement_time + pd.Timedelta(702, unit="S")
                
                # 나머지 필드들
                grouped.loc[first_idx, "Sample Appearance"] = "Blue T-shirt"
                grouped.loc[first_idx, "Open Resources"] = 7
                grouped.loc[first_idx, "Open Detail"] = "Desk 01~07"
            
            return grouped
            
        except Exception as e:
            print(f"Error creating AEMOS template part for {component}: {e}")
            return None

    def _build_aemos_data(self, template_parts, dep_arr):
        """AEMOS 템플릿 데이터 최종 구성"""
        if not template_parts:
            return {"template_dict": [], "service_point_info_dict": [], "metric_dict": {}}
        
        # 템플릿 합치기
        template = pd.concat(template_parts, ignore_index=True)
        
        # 서비스 포인트 정보 생성
        service_points = template[["Touch Point", "Service Point"]].drop_duplicates().reset_index(drop=True)
        service_point_defaults = {
            "Unit(Desk / Lane..)": ["Desk", "Desk"], "Installed(total) Facility": ["11 Desks", "15 Desks"],
            "Manned / Self-service": ["Manned", "Self"], "e-Gate Support": ["No", "Yes"],
            "e-Gate Detail": ["-", "SITA SmartPath(v2.1)"], "Biometric Support": ["No", "Yes"],
            "Biometric Detail": ["-", "Face recognition(One ID, IDEMIA)"], "Dedicated Lane": ["No", "Yes"],
            "Dedicated Detail": ["No", "For PRM"]
        }
        service_points = service_points.rename(columns={"Touch Point": "Touch-Point", "Service Point": "Service-Point"})
        for col, values in service_point_defaults.items():
            service_points[col] = ""
            for i, value in enumerate(values):
                if i < len(service_points):
                    service_points.loc[i, col] = value
        
        # 메트릭 계산
        df = self.pax_df
        metric_dict = {
            "origin_airport_code": df[f"{dep_arr}_airport_iata"].iloc[0] if len(df) > 0 else "",
            "terminal_name": df[f"{dep_arr}_terminal"].iloc[0] if len(df) > 0 else "",
            "num_of_flights": df["flight_number"].nunique(),
            "num_of_passengers": len(df),
            "num_of_touch_point": len(self.process_list),
            "num_of_service_point": service_points["Service-Point"].nunique(),
            "num_of_samples": len(template),
            "sample_ratio": f"{int(len(template)/(len(df)*len(self.process_list))*1000)/10}%" if len(df) > 0 and len(self.process_list) > 0 else "0%"
        }
        
        # 날짜/시간 컬럼을 문자열로 변환
        for col in template.select_dtypes(include=['datetime64']).columns:
            template[col] = template[col].astype(str)
        for col in service_points.select_dtypes(include=['datetime64']).columns:
            service_points[col] = service_points[col].astype(str)
        
        return {
            "template_dict": template.to_dict(orient="records"),
            "service_point_info_dict": service_points.to_dict(orient="records"),
            "metric_dict": metric_dict
        }
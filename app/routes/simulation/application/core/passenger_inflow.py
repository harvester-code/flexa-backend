"""
승객 유입량 분석 Core 모듈

프로세스 흐름 기반으로 각 시설별 15분 간격 승객 유입량을 계산합니다.
S3의 show-up-passenger.parquet 파일과 연계하여 실제 승객 데이터를 분석합니다.

주요 기능:
1. S3 parquet 데이터 로드
2. 프로세스 흐름 파싱 및 시설 그룹 생성  
3. travel_time 누적 계산으로 각 시설 도착 시간 산출
4. 15분 단위 시간 그룹핑하여 시설별 승객 수 집계
5. 응답 JSON 생성
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from fastapi import HTTPException, status
from loguru import logger


class PassengerInflowAnalyzer:
    """승객 유입량 분석 클래스"""
    
    def __init__(self):
        pass
    
    async def analyze_passenger_inflow(
        self, scenario_id: str, process_flow: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        승객 유입량 분석 - 프로세스 흐름 기반 시설별 시간대 분석
        
        Args:
            scenario_id: 시나리오 UUID
            process_flow: 공항 프로세스 단계별 설정 리스트
            
        Returns:
            시간대별 시설 그룹별 승객 유입량 데이터
            
        Raises:
            HTTPException: S3 parquet 읽기 실패, 데이터 처리 오류 시
        """
        try:
            logger.info(f"🔍 승객 유입량 분석 시작 (동적 그룹핑): scenario_id={scenario_id}")
            
            # 1단계: S3에서 승객 데이터 로드
            passenger_df = await self._load_passenger_data_from_s3(scenario_id)
            
            # 2단계: 프로세스 흐름 파싱하여 개별 시설 정보 생성
            facilities_by_process = self._parse_process_flow_to_facilities(process_flow)
            
            # 3단계: 15분 간격 동적 그룹핑으로 승객 수 집계
            time_grouped_data, missing_passengers_data = self._group_by_15min_intervals(
                passenger_df, facilities_by_process
            )
            
            # 4단계: 응답 JSON 형태로 변환
            response_data = self._build_passenger_inflow_response(
                scenario_id, time_grouped_data, missing_passengers_data, int(len(passenger_df))  # ✅ numpy 타입 방지
            )
            
            logger.info(f"✅ 동적 그룹핑 분석 완료: scenario_id={scenario_id}")
            return response_data
            
        except Exception as e:
            logger.error(f"❌ 승객 유입량 분석 실패: scenario_id={scenario_id}, error={str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to analyze passenger inflow: {str(e)}",
            )

    async def _load_passenger_data_from_s3(self, scenario_id: str):
        """S3에서 승객 parquet 데이터 로드"""
        try:
            # TODO: S3Manager를 사용하여 show-up-passenger.parquet 로드
            # 임시로 로컬 parquet 파일 로드 (개발용)
            import pandas as pd
            
            # S3 경로 구성
            s3_path = f"s3://your-bucket/scenario-data/{scenario_id}/show-up-passenger.parquet"
            
            # 개발용: 로컬 파일 사용
            local_path = "/Users/yi/Desktop/flexa/show-up-passenger.parquet"
            passenger_df = pd.read_parquet(local_path)
            
            logger.info(f"📊 승객 데이터 로드 완료: {len(passenger_df):,}명")
            return passenger_df
            
        except Exception as e:
            logger.error(f"❌ S3 승객 데이터 로드 실패: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Passenger data not found for scenario {scenario_id}",
            )

    def _parse_process_flow_to_facilities(self, process_flow: List[Dict[str, Any]]) -> Dict[str, Any]:
        """프로세스 흐름을 파싱하여 개별 시설 정보 생성 (동적 그룹핑 준비)"""
        try:
            facilities_by_process = {}
            cumulative_travel_time = 0
            
            for step_data in process_flow:
                process_name = step_data.get("name", f"process_{step_data.get('step', 0)}")
                travel_time = step_data.get("travel_time_minutes", 0)
                cumulative_travel_time += travel_time
                
                zones = step_data.get("zones", {})
                process_facilities = []
                
                for zone_name, zone_data in zones.items():
                    facilities = zone_data.get("facilities", [])
                    
                    for facility in facilities:
                        if facility.get("id"):
                            process_facilities.append({
                                "id": facility["id"],
                                "zone_name": zone_name,
                                "operating_schedule": facility.get("operating_schedule", {}),
                                "facility_data": facility,
                            })
                
                if process_facilities:
                    facilities_by_process[process_name] = {
                        "arrival_time_offset": cumulative_travel_time,
                        "entry_conditions": step_data.get("entry_conditions", []),
                        "facilities": process_facilities,
                    }
                    
                    logger.info(f"🏭 프로세스 파싱: {process_name} → {len(process_facilities)}개 시설")
            
            logger.info(f"📊 시설 파싱 완료: {len(facilities_by_process)}개 프로세스")
            return facilities_by_process
            
        except Exception as e:
            logger.error(f"❌ 프로세스 흐름 파싱 실패: {str(e)}")
            raise

    def _get_facility_conditions_for_time(self, facility: Dict[str, Any], target_time_str: str) -> Dict[str, Any]:
        """특정 시간에 시설의 운영 상태와 승객 조건을 가져오기"""
        try:
            operating_schedule = facility.get("operating_schedule", {})
            time_blocks = operating_schedule.get("today", {}).get("time_blocks", [])
            
            # 시간을 분 단위로 변환 (예: "07:30" -> 450)
            def time_to_minutes(time_str: str) -> int:
                if not time_str or time_str == "24:00":
                    return 24 * 60 if time_str == "24:00" else 0
                try:
                    hours, minutes = time_str.split(":")
                    return int(hours) * 60 + int(minutes)
                except (ValueError, AttributeError):
                    logger.error(f"❌ 시간 형식 오류: '{time_str}' - 00:00으로 처리")
                    return 0
            
            target_minutes = time_to_minutes(target_time_str)
            
            # 해당 시간대의 조건 찾기
            for block in time_blocks:
                period = block.get("period", "")
                if "-" in period:
                    start_time, end_time = period.split("-")
                    start_minutes = time_to_minutes(start_time)
                    end_minutes = time_to_minutes(end_time)
                    
                    # 24:00 처리 (1440분)
                    if end_minutes == 0 and end_time != "00:00":
                        end_minutes = 24 * 60
                    
                    # 시간 범위 내에 있는지 확인
                    if start_minutes <= target_minutes < end_minutes:
                        return {
                            "is_operating": True,
                            "passenger_conditions": block.get("passenger_conditions", []),
                            "process_time_seconds": block.get("process_time_seconds", 0),
                        }
            
            # 어떤 time_block에도 포함되지 않으면 운영 중단
            return {
                "is_operating": False,
                "passenger_conditions": [],
                "process_time_seconds": 0,
            }
            
        except Exception as e:
            logger.error(f"❌ 시설 조건 확인 실패: {str(e)}")
            return {
                "is_operating": False,
                "passenger_conditions": [],
                "process_time_seconds": 0,
            }

    def _create_dynamic_groups_for_time_slot(self, facilities_by_process: Dict[str, Any], time_slot_str: str) -> Dict[str, Any]:
        """특정 시간대에 대해 시설들을 동적으로 그룹핑"""
        try:
            dynamic_groups = {}
            
            for process_name, process_info in facilities_by_process.items():
                facilities = process_info["facilities"]
                arrival_time_offset = process_info["arrival_time_offset"]
                entry_conditions = process_info["entry_conditions"]
                
                # 시설별 조건 수집
                facility_conditions = {}
                for facility in facilities:
                    facility_id = facility["id"]
                    conditions = self._get_facility_conditions_for_time(facility, time_slot_str)
                    facility_conditions[facility_id] = conditions
                
                # 🔥 핵심: 동일한 조건을 가진 시설들을 그룹핑
                condition_groups = {}
                for facility_id, conditions in facility_conditions.items():
                    if not conditions["is_operating"]:
                        continue  # 비활성화된 시설은 그룹에서 제외
                    
                    # 조건을 문자열로 변환하여 그룹 키 생성
                    passenger_conditions = conditions["passenger_conditions"]
                    condition_key = self._serialize_conditions(passenger_conditions)
                    
                    if condition_key not in condition_groups:
                        condition_groups[condition_key] = {
                            "facilities": [],
                            "passenger_conditions": passenger_conditions,
                            "process_time_seconds": conditions["process_time_seconds"],
                        }
                    
                    condition_groups[condition_key]["facilities"].append(facility_id)
                
                # 각 조건 그룹을 동적 그룹으로 변환
                for condition_key, group_data in condition_groups.items():
                    facility_ids = group_data["facilities"]
                    
                    # 그룹 키 생성 (기존과 동일한 방식)
                    if len(facility_ids) == 1:
                        group_key = facility_ids[0]
                    else:
                        group_key = f"{facility_ids[0]}~{facility_ids[-1]}"
                    
                    dynamic_groups[group_key] = {
                        "process_name": process_name,
                        "facility_ids": facility_ids,
                        "arrival_time_offset": arrival_time_offset,
                        "entry_conditions": entry_conditions,
                        "passenger_conditions": group_data["passenger_conditions"],
                        "process_time_seconds": group_data["process_time_seconds"],
                    }
                    
                    logger.debug(f"🎯 동적 그룹: {process_name} → {group_key} ({len(facility_ids)}개 시설, 조건: {condition_key})")
            
            return dynamic_groups
            
        except Exception as e:
            logger.error(f"❌ 동적 그룹 생성 실패: {str(e)}")
            raise

    def _serialize_conditions(self, passenger_conditions: List[Dict[str, Any]]) -> str:
        """승객 조건을 정렬 가능한 문자열로 직렬화"""
        try:
            if not passenger_conditions:
                return "no_conditions"
            
            # 조건을 정렬하여 일관성 확보
            sorted_conditions = sorted(passenger_conditions, key=lambda x: (x.get("field", ""), str(x.get("values", []))))
            
            condition_parts = []
            for condition in sorted_conditions:
                field = condition.get("field", "")
                values = sorted(condition.get("values", []))  # 값도 정렬
                condition_parts.append(f"{field}:{','.join(values)}")
            
            return "|".join(condition_parts)
            
        except Exception as e:
            logger.error(f"❌ 조건 직렬화 실패: {str(e)}")
            return "error_condition"

    def _group_by_15min_intervals(self, passenger_df, facilities_by_process: Dict[str, Any]):
        """15분 간격으로 동적 그룹별 승객 수 집계"""
        try:
            import pandas as pd
            
            # 전체 시간 범위 파악 (모든 프로세스의 승객 도착 시간)
            all_times = []
            all_arrival_data = {}
            
            # 각 프로세스별로 승객 도착 시간 계산
            for process_name, process_info in facilities_by_process.items():
                arrival_offset = pd.Timedelta(minutes=process_info["arrival_time_offset"])
                process_passenger_df = passenger_df.copy()
                process_passenger_df["facility_arrival_time"] = process_passenger_df["show_up_time"] + arrival_offset
                
                all_times.extend(process_passenger_df["facility_arrival_time"].tolist())
                all_arrival_data[process_name] = process_passenger_df
            
            if not all_times:
                return [], []
            
            min_time = min(all_times)
            max_time = max(all_times)
            
            # 15분 간격 시간대 생성
            time_intervals = []
            current_time = min_time.floor('15min')  # 15분 단위로 내림
            
            while current_time <= max_time.ceil('15min'):
                time_intervals.append(current_time)
                current_time += timedelta(minutes=15)
            
            # 각 시간대별로 동적 그룹 생성 및 승객수 집계
            time_grouped_data = []
            missing_passengers_data = []
            
            for time_slot in time_intervals:
                next_time_slot = time_slot + timedelta(minutes=15)
                time_slot_str = time_slot.strftime("%H:%M")  # 시설 운영시간 비교용
                
                # 🔥 해당 시간대에 대한 동적 그룹 생성
                dynamic_groups = self._create_dynamic_groups_for_time_slot(facilities_by_process, time_slot_str)
                
                time_groups = {}
                missing_groups = {}
                
                # 각 동적 그룹별로 승객 수 계산
                for group_key, group_info in dynamic_groups.items():
                    process_name = group_info["process_name"]
                    
                    # 해당 프로세스의 승객 데이터 가져오기
                    if process_name not in all_arrival_data:
                        continue
                        
                    passenger_df_process = all_arrival_data[process_name]
                    
                    # 해당 시간대에 도착하는 승객 수 계산
                    mask = (
                        (passenger_df_process["facility_arrival_time"] >= time_slot) &
                        (passenger_df_process["facility_arrival_time"] < next_time_slot)
                    )
                    passenger_count = int(mask.sum())  # ✅ numpy.int64 → Python int 변환
                    
                    if passenger_count > 0:
                        # 프로세스별로 그룹 정리
                        if process_name not in time_groups:
                            time_groups[process_name] = {}
                        
                        time_groups[process_name][group_key] = passenger_count
                        
                        logger.debug(f"🎯 동적 집계: {process_name}/{group_key} → {passenger_count}명 @ {time_slot_str}")
                
                # TODO: 미처리 승객 로직 (2단계에서 구현)
                # 현재는 모든 승객이 처리되는 것으로 가정
                
                # 빈 프로세스 제거
                time_groups = {
                    process: groups for process, groups in time_groups.items() 
                    if groups  # 빈 딕셔너리가 아닌 경우만
                }
                
                # 정상 처리된 승객 데이터 추가
                if time_groups:
                    time_grouped_data.append({
                        "time": time_slot.strftime("%Y-%m-%d %H:%M"),
                        "groups": time_groups
                    })
            
            logger.info(f"📊 동적 그룹핑 완료: {len(time_grouped_data)}개 구간")
            return time_grouped_data, missing_passengers_data
            
        except Exception as e:
            logger.error(f"❌ 동적 그룹핑 실패: {str(e)}")
            raise

    def _build_passenger_inflow_response(
        self, scenario_id: str, time_grouped_data: List[Dict], missing_passengers_data: List[Dict], total_passengers: int
    ) -> Dict[str, Any]:
        """최종 응답 JSON 생성 (missing_passengers 포함)"""
        try:
            # 분석 기간 계산
            if time_grouped_data:
                start_time = time_grouped_data[0]["time"]
                end_time = time_grouped_data[-1]["time"]
            else:
                start_time = end_time = datetime.now().strftime("%Y-%m-%d %H:%M")
            
            return {
                "scenario_id": scenario_id,
                "analysis_period": {
                    "start": start_time,
                    "end": end_time,
                    "total_intervals": int(len(time_grouped_data))  # ✅ numpy 타입 방지
                },
                "total_passengers": int(total_passengers),  # ✅ numpy 타입 방지 (이중 보호)
                "chart_data": time_grouped_data,
                "missing_passengers": missing_passengers_data  # 🆕 미처리 승객 데이터 추가
            }
            
        except Exception as e:
            logger.error(f"❌ 응답 생성 실패: {str(e)}")
            raise


class PassengerInflowResponse:
    """승객 유입량 분석 응답 생성 클래스"""
    
    def __init__(self):
        self.analyzer = PassengerInflowAnalyzer()
    
    async def build_response(
        self, scenario_id: str, process_flow: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """승객 유입량 분석 응답 생성"""
        return await self.analyzer.analyze_passenger_inflow(scenario_id, process_flow)

"""
명령 실행 서비스 - 프로세스 추가/삭제/수정 등 실제 작업 수행
"""
import json
import re
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from loguru import logger
import pandas as pd
import numpy as np

from app.routes.simulation.application.service import SimulationService


def normalize_process_name(name: str) -> str:
    """
    프로세스 이름 정규화 (프론트엔드와 동일한 로직)
    예: "checkin" -> "check_in", "Visa-Check" -> "visa_check"
    """
    # 한글 -> 영어 매핑
    korean_mapping = {
        "체크인": "check_in",
        "보안검색": "security_check",
        "입국심사": "immigration",
        "세관": "customs",
        "탑승": "boarding",
        "비자체크": "visa_check",
        "여행세": "travel_tax",
    }
    
    # 한글 매핑 확인
    if name in korean_mapping:
        return korean_mapping[name]
    
    # 영어인 경우 정규화
    normalized = name.lower()
    normalized = re.sub(r'[^a-z0-9]', '_', normalized)  # 영문, 숫자 외 모든 문자를 언더스코어로
    normalized = re.sub(r'_+', '_', normalized)  # 연속된 언더스코어를 하나로
    normalized = normalized.strip('_')  # 앞뒤 언더스코어 제거
    
    return normalized


class CommandExecutor:
    """명령 실행 전담 클래스"""
    
    def __init__(self, simulation_service: SimulationService):
        self.simulation_service = simulation_service
    
    async def add_process(
        self, 
        scenario_id: str, 
        process_name: str,
        zones: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        프로세스 추가
        
        Args:
            scenario_id: 시나리오 ID
            process_name: 프로세스 이름 (정규화 전)
            zones: zone 목록 (선택사항)
        
        Returns:
            실행 결과
        """
        try:
            # 1. 현재 metadata 로드
            metadata_result = await self.simulation_service.load_scenario_metadata(scenario_id)
            metadata = metadata_result.get("metadata", {})
            
            if metadata is None:
                # 새 시나리오인 경우 기본 구조 생성
                metadata = {
                    "context": {
                        "scenarioId": scenario_id,
                        "airport": "",
                        "terminal": "",
                        "date": datetime.now(timezone.utc).date().isoformat(),
                        "lastSavedAt": None,
                    },
                    "flight": {
                        "selectedConditions": None,
                        "appliedFilterResult": None,
                        "total_flights": None,
                        "airlines": None,
                        "filters": None,
                    },
                    "passenger": {
                        "settings": {"min_arrival_minutes": None},
                        "pax_generation": {"rules": [], "default": {"load_factor": None, "flightCount": 0}},
                        "pax_demographics": {
                            "nationality": {"available_values": [], "rules": [], "default": {"flightCount": 0}},
                            "profile": {"available_values": [], "rules": [], "default": {"flightCount": 0}},
                        },
                        "pax_arrival_patterns": {"rules": [], "default": {"mean": None, "std": None, "flightCount": 0}},
                    },
                    "process_flow": [],
                    "terminalLayout": {"zoneAreas": {}},
                    "workflow": {
                        "currentStep": 1,
                        "step1Completed": False,
                        "step2Completed": False,
                        "availableSteps": [1],
                    },
                    "savedAt": None,
                }
            
            # 2. process_flow 가져오기
            process_flow = metadata.get("process_flow", [])
            
            # 3. 프로세스 이름 정규화
            normalized_name = normalize_process_name(process_name)
            
            # 4. 중복 확인
            existing_processes = [p.get("name") for p in process_flow]
            if normalized_name in existing_processes:
                return {
                    "success": False,
                    "message": f"프로세스 '{process_name}'가 이미 존재합니다.",
                    "error": f"Process '{normalized_name}' already exists",
                }
            
            # 5. 새 프로세스 생성
            new_step = {
                "step": len(process_flow),
                "name": normalized_name,
                "travel_time_minutes": 0,
                "entry_conditions": [],
                "zones": {},
            }
            
            # zones가 제공된 경우 설정
            if zones and isinstance(zones, list):
                for zone_name in zones:
                    new_step["zones"][zone_name] = {
                        "facilities": []
                    }
            
            # 6. process_flow에 추가
            process_flow.append(new_step)
            metadata["process_flow"] = process_flow
            
            # 7. savedAt 업데이트
            metadata["savedAt"] = datetime.now(timezone.utc).isoformat()
            
            # 8. S3에 저장
            await self.simulation_service.save_scenario_metadata(scenario_id, metadata)
            
            logger.info(f"✅ Process '{normalized_name}' added to scenario {scenario_id}")
            
            return {
                "success": True,
                "message": f"프로세스 '{process_name}'가 추가되었습니다.",
                "action": "add_process",
                "data": {
                    "process": new_step,
                    "total_processes": len(process_flow),
                },
            }
        
        except Exception as e:
            logger.error(f"Failed to add process: {str(e)}")
            return {
                "success": False,
                "message": f"프로세스 추가 중 오류가 발생했습니다: {str(e)}",
                "error": str(e),
            }
    
    async def remove_process(
        self,
        scenario_id: str,
        process_name: str,
        step: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        프로세스 삭제
        
        Args:
            scenario_id: 시나리오 ID
            process_name: 프로세스 이름
            step: step 번호 (선택사항, 이름으로 찾을 수 없을 때 사용)
        
        Returns:
            실행 결과
        """
        try:
            # 1. 현재 metadata 로드
            metadata_result = await self.simulation_service.load_scenario_metadata(scenario_id)
            metadata = metadata_result.get("metadata", {})
            
            if metadata is None:
                return {
                    "success": False,
                    "message": "시나리오를 찾을 수 없습니다.",
                    "error": "Scenario not found",
                }
            
            process_flow = metadata.get("process_flow", [])
            
            if not process_flow:
                return {
                    "success": False,
                    "message": "삭제할 프로세스가 없습니다.",
                    "error": "No processes to remove",
                }
            
            # 2. 프로세스 찾기
            normalized_name = normalize_process_name(process_name)
            process_index = None
            
            if step is not None:
                # step 번호로 찾기
                for idx, p in enumerate(process_flow):
                    if p.get("step") == step:
                        process_index = idx
                        break
            else:
                # 이름으로 찾기
                for idx, p in enumerate(process_flow):
                    if p.get("name") == normalized_name:
                        process_index = idx
                        break
            
            if process_index is None:
                return {
                    "success": False,
                    "message": f"프로세스 '{process_name}'를 찾을 수 없습니다.",
                    "error": f"Process '{normalized_name}' not found",
                }
            
            # 3. 프로세스 삭제
            removed_process = process_flow.pop(process_index)
            
            # 4. step 번호 재정렬
            for idx, p in enumerate(process_flow):
                p["step"] = idx
            
            metadata["process_flow"] = process_flow
            metadata["savedAt"] = datetime.now(timezone.utc).isoformat()
            
            # 5. S3에 저장
            await self.simulation_service.save_scenario_metadata(scenario_id, metadata)
            
            logger.info(f"✅ Process '{normalized_name}' removed from scenario {scenario_id}")
            
            return {
                "success": True,
                "message": f"프로세스 '{process_name}'가 삭제되었습니다.",
                "action": "remove_process",
                "data": {
                    "removed_process": removed_process,
                    "total_processes": len(process_flow),
                },
            }
        
        except Exception as e:
            logger.error(f"Failed to remove process: {str(e)}")
            return {
                "success": False,
                "message": f"프로세스 삭제 중 오류가 발생했습니다: {str(e)}",
                "error": str(e),
            }
    
    async def list_files(self, scenario_id: str) -> Dict[str, Any]:
        """
        S3 폴더의 파일 목록 조회
        
        Args:
            scenario_id: 시나리오 ID
        
        Returns:
            파일 목록 정보
        """
        try:
            files = await self.simulation_service.s3_manager.list_files_async(scenario_id)
            
            if not files:
                return {
                    "success": True,
                    "message": "S3 폴더에 파일이 없습니다.",
                    "files": [],
                    "count": 0,
                }
            
            # 파일을 카테고리별로 분류
            file_categories = {
                "metadata": [f for f in files if "metadata" in f.lower()],
                "parquet": [f for f in files if f.endswith(".parquet")],
                "json": [f for f in files if f.endswith(".json") and "metadata" not in f.lower()],
                "other": [f for f in files if f not in [item for sublist in [
                    [f for f in files if "metadata" in f.lower()],
                    [f for f in files if f.endswith(".parquet")],
                    [f for f in files if f.endswith(".json") and "metadata" not in f.lower()]
                ] for item in sublist]]
            }
            
            message_parts = [f"총 {len(files)}개의 파일이 있습니다:\n"]
            
            if file_categories["metadata"]:
                message_parts.append(f"\n📄 메타데이터 파일 ({len(file_categories['metadata'])}개):")
                for f in file_categories["metadata"]:
                    message_parts.append(f"  - {f}")
            
            if file_categories["parquet"]:
                message_parts.append(f"\n📊 Parquet 파일 ({len(file_categories['parquet'])}개):")
                for f in file_categories["parquet"]:
                    message_parts.append(f"  - {f}")
            
            if file_categories["json"]:
                message_parts.append(f"\n📋 JSON 파일 ({len(file_categories['json'])}개):")
                for f in file_categories["json"]:
                    message_parts.append(f"  - {f}")
            
            if file_categories["other"]:
                message_parts.append(f"\n📁 기타 파일 ({len(file_categories['other'])}개):")
                for f in file_categories["other"]:
                    message_parts.append(f"  - {f}")
            
            return {
                "success": True,
                "message": "\n".join(message_parts),
                "files": files,
                "count": len(files),
                "categories": {k: len(v) for k, v in file_categories.items()},
            }
        
        except Exception as e:
            logger.error(f"Failed to list files: {str(e)}")
            return {
                "success": False,
                "message": f"파일 목록 조회 중 오류가 발생했습니다: {str(e)}",
                "error": str(e),
            }
    
    async def read_file(
        self,
        scenario_id: str,
        filename: str,
        summary_type: str = "summary"
    ) -> Dict[str, Any]:
        """
        S3 파일 내용 읽기 및 분석
        
        Args:
            scenario_id: 시나리오 ID
            filename: 파일 이름
            summary_type: 요약 타입 (summary, full, structure)
        
        Returns:
            파일 내용 및 분석 결과
        """
        try:
            # 1. 파일 읽기
            if filename.endswith(".json"):
                # JSON 파일 읽기
                content = await self.simulation_service.s3_manager.get_json_async(
                    scenario_id=scenario_id,
                    filename=filename
                )
                
                if content is None:
                    return {
                        "success": False,
                        "message": f"파일 '{filename}'을 찾을 수 없습니다.",
                        "error": "File not found",
                    }
                
                # 2. 요약 타입에 따라 처리
                if summary_type == "structure":
                    # 구조만 반환
                    import json
                    structure = self._get_json_structure(content)
                    return {
                        "success": True,
                        "message": f"파일 '{filename}'의 구조:\n{json.dumps(structure, indent=2, ensure_ascii=False)}",
                        "filename": filename,
                        "structure": structure,
                    }
                elif summary_type == "full":
                    # 전체 내용 반환 (큰 파일은 주의)
                    import json
                    return {
                        "success": True,
                        "message": f"파일 '{filename}'의 전체 내용:\n{json.dumps(content, indent=2, ensure_ascii=False)[:5000]}...",
                        "filename": filename,
                        "content": content,
                    }
                else:
                    # summary: AI에게 전달하여 요약
                    import json
                    
                    # 파일이 크든 작든 구조화된 요약 정보 추출
                    if isinstance(content, dict):
                        summary_info = {}
                        
                        # context 정보
                        if "context" in content:
                            ctx = content["context"]
                            summary_info["시나리오_정보"] = {
                                "공항": ctx.get("airport", ""),
                                "터미널": ctx.get("terminal", ""),
                                "날짜": ctx.get("date", ""),
                                "저장_시각": ctx.get("lastSavedAt", ""),
                            }
                        
                        # process_flow 정보 (구체적으로)
                        if "process_flow" in content:
                            pf = content["process_flow"]
                            summary_info["프로세스_흐름"] = []
                            for proc in pf:  # 모든 프로세스
                                proc_info = {
                                    "이름": proc.get("name", ""),
                                    "단계": proc.get("step", ""),
                                    "이동시간_분": proc.get("travel_time_minutes", 0),
                                    "기본_처리시간_초": proc.get("process_time_seconds", 0),
                                    "구역_개수": len(proc.get("zones", {})),
                                }

                                # 각 구역의 시설 정보 (복잡한 시나리오 대응: 상세 목록 대신 요약만)
                                zones_summary = {}
                                total_facilities = 0
                                active_facilities = 0
                                sample_operating_period = None
                                sample_process_time = None

                                for zone_name, zone_data in proc.get("zones", {}).items():
                                    facilities = zone_data.get("facilities", [])
                                    facility_count = len(facilities)
                                    total_facilities += facility_count

                                    zone_active = 0
                                    for fac in facilities:
                                        operating_schedule = fac.get("operating_schedule", {})
                                        time_blocks = operating_schedule.get("time_blocks", [])

                                        if time_blocks:
                                            tb = time_blocks[0]
                                            if tb.get("activate", False):
                                                zone_active += 1
                                                active_facilities += 1

                                            # 대표 값 저장 (첫 번째 활성 시설)
                                            if sample_operating_period is None and tb.get("activate", False):
                                                sample_operating_period = tb.get("period", "")
                                                sample_process_time = tb.get("process_time_seconds", proc.get("process_time_seconds", 0))

                                    zones_summary[zone_name] = {
                                        "시설_개수": facility_count,
                                        "활성_시설_개수": zone_active
                                    }

                                proc_info["구역별_요약"] = zones_summary
                                proc_info["총_시설_개수"] = total_facilities
                                proc_info["활성_시설_개수"] = active_facilities

                                # 대표 운영 정보
                                if sample_operating_period:
                                    proc_info["운영기간_예시"] = sample_operating_period
                                if sample_process_time:
                                    proc_info["처리시간_초_예시"] = sample_process_time

                                summary_info["프로세스_흐름"].append(proc_info)
                        
                        # flight 정보 요약
                        if "flight" in content:
                            flight = content["flight"]
                            flight_summary = {}
                            if flight.get("selectedConditions"):
                                sc = flight["selectedConditions"]
                                flight_summary["필터_타입"] = sc.get("type", "")
                                flight_summary["조건_개수"] = len(sc.get("conditions", []))
                                if sc.get("expected_flights"):
                                    flight_summary["선택된_항공편"] = sc["expected_flights"].get("selected", 0)
                                    flight_summary["전체_항공편"] = sc["expected_flights"].get("total", 0)
                            if flight.get("appliedFilterResult"):
                                afr = flight["appliedFilterResult"]
                                flight_summary["적용된_필터_결과"] = afr.get("total", 0)
                            if flight_summary:
                                summary_info["항공편_정보"] = flight_summary
                        
                        # passenger 정보 요약
                        if "passenger" in content:
                            passenger = content["passenger"]
                            pax_summary = {}
                            if passenger.get("settings"):
                                pax_summary["최소_도착_시간_분"] = passenger["settings"].get("min_arrival_minutes")
                            if passenger.get("pax_generation"):
                                pg = passenger["pax_generation"]
                                pax_summary["생성_규칙_개수"] = len(pg.get("rules", []))
                                if pg.get("default"):
                                    pax_summary["기본_적재율"] = pg["default"].get("load_factor")
                            if passenger.get("pax_demographics"):
                                pd = passenger["pax_demographics"]
                                if pd.get("nationality"):
                                    nat = pd["nationality"]
                                    pax_summary["국적_규칙_개수"] = len(nat.get("rules", []))
                                    pax_summary["사용가능_국적_수"] = len(nat.get("available_values", []))
                                if pd.get("profile"):
                                    prof = pd["profile"]
                                    pax_summary["프로필_규칙_개수"] = len(prof.get("rules", []))
                                    pax_summary["사용가능_프로필_수"] = len(prof.get("available_values", []))
                            if pax_summary:
                                summary_info["승객_정보"] = pax_summary
                        
                        # workflow 정보
                        if "workflow" in content:
                            wf = content["workflow"]
                            summary_info["작업_흐름"] = {
                                "현재_단계": wf.get("currentStep", ""),
                                "1단계_완료": wf.get("step1Completed", False),
                                "2단계_완료": wf.get("step2Completed", False),
                                "사용가능_단계": wf.get("availableSteps", []),
                            }
                        
                        # savedAt 정보
                        if "savedAt" in content:
                            summary_info["저장_시각"] = content.get("savedAt", "")
                        
                        content_str = json.dumps(summary_info, indent=2, ensure_ascii=False)
                    else:
                        # dict가 아닌 경우
                        content_str = json.dumps(content, indent=2, ensure_ascii=False)
                    
                    return {
                        "success": True,
                        "message": f"파일 '{filename}'의 내용을 분석 중입니다.",
                        "filename": filename,
                        "content_preview": content_str[:60000],  # 구조화된 요약 정보 (복잡한 시나리오 대응)
                        "full_content": content,  # 전체 내용은 별도로 전달
                        "needs_ai_analysis": True,
                    }
            
            elif filename.endswith(".parquet"):
                # Parquet 파일 읽기 및 분석
                try:
                    df = await self.simulation_service.s3_manager.get_parquet_async(
                        scenario_id=scenario_id,
                        filename=filename
                    )

                    if df is None:
                        return {
                            "success": False,
                            "message": f"파일 '{filename}'을 찾을 수 없습니다.",
                            "error": "File not found",
                        }

                    # 파일 타입별로 해당 엔드포인트의 Response 로직 사용
                    if filename == "flight-schedule.parquet":
                        # FlightScheduleResponse 로직 사용
                        analysis = await self._analyze_flight_schedule_with_response(df, scenario_id)
                    elif filename == "show-up-passenger.parquet":
                        # ShowUpPassengerResponse 로직 사용
                        analysis = await self._analyze_show_up_passenger_with_response(df, scenario_id)
                    elif filename == "simulation-pax.parquet":
                        # Lambda 시뮬레이션 로직 기반 분석
                        analysis = await self._analyze_simulation_pax_with_response(df, scenario_id)
                    else:
                        # 기타 parquet: 기존 분석 로직 사용
                        analysis = await self._analyze_parquet(df, filename, scenario_id)

                    # summary_type에 따라 처리
                    if summary_type == "structure":
                        # 구조 정보만
                        return {
                            "success": True,
                            "message": f"파일 '{filename}'의 구조:\n{analysis.get('structure_str', '')}",
                            "filename": filename,
                            "structure": analysis.get("structure"),
                        }
                    elif summary_type == "full":
                        # 전체 분석 결과
                        return {
                            "success": True,
                            "message": f"파일 '{filename}'의 전체 분석:\n{analysis.get('full_summary', '')}",
                            "filename": filename,
                            "analysis": analysis,
                        }
                    else:
                        # summary: AI에게 분석 요청
                        import json
                        content_str = json.dumps(analysis.get("summary_info", analysis), indent=2, ensure_ascii=False)

                        return {
                            "success": True,
                            "message": f"파일 '{filename}'의 내용을 분석 중입니다.",
                            "filename": filename,
                            "content_preview": content_str[:60000],  # 복잡한 시나리오 대응
                            "full_content": analysis,
                            "needs_ai_analysis": True,
                        }

                except Exception as e:
                    logger.error(f"Failed to analyze parquet file: {str(e)}")
                    return {
                        "success": False,
                        "message": f"Parquet 파일 분석 중 오류가 발생했습니다: {str(e)}",
                        "error": str(e),
                    }
            
            else:
                return {
                    "success": False,
                    "message": f"지원하지 않는 파일 형식입니다: {filename}",
                    "error": "Unsupported file type",
                }
        
        except Exception as e:
            logger.error(f"Failed to read file: {str(e)}")
            return {
                "success": False,
                "message": f"파일 읽기 중 오류가 발생했습니다: {str(e)}",
                "error": str(e),
            }
    
    def _get_json_structure(self, obj: Any, max_depth: int = 3, current_depth: int = 0) -> Any:
        """JSON 구조 추출 (재귀적)"""
        if current_depth >= max_depth:
            return "..."

        if isinstance(obj, dict):
            return {k: self._get_json_structure(v, max_depth, current_depth + 1) for k, v in obj.items()}
        elif isinstance(obj, list):
            if len(obj) == 0:
                return []
            return [self._get_json_structure(obj[0], max_depth, current_depth + 1), f"... ({len(obj)} items)"]
        else:
            return type(obj).__name__

    async def _analyze_show_up_passenger(self, df, filename: str, scenario_id: str) -> Dict[str, Any]:
        """
        show-up-passenger.parquet 전용 분석
        metadata.json 설정과 실제 결과를 비교 분석

        Args:
            df: pandas DataFrame
            filename: 파일명
            scenario_id: 시나리오 ID

        Returns:
            분석 결과 딕셔너리
        """
        import pandas as pd
        import numpy as np

        # 1. metadata.json 로드
        metadata = None
        try:
            metadata = await self.simulation_service.s3_manager.get_json_async(
                scenario_id=scenario_id,
                filename="metadata.json"
            )
        except Exception as e:
            logger.warning(f"Could not load metadata.json: {str(e)}")

        analysis = {
            "파일_정보": {
                "파일명": filename,
                "총_승객_수": len(df),
                "총_컬럼_수": len(df.columns),
                "메모리_사용량_MB": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            },
            "항공편_통계": {},
            "항공사별_분석": {},
            "터미널별_분석": {},
            "인구통계_분석": {},
            "도착_패턴_분석": {},
            "설정_vs_실제": {},
            "sample_data": [],
        }

        # 2. 항공편 통계
        if "flight_number" in df.columns and "operating_carrier_iata" in df.columns:
            unique_flights = df[["operating_carrier_iata", "flight_number", "flight_date"]].drop_duplicates()
            analysis["항공편_통계"]["총_항공편_수"] = len(unique_flights)

            if "total_seats" in df.columns:
                unique_flights_with_seats = df[
                    ["operating_carrier_iata", "flight_number", "flight_date", "total_seats"]
                ].drop_duplicates(subset=["operating_carrier_iata", "flight_number", "flight_date"], keep="first")
                avg_seats = unique_flights_with_seats["total_seats"].mean()
                analysis["항공편_통계"]["평균_좌석_수"] = round(float(avg_seats), 2)

        # 3. 항공사별 분석
        if "operating_carrier_name" in df.columns or "operating_carrier_iata" in df.columns:
            carrier_col = "operating_carrier_name" if "operating_carrier_name" in df.columns else "operating_carrier_iata"
            carrier_stats = df.groupby(carrier_col).agg({
                "flight_number": "count",  # 승객 수
            }).rename(columns={"flight_number": "승객_수"})

            # 항공편 수 계산
            if "flight_number" in df.columns:
                flights_per_carrier = df.groupby(carrier_col)[
                    ["flight_number", "flight_date"]
                ].apply(lambda x: x.drop_duplicates().shape[0])
                carrier_stats["항공편_수"] = flights_per_carrier

            # 평균 탑승률 계산 (승객 수 / (항공편 수 * 평균 좌석 수))
            if "total_seats" in df.columns and "항공편_수" in carrier_stats.columns:
                avg_seats_per_carrier = df.groupby(carrier_col).agg({
                    "total_seats": lambda x: x.iloc[0] if len(x) > 0 else 0
                })["total_seats"]

                carrier_stats["평균_탑승률_%"] = (
                    carrier_stats["승객_수"] / (carrier_stats["항공편_수"] * avg_seats_per_carrier) * 100
                ).round(2)

            # 상위 10개 항공사만
            carrier_stats_sorted = carrier_stats.sort_values("승객_수", ascending=False).head(10)
            analysis["항공사별_분석"] = carrier_stats_sorted.to_dict("index")

        # 4. 터미널별 분석
        for terminal_col in ["departure_terminal", "arrival_terminal"]:
            if terminal_col in df.columns:
                terminal_stats = df[terminal_col].value_counts().head(10).to_dict()
                analysis["터미널별_분석"][terminal_col] = {
                    str(k): int(v) for k, v in terminal_stats.items()
                }

        # 5. 인구통계 분석 (nationality, profile)
        for demo_col in ["nationality", "profile"]:
            if demo_col in df.columns:
                # 실제 분포
                actual_dist = df[demo_col].value_counts()
                actual_pct = (actual_dist / len(df) * 100).round(2)

                demo_analysis = {
                    "실제_분포_승객수": actual_dist.head(10).to_dict(),
                    "실제_분포_%": actual_pct.head(10).to_dict(),
                }

                # metadata 설정과 비교
                if metadata and "passenger" in metadata:
                    pax_demographics = metadata["passenger"].get("pax_demographics", {})
                    if demo_col in pax_demographics:
                        config = pax_demographics[demo_col]
                        default_dist = config.get("default", {})
                        # flightCount 제외
                        default_dist = {k: v for k, v in default_dist.items() if k != "flightCount"}
                        if default_dist:
                            demo_analysis["설정된_분포_%"] = default_dist

                            # 차이 계산
                            diff = {}
                            for key in default_dist.keys():
                                actual_val = actual_pct.get(key, 0)
                                config_val = default_dist[key]
                                diff[key] = round(actual_val - config_val, 2)
                            demo_analysis["차이_%_실제빼기설정"] = diff

                analysis["인구통계_분석"][demo_col] = demo_analysis

        # 6. 도착 패턴 분석
        if "show_up_time" in df.columns and "scheduled_departure_local" in df.columns:
            # 출발시간 대비 도착시간 차이 (분 단위)
            df_temp = df.copy()
            df_temp["show_up_time"] = pd.to_datetime(df_temp["show_up_time"])
            df_temp["scheduled_departure_local"] = pd.to_datetime(df_temp["scheduled_departure_local"])
            df_temp["minutes_before"] = (
                df_temp["scheduled_departure_local"] - df_temp["show_up_time"]
            ).dt.total_seconds() / 60

            arrival_stats = {
                "평균_도착시간_분전": round(float(df_temp["minutes_before"].mean()), 2),
                "중앙값_도착시간_분전": round(float(df_temp["minutes_before"].median()), 2),
                "표준편차_분": round(float(df_temp["minutes_before"].std()), 2),
                "최소_도착시간_분전": round(float(df_temp["minutes_before"].min()), 2),
                "최대_도착시간_분전": round(float(df_temp["minutes_before"].max()), 2),
            }

            # metadata 설정과 비교
            if metadata and "passenger" in metadata:
                pax_arrival = metadata["passenger"].get("pax_arrival_patterns", {})
                default_pattern = pax_arrival.get("default", {})
                if default_pattern:
                    arrival_stats["설정된_평균_분"] = default_pattern.get("mean")
                    arrival_stats["설정된_표준편차_분"] = default_pattern.get("std")

                    # 차이
                    if "mean" in default_pattern:
                        arrival_stats["평균_차이_분_실제빼기설정"] = round(
                            arrival_stats["평균_도착시간_분전"] - default_pattern["mean"], 2
                        )

                settings = metadata["passenger"].get("settings", {})
                if "min_arrival_minutes" in settings:
                    arrival_stats["설정된_최소도착시간_분"] = settings["min_arrival_minutes"]

            analysis["도착_패턴_분석"] = arrival_stats

            # 시간대별 분포 (상위 20개)
            df_temp["show_up_hour"] = df_temp["show_up_time"].dt.strftime("%Y-%m-%d %H:00")
            hourly_dist = df_temp["show_up_hour"].value_counts().sort_index().head(20)
            analysis["도착_패턴_분석"]["시간대별_승객수_상위20"] = hourly_dist.to_dict()

        # 7. 탑승률 분석 (설정 vs 실제)
        if metadata and "passenger" in metadata:
            pax_generation = metadata["passenger"].get("pax_generation", {})
            default_load_factor = pax_generation.get("default", {}).get("load_factor")

            if default_load_factor is not None:
                # 설정값
                display_lf = int(default_load_factor) if default_load_factor > 1 else int(default_load_factor * 100)
                analysis["설정_vs_실제"]["설정된_탑승률_%"] = display_lf

                # 실제 탑승률 계산
                if "total_seats" in df.columns and "항공편_수" in analysis["항공편_통계"]:
                    total_passengers = len(df)
                    total_flights = analysis["항공편_통계"]["총_항공편_수"]
                    avg_seats = analysis["항공편_통계"].get("평균_좌석_수", 0)

                    if total_flights > 0 and avg_seats > 0:
                        actual_lf = (total_passengers / (total_flights * avg_seats)) * 100
                        analysis["설정_vs_실제"]["실제_탑승률_%"] = round(actual_lf, 2)
                        analysis["설정_vs_실제"]["탑승률_차이_%"] = round(actual_lf - display_lf, 2)

        # 8. 샘플 데이터 (처음 3개)
        sample_rows = df.head(3).to_dict("records")
        for row in sample_rows:
            clean_row = {}
            for k, v in row.items():
                if pd.isna(v):
                    clean_row[k] = None
                elif isinstance(v, (pd.Timestamp, np.datetime64)):
                    clean_row[k] = str(v)
                elif isinstance(v, (pd.Timedelta, np.timedelta64)):
                    clean_row[k] = str(v)
                elif isinstance(v, (np.integer, np.floating)):
                    clean_row[k] = float(v) if isinstance(v, np.floating) else int(v)
                else:
                    clean_row[k] = str(v) if not isinstance(v, (str, int, float, bool)) else v
            analysis["sample_data"].append(clean_row)

        # 요약 정보 생성
        summary_info = {
            "파일명": filename,
            "파일_정보": analysis["파일_정보"],
            "항공편_통계": analysis["항공편_통계"],
            "항공사별_분석_상위10": analysis["항공사별_분석"],
            "인구통계_분석": analysis["인구통계_분석"],
            "도착_패턴_분석": analysis["도착_패턴_분석"],
            "설정_vs_실제": analysis["설정_vs_실제"],
        }

        # 구조 문자열
        structure_str = f"총 {len(df):,}명 승객, {len(df.columns)}개 컬럼\n"
        structure_str += f"항공편 수: {analysis['항공편_통계'].get('총_항공편_수', 0)}편\n"
        structure_str += f"컬럼 목록: {', '.join(df.columns.tolist()[:15])}"
        if len(df.columns) > 15:
            structure_str += f"... (외 {len(df.columns) - 15}개)"

        # 전체 요약
        full_summary = f"파일: {filename}\n"
        full_summary += f"총 승객 수: {len(df):,}명\n"
        full_summary += f"총 항공편 수: {analysis['항공편_통계'].get('총_항공편_수', 0)}편\n"
        full_summary += f"평균 좌석 수: {analysis['항공편_통계'].get('평균_좌석_수', 0)}\n"
        if "설정_vs_실제" in analysis and "실제_탑승률_%" in analysis["설정_vs_실제"]:
            full_summary += f"실제 탑승률: {analysis['설정_vs_실제']['실제_탑승률_%']}%\n"

        analysis["summary_info"] = summary_info
        analysis["structure"] = {"rows": len(df), "columns": df.columns.tolist()}
        analysis["structure_str"] = structure_str
        analysis["full_summary"] = full_summary

        return analysis

    async def _analyze_parquet(self, df, filename: str, scenario_id: str = None) -> Dict[str, Any]:
        """
        Parquet 파일 분석 (파일 타입별 최적화)

        Args:
            df: pandas DataFrame
            filename: 파일명
            scenario_id: 시나리오 ID (show-up-passenger 분석 시 metadata 로드용)

        Returns:
            분석 결과 딕셔너리
        """
        import pandas as pd
        import numpy as np

        # show-up-passenger.parquet 특화 분석
        if "show-up-passenger" in filename and scenario_id:
            return await self._analyze_show_up_passenger(df, filename, scenario_id)

        # simulation-pax.parquet 또는 일반 parquet 분석
        analysis = {
            "basic_info": {
                "총_행_수": len(df),
                "총_컬럼_수": len(df.columns),
                "메모리_사용량_MB": round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            },
            "columns": {},
            "process_analysis": {},
            "sample_data": [],
        }

        # 컬럼 정보
        for col in df.columns:
            dtype_str = str(df[col].dtype)
            null_count = int(df[col].isnull().sum())
            null_pct = round(null_count / len(df) * 100, 2) if len(df) > 0 else 0

            col_info = {
                "데이터_타입": dtype_str,
                "결측값_수": null_count,
                "결측값_비율": f"{null_pct}%",
            }

            # 숫자형 컬럼 통계
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info["통계"] = {
                    "평균": round(float(df[col].mean()), 2) if not df[col].isnull().all() else None,
                    "최소": round(float(df[col].min()), 2) if not df[col].isnull().all() else None,
                    "최대": round(float(df[col].max()), 2) if not df[col].isnull().all() else None,
                }
            # 범주형 컬럼 통계
            elif pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_categorical_dtype(df[col]):
                unique_count = df[col].nunique()
                col_info["고유값_수"] = unique_count
                if unique_count <= 20:  # 고유값이 적으면 값 분포 표시
                    value_counts = df[col].value_counts().head(10).to_dict()
                    col_info["값_분포"] = {str(k): int(v) for k, v in value_counts.items()}

            analysis["columns"][col] = col_info

        # simulation-pax.parquet 특화 분석
        # 프로세스별 분석 (컬럼명 패턴: {process_name}_status, {process_name}_done_time 등)
        process_names = set()
        for col in df.columns:
            if "_status" in col:
                process_name = col.replace("_status", "")
                process_names.add(process_name)

        for process_name in sorted(process_names):
            process_info = {}

            # Status 분석
            status_col = f"{process_name}_status"
            if status_col in df.columns:
                status_counts = df[status_col].value_counts().to_dict()
                process_info["status_분포"] = {str(k): int(v) for k, v in status_counts.items()}

                total_passengers = len(df)
                if "completed" in status_counts:
                    process_info["완료율"] = f"{round(status_counts['completed'] / total_passengers * 100, 2)}%"

            # 대기 시간 분석
            queue_wait_col = f"{process_name}_queue_wait_time"
            if queue_wait_col in df.columns:
                # Timedelta를 초 단위로 변환
                wait_times = df[queue_wait_col].dropna()
                if len(wait_times) > 0:
                    # Timedelta를 초로 변환
                    if pd.api.types.is_timedelta64_dtype(wait_times):
                        wait_seconds = wait_times.dt.total_seconds()
                        process_info["대기시간_통계_초"] = {
                            "평균": round(float(wait_seconds.mean()), 2),
                            "중앙값": round(float(wait_seconds.median()), 2),
                            "최소": round(float(wait_seconds.min()), 2),
                            "최대": round(float(wait_seconds.max()), 2),
                        }

            # 큐 길이 분석
            queue_length_col = f"{process_name}_queue_length"
            if queue_length_col in df.columns:
                queue_lengths = df[queue_length_col].dropna()
                if len(queue_lengths) > 0:
                    process_info["큐_길이_통계"] = {
                        "평균": round(float(queue_lengths.mean()), 2),
                        "최대": round(float(queue_lengths.max()), 2),
                    }

            # 시설 사용 분석
            facility_col = f"{process_name}_facility"
            if facility_col in df.columns:
                facility_counts = df[facility_col].value_counts().head(10).to_dict()
                process_info["상위_시설_사용"] = {str(k): int(v) for k, v in facility_counts.items()}

            if process_info:
                analysis["process_analysis"][process_name] = process_info

        # 샘플 데이터 (처음 5개 행)
        sample_rows = df.head(5).to_dict('records')
        for row in sample_rows:
            # Timestamp, Timedelta 등을 문자열로 변환
            clean_row = {}
            for k, v in row.items():
                if pd.isna(v):
                    clean_row[k] = None
                elif isinstance(v, (pd.Timestamp, np.datetime64)):
                    clean_row[k] = str(v)
                elif isinstance(v, (pd.Timedelta, np.timedelta64)):
                    clean_row[k] = str(v)
                elif isinstance(v, (np.integer, np.floating)):
                    clean_row[k] = float(v) if isinstance(v, np.floating) else int(v)
                else:
                    clean_row[k] = str(v) if not isinstance(v, (str, int, float, bool)) else v
            analysis["sample_data"].append(clean_row)

        # 요약 정보 생성
        summary_info = {
            "파일명": filename,
            "기본_정보": analysis["basic_info"],
            "프로세스_분석": analysis["process_analysis"],
        }

        # 구조 문자열
        structure_str = f"총 {len(df):,}행, {len(df.columns)}개 컬럼\n"
        structure_str += f"컬럼 목록: {', '.join(df.columns.tolist()[:20])}"
        if len(df.columns) > 20:
            structure_str += f"... (외 {len(df.columns) - 20}개)"

        # 전체 요약
        full_summary = f"파일: {filename}\n"
        full_summary += f"총 승객 수: {len(df):,}명\n"
        full_summary += f"총 컬럼 수: {len(df.columns)}개\n"
        full_summary += f"프로세스 수: {len(process_names)}개\n"
        if process_names:
            full_summary += f"프로세스 목록: {', '.join(sorted(process_names))}\n"

        analysis["summary_info"] = summary_info
        analysis["structure"] = {"rows": len(df), "columns": df.columns.tolist()}
        analysis["structure_str"] = structure_str
        analysis["full_summary"] = full_summary

        return analysis

    async def _analyze_flight_schedule_with_response(self, df, scenario_id: str) -> Dict[str, Any]:
        """
        flight-schedule.parquet 분석 - FlightScheduleResponse 로직 사용

        Args:
            df: pandas DataFrame
            scenario_id: 시나리오 ID

        Returns:
            FlightScheduleResponse.build_response() 결과
        """
        from app.routes.simulation.application.core.flight_schedules import FlightScheduleResponse

        # metadata에서 airport, date, flight_type 추출
        metadata = None
        try:
            metadata = await self.simulation_service.s3_manager.get_json_async(
                scenario_id=scenario_id,
                filename="metadata.json"
            )
        except Exception as e:
            logger.warning(f"Could not load metadata.json: {str(e)}")

        # metadata에서 정보 추출
        airport = None
        date = None
        if metadata and "context" in metadata:
            airport = metadata["context"].get("airport")
            date = metadata["context"].get("date")

        # flight_type은 기본값 "departure" 사용 (metadata에는 없음)
        flight_type = "departure"

        # DataFrame을 list of dict로 변환
        flight_schedule_data = df.to_dict("records")

        # FlightScheduleResponse를 사용하여 응답 생성
        response_builder = FlightScheduleResponse()
        response_data = await response_builder.build_response(
            flight_schedule_data=flight_schedule_data,
            applied_conditions=None,
            flight_type=flight_type,
            airport=airport,
            date=date,
            scenario_id=scenario_id
        )

        # 항공기 정보 추출
        aircraft_info = self._extract_aircraft_info(df)

        # 목적지별 항공편 분석 (flight-schedule도 동일한 컬럼 구조)
        destination_analysis = self._analyze_destinations(df)

        # AI에게 전달할 형식으로 변환
        summary_info = {
            "파일명": "flight-schedule.parquet",
            "기본_정보": {
                "총_항공편_수": response_data.get("total", 0),
                "공항": response_data.get("airport"),
                "날짜": response_data.get("date"),
            },
            "목적지별_항공편": destination_analysis,
            "항공기_정보": aircraft_info,
            "차트_데이터": response_data.get("chart_y_data", {}),
            "parquet_메타데이터": response_data.get("parquet_metadata", [])
        }

        structure_str = f"총 {response_data.get('total', 0):,}편, {len(df.columns)}개 컬럼"
        full_summary = f"파일: flight-schedule.parquet\n"
        full_summary += f"총 항공편 수: {response_data.get('total', 0):,}편\n"

        return {
            "summary_info": summary_info,
            "structure": {"rows": len(df), "columns": df.columns.tolist()},
            "structure_str": structure_str,
            "full_summary": full_summary,
            "response_data": response_data,  # 전체 응답 데이터도 포함
        }

    async def _analyze_show_up_passenger_with_response(self, df, scenario_id: str) -> Dict[str, Any]:
        """
        show-up-passenger.parquet 분석 - ShowUpPassengerResponse 로직 사용

        Args:
            df: pandas DataFrame
            scenario_id: 시나리오 ID

        Returns:
            ShowUpPassengerResponse.build_response() 결과
        """
        from app.routes.simulation.application.core.show_up_pax import ShowUpPassengerResponse

        # metadata 로드 (config로 사용)
        metadata = None
        try:
            metadata = await self.simulation_service.s3_manager.get_json_async(
                scenario_id=scenario_id,
                filename="metadata.json"
            )
        except Exception as e:
            logger.warning(f"Could not load metadata.json: {str(e)}")

        # 목적지별 항공편 분석 (metadata 없이도 실행 가능)
        destination_analysis = self._analyze_destinations(df)

        # 항공기 정보 추출
        aircraft_info = self._extract_aircraft_info(df)

        # 승객 도착 시간 통계 추출
        passenger_arrival_stats = self._extract_passenger_arrival_stats(df)

        # 국적 및 프로필 정보 추출
        demographics_info = self._extract_demographics_info(df)

        if metadata is None:
            # metadata가 없는 경우 간단한 응답 생성
            airport_val = df['departure_airport_iata'].iloc[0] if 'departure_airport_iata' in df.columns and len(df) > 0 else None
            date_val = df['flight_date'].iloc[0] if 'flight_date' in df.columns and len(df) > 0 else None

            # Timestamp를 문자열로 변환
            if date_val is not None:
                date_val = str(date_val)

            summary_info = {
                "파일명": "show-up-passenger.parquet",
                "기본_정보": {
                    "총_승객_수": len(df),
                    "공항": str(airport_val) if airport_val is not None else None,
                    "날짜": date_val,
                },
                "목적지별_항공편": destination_analysis,
                "항공기_정보": aircraft_info,
                "승객_도착_통계": passenger_arrival_stats,
                "승객_인구통계": demographics_info,
            }

            return {
                "summary_info": summary_info,
                "structure": {"rows": len(df), "columns": df.columns.tolist()},
                "structure_str": f"총 {len(df):,}명 승객, {len(df.columns)}개 컬럼",
                "full_summary": f"파일: show-up-passenger.parquet\n총 승객 수: {len(df):,}명\n",
            }

        # metadata에서 정보 추출
        airport = None
        date = None
        if "context" in metadata:
            airport = metadata["context"].get("airport")
            date = metadata["context"].get("date")

        # config 구성 (passenger 설정 사용)
        config = {
            "settings": metadata.get("passenger", {}).get("settings", {}),
            "pax_generation": metadata.get("passenger", {}).get("pax_generation", {}),
            "pax_demographics": metadata.get("passenger", {}).get("pax_demographics", {}),
            "pax_arrival_patterns": metadata.get("passenger", {}).get("pax_arrival_patterns", {}),
        }

        # ShowUpPassengerResponse를 사용하여 응답 생성
        response_builder = ShowUpPassengerResponse()
        response_data = await response_builder.build_response(
            pax_df=df,
            config=config,
            airport=airport,
            date=date,
            scenario_id=scenario_id
        )

        # AI에게 전달할 형식으로 변환
        summary_info = {
            "파일명": "show-up-passenger.parquet",
            "기본_정보": {
                "총_승객_수": response_data.get("total", 0),
                "공항": response_data.get("airport"),
                "날짜": response_data.get("date"),
            },
            "요약_정보": response_data.get("summary", {}),
            "목적지별_항공편": destination_analysis,  # 목적지별 분석 추가
            "항공기_정보": aircraft_info,
            "승객_도착_통계": passenger_arrival_stats,
            "승객_인구통계": demographics_info,  # 국적/프로필 정보 추가
            "차트_데이터": response_data.get("chart_y_data", {}),
        }

        structure_str = f"총 {response_data.get('total', 0):,}명 승객, {len(df.columns)}개 컬럼"
        full_summary = f"파일: show-up-passenger.parquet\n"
        full_summary += f"총 승객 수: {response_data.get('total', 0):,}명\n"

        summary = response_data.get("summary", {})
        if summary:
            full_summary += f"총 항공편 수: {summary.get('flights', 0)}편\n"
            full_summary += f"평균 좌석 수: {summary.get('avg_seats', 0)}\n"
            full_summary += f"탑승률: {summary.get('load_factor', 0)}%\n"

        return {
            "summary_info": summary_info,
            "structure": {"rows": len(df), "columns": df.columns.tolist()},
            "structure_str": structure_str,
            "full_summary": full_summary,
            "response_data": response_data,  # 전체 응답 데이터도 포함
        }

    def _extract_aircraft_info(self, df) -> Dict[str, Any]:
        """
        항공기 정보 추출

        Args:
            df: pandas DataFrame

        Returns:
            항공기 정보 딕셔너리
        """
        import pandas as pd

        aircraft_info = {}

        # aircraft_type_name 우선, 없으면 aircraft_type_iata fallback
        name_col = 'aircraft_type_name' if 'aircraft_type_name' in df.columns else 'aircraft_type_iata'
        if name_col in df.columns:
            aircraft_types = df[name_col].dropna().unique()
            if len(aircraft_types) > 0:
                aircraft_list = [{"기종_명": str(v)} for v in aircraft_types]

                aircraft_info["사용_기종"] = aircraft_list
                if len(aircraft_list) == 1:
                    aircraft_info["설명"] = f"모든 항공편이 {aircraft_list[0]['기종_명']}을 사용합니다"
                else:
                    aircraft_info["설명"] = f"총 {len(aircraft_list)}개 기종을 사용합니다"

        # total_seats 정보도 추가
        if 'total_seats' in df.columns:
            seats = df['total_seats'].dropna()
            if len(seats) > 0:
                unique_seats = seats.unique()
                if len(unique_seats) == 1:
                    aircraft_info["좌석_수"] = f"{int(unique_seats[0])}석"
                else:
                    aircraft_info["좌석_수_범위"] = f"{int(seats.min())}~{int(seats.max())}석"

        return aircraft_info if aircraft_info else {"설명": "항공기 정보가 없습니다"}

    def _extract_demographics_info(self, df) -> Dict[str, Any]:
        """승객 국적 및 프로필 정보 추출"""
        import pandas as pd

        demographics = {}

        # 국적 정보
        if 'nationality' in df.columns:
            nationalities = df['nationality'].dropna()
            if len(nationalities) > 0:
                nat_counts = nationalities.value_counts()
                demographics["국적_분포"] = {
                    str(nat): int(count) for nat, count in nat_counts.items()
                }
                demographics["총_국적_수"] = len(nat_counts)

        # 프로필 정보
        if 'profile' in df.columns:
            profiles = df['profile'].dropna()
            if len(profiles) > 0:
                prof_counts = profiles.value_counts()
                demographics["프로필_분포"] = {
                    str(prof): int(count) for prof, count in prof_counts.items()
                }
                demographics["총_프로필_수"] = len(prof_counts)

        return demographics if demographics else {"설명": "국적 및 프로필 정보가 없습니다"}

    def _extract_passenger_arrival_stats(self, df) -> Dict[str, Any]:
        """
        승객 도착 시간 통계 추출

        Args:
            df: pandas DataFrame

        Returns:
            승객 도착 시간 통계
        """
        import pandas as pd
        import numpy as np

        stats = {}

        # show_up_time과 scheduled_departure_local 비교
        if 'show_up_time' in df.columns and 'scheduled_departure_local' in df.columns:
            df_temp = df.copy()
            df_temp['show_up_time'] = pd.to_datetime(df_temp['show_up_time'])
            df_temp['scheduled_departure_local'] = pd.to_datetime(df_temp['scheduled_departure_local'])

            # 출발 전 도착 시간 계산 (분 단위)
            valid_mask = df_temp['show_up_time'].notna() & df_temp['scheduled_departure_local'].notna()
            if valid_mask.sum() > 0:
                time_before_departure = (
                    df_temp.loc[valid_mask, 'scheduled_departure_local'] -
                    df_temp.loc[valid_mask, 'show_up_time']
                ).dt.total_seconds() / 60

                stats["평균_도착시간"] = f"출발 {round(time_before_departure.mean(), 1)}분 전"
                stats["중앙값_도착시간"] = f"출발 {round(time_before_departure.median(), 1)}분 전"
                stats["최소_도착시간"] = f"출발 {round(time_before_departure.min(), 1)}분 전"
                stats["최대_도착시간"] = f"출발 {round(time_before_departure.max(), 1)}분 전"
                stats["표준편차_분"] = round(float(time_before_departure.std()), 1)

                # 시간대별 분포
                if time_before_departure.mean() >= 60:
                    avg_hours = time_before_departure.mean() / 60
                    stats["설명"] = f"승객들은 평균적으로 출발 {round(avg_hours, 1)}시간 전에 공항에 도착합니다"
                else:
                    stats["설명"] = f"승객들은 평균적으로 출발 {round(time_before_departure.mean(), 1)}분 전에 공항에 도착합니다"

        return stats if stats else {"설명": "승객 도착 시간 정보가 없습니다"}

    def _analyze_destinations(self, df) -> Dict[str, Any]:
        """
        목적지별 항공편 분석 (show-up-passenger.parquet 전용)

        Args:
            df: pandas DataFrame

        Returns:
            목적지별 항공편 통계
        """
        import pandas as pd

        # arrival_city 컬럼이 없으면 빈 결과 반환
        if 'arrival_city' not in df.columns:
            return {
                "에러": "arrival_city 컬럼이 없습니다",
                "설명": "목적지 정보를 분석할 수 없습니다"
            }

        # 목적지별 분석
        destinations = {}

        # 목적지별로 그룹핑 (NaN 제외)
        valid_destinations = df[df['arrival_city'].notna()]['arrival_city'].unique()

        for destination in sorted(valid_destinations):
            dest_df = df[df['arrival_city'] == destination]

            # 항공편별로 그룹핑 (operating_carrier_name + scheduled_departure_local)
            flight_groups = dest_df.groupby(['operating_carrier_name', 'scheduled_departure_local'])

            flights_list = []
            for (carrier, departure_time), flight_df in flight_groups:
                passenger_count = len(flight_df)

                # 출발 시각 포맷팅 (HH:MM 형식)
                if pd.notna(departure_time):
                    if isinstance(departure_time, str):
                        time_str = departure_time
                    else:
                        time_str = str(departure_time)

                    # 시간만 추출
                    try:
                        if 'T' in time_str:
                            time_part = time_str.split('T')[1].split('+')[0].split('.')[0][:5]
                        elif ' ' in time_str:
                            time_part = time_str.split(' ')[1][:5]
                        else:
                            time_part = time_str[:5]
                    except:
                        time_part = str(departure_time)[:16]
                else:
                    time_part = "N/A"

                # 좌석 수 정보
                total_seats = flight_df['total_seats'].iloc[0] if 'total_seats' in flight_df.columns else None

                flight_info = {
                    "항공사": str(carrier) if pd.notna(carrier) else "알 수 없음",
                    "출발시각": time_part,
                    "승객수": passenger_count,
                }

                if total_seats is not None and pd.notna(total_seats):
                    flight_info["좌석수"] = int(total_seats)
                    flight_info["탑승률_%"] = round(passenger_count / total_seats * 100, 1)

                flights_list.append(flight_info)

            # 출발 시각 순으로 정렬
            flights_list.sort(key=lambda x: x.get("출발시각", ""))

            destinations[str(destination)] = {
                "항공편_수": len(flights_list),
                "총_승객_수": len(dest_df),
                "항공편_목록": flights_list
            }

        # 전체 통계 추가
        result = {
            "총_목적지_수": len(destinations),
            "목적지별_상세": destinations
        }

        return result

    def _analyze_flights_in_simulation(self, df, process_names: list) -> Dict[str, Any]:
        """
        항공편별 분석 - 목적지, 항공편, 승객 수, 대기 시간 통계

        show-up-passenger.parquet의 항공편 정보를 활용하여 목적지별/항공편별 통계 생성
        Lambda 시뮬레이션(lambda_function.py)은 show-up-passenger의 모든 컬럼을 유지하고
        각 프로세스별 시뮬레이션 결과 컬럼만 추가함

        Args:
            df: pandas DataFrame (simulation-pax.parquet)
            process_names: 프로세스 이름 목록

        Returns:
            항공편별 분석 결과
        """
        import pandas as pd

        logger.info(f"Starting flight analysis. DataFrame shape: {df.shape}")
        logger.info(f"Available columns (first 30): {df.columns.tolist()[:30]}")

        # 필수 컬럼 확인 (flight_number는 선택 사항)
        required_cols = ['arrival_city', 'scheduled_departure_local']
        missing_cols = [col for col in required_cols if col not in df.columns]

        # operating_carrier_iata만 사용 (실제 운항 항공사)
        if 'operating_carrier_iata' not in df.columns:
            missing_cols.append('operating_carrier_iata')

        if missing_cols:
            logger.error(f"Missing flight columns: {missing_cols}. Cannot perform flight analysis.")
            logger.error(f"Available columns: {df.columns.tolist()}")
            return {
                "에러": "항공편 정보 컬럼 누락",
                "누락된_컬럼": missing_cols,
                "사용가능한_컬럼": df.columns.tolist()[:30],  # 처음 30개만
                "설명": "show-up-passenger.parquet에서 항공편 정보가 제대로 전달되지 않았습니다."
            }

        carrier_col = 'operating_carrier_iata'
        has_flight_number = 'flight_number' in df.columns and df['flight_number'].notna().any()
        logger.info(f"Using carrier column: {carrier_col}, has_flight_number: {has_flight_number}")

        flight_analysis = {
            "목적지별_분석": {},
            "전체_항공편_통계": {
                "총_항공편_수": 0,
                "총_목적지_수": 0
            }
        }

        # 목적지별로 그룹핑 (NaN 제외)
        destinations = df['arrival_city'].dropna().unique()
        valid_dest_count = len(destinations)

        logger.info(f"Found {valid_dest_count} valid destinations (excluding NaN)")

        if valid_dest_count == 0:
            logger.warning("No valid destinations found in arrival_city column")
            return {
                "목적지별_분석": {},
                "전체_항공편_통계": {
                    "총_항공편_수": 0,
                    "총_목적지_수": 0
                },
                "경고": "arrival_city 컬럼에 유효한 목적지 데이터가 없습니다"
            }

        flight_analysis["전체_항공편_통계"]["총_목적지_수"] = valid_dest_count

        total_flights = 0

        for destination in sorted(destinations):
            if pd.isna(destination):
                continue

            dest_df = df[df['arrival_city'] == destination]

            # 항공편별로 그룹핑 (carrier_col + scheduled_departure_local, flight_number는 선택)
            if has_flight_number:
                flight_groups = dest_df.groupby([carrier_col, 'flight_number', 'scheduled_departure_local'])
            else:
                flight_groups = dest_df.groupby([carrier_col, 'scheduled_departure_local'])

            flights_list = []

            for group_keys, flight_df in flight_groups:
                # flight_number 유무에 따라 unpacking
                if has_flight_number:
                    carrier, flight_num, departure_time = group_keys
                else:
                    carrier, departure_time = group_keys
                    flight_num = None
                passenger_count = len(flight_df)

                # 출발 시각 포맷팅
                if pd.notna(departure_time):
                    if isinstance(departure_time, str):
                        departure_time_str = departure_time
                    else:
                        departure_time_str = str(departure_time)

                    # 시간만 추출 (HH:MM 형식)
                    try:
                        if 'T' in departure_time_str:
                            time_part = departure_time_str.split('T')[1].split('+')[0].split('.')[0][:5]
                        else:
                            time_part = departure_time_str.split(' ')[1][:5] if ' ' in departure_time_str else departure_time_str[:5]
                    except:
                        time_part = str(departure_time)[:16]
                else:
                    time_part = "N/A"

                # carrier 값 정리 (IATA 코드)
                carrier_code = str(carrier) if pd.notna(carrier) else ""

                flight_info = {
                    "항공사_코드": carrier_code,
                    "항공사": carrier_code,  # 호환성 유지
                    "출발시각": time_part,
                    "승객수": passenger_count
                }

                # flight_number가 있는 경우에만 추가
                if flight_num is not None and pd.notna(flight_num):
                    flight_info["편명"] = f"{carrier_code}{flight_num}" if carrier_code else str(flight_num)

                # 각 프로세스별 평균 대기 시간 계산
                for process_name in process_names:
                    status_col = f"{process_name}_status"
                    queue_wait_col = f"{process_name}_queue_wait_time"

                    # completed 승객만 대상
                    if status_col in flight_df.columns:
                        completed_mask = flight_df[status_col] == "completed"

                        if queue_wait_col in flight_df.columns:
                            queue_wait_times = flight_df.loc[completed_mask, queue_wait_col].dropna()

                            if len(queue_wait_times) > 0 and pd.api.types.is_timedelta64_dtype(queue_wait_times):
                                avg_wait_seconds = queue_wait_times.dt.total_seconds().mean()
                                avg_wait_minutes = avg_wait_seconds / 60
                                flight_info[f"{process_name}_평균대기_분"] = round(float(avg_wait_minutes), 2)
                            else:
                                flight_info[f"{process_name}_평균대기_분"] = 0.0
                        else:
                            flight_info[f"{process_name}_평균대기_분"] = 0.0

                flights_list.append(flight_info)

            # 목적지별 통계 생성
            flight_analysis["목적지별_분석"][str(destination)] = {
                "항공편_수": len(flights_list),
                "총_승객_수": len(dest_df),
                "항공편_목록": sorted(flights_list, key=lambda x: x.get("출발시각", ""))
            }

            total_flights += len(flights_list)

        flight_analysis["전체_항공편_통계"]["총_항공편_수"] = total_flights

        return flight_analysis

    async def _analyze_simulation_pax_with_response(self, df, scenario_id: str) -> Dict[str, Any]:
        """
        simulation-pax.parquet 분석 - Lambda 시뮬레이션 로직 기반 설명

        Lambda 함수(lambda_function.py)가 수행하는 이벤트 기반 시뮬레이션의 결과를 분석합니다:

        **중요: Lambda는 show-up-passenger.parquet의 모든 컬럼을 유지합니다**
        - lambda_function.py의 process_all_steps()에서 df.copy()로 원본 유지
        - 즉, arrival_city, carrier, flight_number, scheduled_departure_local 등 모든 항공편 정보 포함
        - 시뮬레이션 결과 컬럼만 추가: {process}_status, {process}_queue_wait_time 등

        시뮬레이션 원리:
        1. 각 프로세스별로 승객이 show_up_time 또는 이전 단계 done_time에서 시작
        2. travel_time_minutes를 더해 프로세스 도착 예정 시간(on_pred) 계산
        3. 가능한 시설 중 가장 빨리 처리될 시설에 배정
        4. open_wait_time(시설 오픈 대기) + queue_wait_time(큐 대기) 계산
        5. status: completed(처리 완료), failed(시설 없음), skipped(조건 미충족)

        Args:
            df: pandas DataFrame (simulation-pax.parquet)
            scenario_id: 시나리오 ID

        Returns:
            Lambda 시뮬레이션 원리 기반 분석 결과 + 항공편별 분석
        """
        import pandas as pd
        import numpy as np

        logger.info(f"Analyzing simulation-pax.parquet: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Checking for flight info columns: arrival_city={('arrival_city' in df.columns)}, carrier={('carrier' in df.columns)}, flight_number={('flight_number' in df.columns)}")

        # metadata.json에서 process_flow 정보 로드
        metadata = None
        try:
            metadata = await self.simulation_service.s3_manager.get_json_async(
                scenario_id=scenario_id,
                filename="metadata.json"
            )
        except Exception as e:
            logger.warning(f"Could not load metadata.json: {str(e)}")

        # 프로세스 컬럼 자동 감지
        process_names = set()
        for col in df.columns:
            if "_status" in col:
                process_name = col.replace("_status", "")
                process_names.add(process_name)

        process_names = sorted(process_names)

        # 기본 정보
        analysis = {
            "파일명": "simulation-pax.parquet",
            "기본_정보": {
                "총_승객_수": len(df),
                "총_프로세스_수": len(process_names),
                "프로세스_목록": process_names,
            },
            "시뮬레이션_원리": {
                "설명": "이벤트 기반 시뮬레이션으로 각 승객이 프로세스를 순차적으로 거치며 처리됩니다.",
                "처리_흐름": [
                    "1. 승객 공항 도착 (show_up_time)",
                    "2. 각 프로세스로 이동 (travel_time_minutes 소요)",
                    "3. 가능한 시설 중 가장 빨리 처리될 시설 배정",
                    "4. 시설 오픈 대기 (open_wait_time) + 큐 대기 (queue_wait_time)",
                    "5. 서비스 처리 후 다음 프로세스로 이동"
                ],
                "status_의미": {
                    "completed": "정상 처리 완료",
                    "failed": "시설 배정 실패 (운영 시간 부족 또는 조건 불일치)",
                    "skipped": "조건 미충족으로 건너뜀 (entry_conditions)"
                }
            },
            "프로세스별_분석": {},
            "전체_처리_통계": {}
        }

        # metadata에서 각 프로세스 설정 정보 추출
        process_configs = {}
        if metadata and "process_flow" in metadata:
            for proc in metadata["process_flow"]:
                proc_name = proc.get("name")
                process_configs[proc_name] = {
                    "travel_time_minutes": proc.get("travel_time_minutes", 0),
                    "총_zone_수": len(proc.get("zones", {})),
                    "총_시설_수": sum(
                        len(zone.get("facilities", []))
                        for zone in proc.get("zones", {}).values()
                    ),
                    "entry_conditions": proc.get("entry_conditions", [])
                }

        # 각 프로세스별 분석
        for process_name in process_names:
            process_analysis = {
                "프로세스_설정": process_configs.get(process_name, {}),
            }

            # Status 분석
            status_col = f"{process_name}_status"
            if status_col in df.columns:
                status_counts = df[status_col].value_counts().to_dict()
                total = len(df)

                process_analysis["처리_결과"] = {
                    "completed": {
                        "승객_수": status_counts.get("completed", 0),
                        "비율_%": round(status_counts.get("completed", 0) / total * 100, 2)
                    },
                    "failed": {
                        "승객_수": status_counts.get("failed", 0),
                        "비율_%": round(status_counts.get("failed", 0) / total * 100, 2)
                    },
                    "skipped": {
                        "승객_수": status_counts.get("skipped", 0),
                        "비율_%": round(status_counts.get("skipped", 0) / total * 100, 2)
                    }
                }

            # 대기 시간 분석 (completed 승객만)
            completed_mask = df[status_col] == "completed" if status_col in df.columns else pd.Series(False, index=df.index)

            # open_wait_time: 시설 오픈 대기 시간
            open_wait_col = f"{process_name}_open_wait_time"
            if open_wait_col in df.columns:
                open_wait_times = df.loc[completed_mask, open_wait_col].dropna()
                if len(open_wait_times) > 0 and pd.api.types.is_timedelta64_dtype(open_wait_times):
                    open_wait_seconds = open_wait_times.dt.total_seconds()
                    open_wait_minutes = open_wait_seconds / 60

                    process_analysis["오픈_대기시간_분"] = {
                        "평균": round(float(open_wait_minutes.mean()), 2),
                        "중앙값": round(float(open_wait_minutes.median()), 2),
                        "최대": round(float(open_wait_minutes.max()), 2),
                        "설명": "시설이 아직 오픈하지 않아서 기다린 시간"
                    }

            # queue_wait_time: 큐 대기 시간
            queue_wait_col = f"{process_name}_queue_wait_time"
            if queue_wait_col in df.columns:
                queue_wait_times = df.loc[completed_mask, queue_wait_col].dropna()
                if len(queue_wait_times) > 0 and pd.api.types.is_timedelta64_dtype(queue_wait_times):
                    queue_wait_seconds = queue_wait_times.dt.total_seconds()
                    queue_wait_minutes = queue_wait_seconds / 60

                    process_analysis["큐_대기시간_분"] = {
                        "평균": round(float(queue_wait_minutes.mean()), 2),
                        "중앙값": round(float(queue_wait_minutes.median()), 2),
                        "최대": round(float(queue_wait_minutes.max()), 2),
                        "설명": "시설이 이미 다른 승객을 처리 중이어서 대기한 시간"
                    }

            # 큐 길이 분석
            queue_length_col = f"{process_name}_queue_length"
            if queue_length_col in df.columns:
                queue_lengths = df.loc[completed_mask, queue_length_col].dropna()
                if len(queue_lengths) > 0:
                    process_analysis["큐_길이_통계"] = {
                        "평균": round(float(queue_lengths.mean()), 2),
                        "중앙값": round(float(queue_lengths.median()), 2),
                        "최대": int(queue_lengths.max()),
                        "설명": "승객이 도착했을 때 앞에 대기 중인 승객 수"
                    }

            # 시설 사용 분석
            facility_col = f"{process_name}_facility"
            if facility_col in df.columns:
                facility_counts = df.loc[completed_mask, facility_col].value_counts()

                process_analysis["시설_활용도"] = {
                    "총_사용된_시설_수": len(facility_counts),
                    "상위_10개_시설": facility_counts.head(10).to_dict(),
                    "설명": "각 시설이 처리한 승객 수"
                }

            # Zone 분포
            zone_col = f"{process_name}_zone"
            if zone_col in df.columns:
                zone_counts = df.loc[completed_mask, zone_col].value_counts().to_dict()
                process_analysis["zone_분포"] = {
                    str(k): int(v) for k, v in zone_counts.items()
                }

            analysis["프로세스별_분석"][process_name] = process_analysis

        # 전체 처리 통계
        if process_names:
            first_process = process_names[0]
            last_process = process_names[-1]

            # 전체 완료율 (마지막 프로세스 기준)
            last_status_col = f"{last_process}_status"
            if last_status_col in df.columns:
                last_status_counts = df[last_status_col].value_counts().to_dict()
                total = len(df)

                analysis["전체_처리_통계"]["최종_완료율_%"] = round(
                    last_status_counts.get("completed", 0) / total * 100, 2
                )

            # 총 처리 시간 (첫 프로세스 시작 ~ 마지막 프로세스 완료)
            first_start_col = f"{first_process}_start_time"
            last_done_col = f"{last_process}_done_time"

            if first_start_col in df.columns and last_done_col in df.columns:
                df_temp = df.copy()
                df_temp[first_start_col] = pd.to_datetime(df_temp[first_start_col])
                df_temp[last_done_col] = pd.to_datetime(df_temp[last_done_col])

                # 완료된 승객만
                completed_mask = (df_temp[first_start_col].notna()) & (df_temp[last_done_col].notna())
                if completed_mask.sum() > 0:
                    total_times = (df_temp.loc[completed_mask, last_done_col] -
                                   df_temp.loc[completed_mask, first_start_col]).dt.total_seconds() / 60

                    analysis["전체_처리_통계"]["총_처리시간_분"] = {
                        "평균": round(float(total_times.mean()), 2),
                        "중앙값": round(float(total_times.median()), 2),
                        "최소": round(float(total_times.min()), 2),
                        "최대": round(float(total_times.max()), 2),
                        "설명": "첫 프로세스 시작부터 마지막 프로세스 완료까지 소요 시간"
                    }

            # 병목 프로세스 찾기 (평균 큐 대기시간이 가장 긴 프로세스)
            max_queue_wait = 0
            bottleneck_process = None

            for process_name in process_names:
                if process_name in analysis["프로세스별_분석"]:
                    queue_wait = analysis["프로세스별_분석"][process_name].get("큐_대기시간_분", {})
                    avg_wait = queue_wait.get("평균", 0)
                    if avg_wait > max_queue_wait:
                        max_queue_wait = avg_wait
                        bottleneck_process = process_name

            if bottleneck_process:
                analysis["전체_처리_통계"]["병목_프로세스"] = {
                    "프로세스_이름": bottleneck_process,
                    "평균_큐_대기시간_분": round(max_queue_wait, 2),
                    "설명": "큐 대기시간이 가장 긴 프로세스"
                }

        # 항공편별 분석 추가 (Lambda는 show-up-passenger의 모든 컬럼을 유지함)
        logger.info("Calling _analyze_flights_in_simulation...")
        flight_analysis = self._analyze_flights_in_simulation(df, process_names)

        if flight_analysis is None:
            logger.error("Flight analysis returned None - this should not happen")
            flight_analysis = {
                "에러": "항공편 분석 실패",
                "설명": "예상치 못한 오류로 항공편 분석을 수행할 수 없습니다."
            }

        analysis["항공편별_분석"] = flight_analysis
        logger.info(f"Flight analysis completed. Keys: {list(flight_analysis.keys())}")

        # summary_info 생성 (AI에게 전달할 형식)
        # 항공편별_분석을 앞쪽에 배치하여 JSON 직렬화 시 20000자 제한에 잘리지 않도록 함
        summary_info = {
            "파일명": "simulation-pax.parquet",
            "기본_정보": analysis["기본_정보"],
            "항공편별_분석": analysis["항공편별_분석"],  # 사용자 질문에 중요한 정보이므로 앞쪽 배치
            "시뮬레이션_원리": analysis["시뮬레이션_원리"],
            "전체_처리_통계": analysis["전체_처리_통계"],
            "프로세스별_분석": analysis["프로세스별_분석"],  # 상세한 정보는 뒤쪽 배치
        }

        structure_str = f"총 {len(df):,}명, {len(df.columns)}개 컬럼, {len(process_names)}개 프로세스"

        full_summary = f"파일: simulation-pax.parquet\n"
        full_summary += f"총 승객 수: {len(df):,}명\n"
        full_summary += f"프로세스 수: {len(process_names)}개\n"
        full_summary += f"프로세스 목록: {', '.join(process_names)}\n"

        # 항공편별 분석 요약
        if "전체_항공편_통계" in flight_analysis:
            stats = flight_analysis["전체_항공편_통계"]
            full_summary += f"총 목적지 수: {stats.get('총_목적지_수', 0)}개\n"
            full_summary += f"총 항공편 수: {stats.get('총_항공편_수', 0)}편\n"
        elif "에러" in flight_analysis:
            full_summary += f"항공편 분석 에러: {flight_analysis.get('설명', '알 수 없는 오류')}\n"

        if "최종_완료율_%" in analysis["전체_처리_통계"]:
            full_summary += f"최종 완료율: {analysis['전체_처리_통계']['최종_완료율_%']}%\n"

        return {
            "summary_info": summary_info,
            "structure": {"rows": len(df), "columns": df.columns.tolist()},
            "structure_str": structure_str,
            "full_summary": full_summary,
        }

    async def get_scenario_context(self, scenario_id: str) -> Dict[str, Any]:
        """
        시나리오 컨텍스트 조회 (AI에게 제공할 정보)

        Returns:
            시나리오 컨텍스트 정보
        """
        try:
            metadata_result = await self.simulation_service.load_scenario_metadata(scenario_id)
            metadata = metadata_result.get("metadata", {})

            if metadata is None:
                return {
                    "scenario_id": scenario_id,
                    "process_flow": [],
                    "has_metadata": False,
                }

            process_flow = metadata.get("process_flow", [])

            return {
                "scenario_id": scenario_id,
                "process_flow": process_flow,
                "process_count": len(process_flow),
                "process_names": [p.get("name") for p in process_flow],
                "has_metadata": True,
                "context": metadata.get("context", {}),
            }

        except Exception as e:
            logger.error(f"Failed to get scenario context: {str(e)}")
            return {
                "scenario_id": scenario_id,
                "error": str(e),
            }

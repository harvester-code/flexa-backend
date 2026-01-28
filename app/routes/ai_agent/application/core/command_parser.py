"""
명령 파싱 서비스 - Function Calling을 사용하여 사용자 명령을 파싱
"""
import json
import os
import aiohttp
from typing import Dict, Any, Optional
from loguru import logger

from app.routes.ai_agent.interface.schema import Message
from .command_executor import CommandExecutor


class CommandParser:
    """명령 파싱 전담 클래스 - Function Calling 사용"""
    
    def __init__(self, command_executor: CommandExecutor):
        self.command_executor = command_executor
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.base_url = "https://api.openai.com/v1"
    
    def _get_functions(self) -> list:
        """Function Calling용 함수 정의"""
        return [
            {
                "name": "add_process",
                "description": "프로세스 플로우에 새 프로세스를 추가합니다. 예: 'checkin 프로세스 추가해줘', '보안검색 단계 추가'",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "process_name": {
                            "type": "string",
                            "description": "추가할 프로세스 이름 (예: checkin, security_check, 체크인, 보안검색)"
                        }
                    },
                    "required": ["process_name"],
                    "additionalProperties": False
                },
                "strict": True  # Structured Outputs 활성화
            },
            {
                "name": "remove_process",
                "description": "프로세스 플로우에서 프로세스를 삭제합니다. 예: 'checkin 프로세스 삭제해줘', '보안검색 단계 제거'",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "process_name": {
                            "type": "string",
                            "description": "삭제할 프로세스 이름"
                        }
                    },
                    "required": ["process_name"],
                    "additionalProperties": False
                },
                "strict": True
            },
            {
                "name": "list_processes",
                "description": "현재 프로세스 플로우 목록을 조회합니다. 예: '프로세스 목록 보여줘', '현재 설정된 단계들 알려줘'",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                    "additionalProperties": False
                },
                "strict": True
            },
            {
                "name": "list_files",
                "description": "S3 폴더에 있는 파일 목록을 조회합니다. 예: '무슨 파일 있는지 확인해', 'S3 파일 목록 보여줘', '시나리오 폴더의 파일들 알려줘'",
                "parameters": {
                    "type": "object",
                    "properties": {},
                    "required": [],
                    "additionalProperties": False
                },
                "strict": True
            },
            {
                "name": "read_file",
                "description": "S3에 있는 파일의 내용을 읽고 분석합니다. 예: 'metadata-for-frontend.json 내용 요약해', 'home-static-response.json 분석해', '파일 내용 보여줘'",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "filename": {
                            "type": "string",
                            "description": "읽을 파일 이름 (예: metadata-for-frontend.json, home-static-response.json)"
                        }
                    },
                    "required": ["filename"],
                    "additionalProperties": False
                },
                "strict": True
            }
        ]
    
    async def parse_command(
        self,
        user_content: str,
        scenario_id: str,
        model: str = "gpt-4o-2024-08-06",
        temperature: float = 0.1
    ) -> Dict[str, Any]:
        """
        사용자 명령을 파싱하여 실행 가능한 액션으로 변환
        
        Args:
            user_content: 사용자 명령 (예: "checkin 프로세스 추가해줘")
            scenario_id: 시나리오 ID
            model: 사용할 OpenAI 모델
            temperature: temperature 설정
        
        Returns:
            파싱된 명령 정보
        """
        try:
            # 1. 시나리오 컨텍스트 조회
            context = await self.command_executor.get_scenario_context(scenario_id)
            
            # 2. System Prompt 구성
            system_prompt = f"""당신은 Flexa 공항 시뮬레이션 시스템의 AI 어시스턴트입니다.

현재 시나리오 정보:
- 시나리오 ID: {scenario_id}
- 프로세스 개수: {context.get('process_count', 0)}개
- 현재 프로세스 목록: {', '.join(context.get('process_names', [])) or '없음'}

사용 가능한 명령:
1. 프로세스 추가: "checkin 프로세스 추가해줘", "보안검색 단계 추가", "체크인 카운터 프로세스 추가"
2. 프로세스 삭제: "checkin 프로세스 삭제해줘", "보안검색 단계 제거"
3. 프로세스 목록: "프로세스 목록 보여줘", "현재 설정된 단계들 알려줘"
4. 파일 목록: "무슨 파일 있는지 확인해", "S3 파일 목록 보여줘", "시나리오 폴더의 파일들 알려줘"
5. 파일 읽기/분석: "metadata-for-frontend.json 내용 요약해", "home-static-response.json 분석해", "파일 내용 보여줘"

중요 규칙:
- 프로세스 이름은 영어로 정규화됩니다 (예: "체크인" -> "check_in", "checkin" -> "check_in")
- step 번호는 자동으로 할당됩니다
- zones는 빈 객체로 시작하며, 나중에 UI에서 설정됩니다

사용자의 명령을 분석하여 적절한 함수를 호출하세요."""

            # 3. 메시지 구성
            messages = [
                Message(role="system", content=system_prompt),
                Message(role="user", content=user_content)
            ]
            
            # 4. Function Calling 요청
            functions = self._get_functions()
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
            
            payload = {
                "model": model,
                "messages": [msg.model_dump() for msg in messages],
                "tools": [{"type": "function", "function": f} for f in functions],
                "tool_choice": "auto",  # AI가 적절한 함수 선택
                "temperature": temperature,
                "max_tokens": 1024,
            }
            
            logger.info(f"Calling OpenAI API for command parsing: {user_content[:50]}...")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"OpenAI API error: {error_text}")
                        return {
                            "action": "error",
                            "error": f"OpenAI API error: {error_text}",
                        }
                    
                    result = await response.json()
                    
                    # 5. Function 호출 결과 파싱
                    message = result.get("choices", [{}])[0].get("message", {})
                    tool_calls = message.get("tool_calls", [])
                    
                    if not tool_calls:
                        # 함수 호출이 없는 경우 - 일반 대화로 처리
                        content = message.get("content", "")
                        return {
                            "action": "chat",
                            "content": content,
                            "model": result.get("model"),
                            "usage": result.get("usage", {}),
                        }
                    
                    # 첫 번째 tool call 사용
                    tool_call = tool_calls[0]
                    function_name = tool_call.get("function", {}).get("name")
                    function_args_str = tool_call.get("function", {}).get("arguments", "{}")
                    
                    try:
                        function_args = json.loads(function_args_str)
                    except json.JSONDecodeError:
                        logger.error(f"Failed to parse function arguments: {function_args_str}")
                        return {
                            "action": "error",
                            "error": "Failed to parse function arguments",
                        }
                    
                    logger.info(f"Parsed command: {function_name} with args: {function_args}")
                    
                    return {
                        "action": function_name,
                        "parameters": function_args,
                        "model": result.get("model"),
                        "usage": result.get("usage", {}),
                    }
        
        except Exception as e:
            logger.error(f"Failed to parse command: {str(e)}")
            return {
                "action": "error",
                "error": str(e),
            }
    
    async def analyze_file_content(
        self,
        scenario_id: str,
        filename: str,
        file_content: Any,
        user_query: str,
        model: str = "gpt-4o-2024-08-06",
        temperature: float = 0.1
    ) -> Dict[str, Any]:
        """
        파일 내용을 AI에게 전달하여 분석
        
        Args:
            scenario_id: 시나리오 ID
            filename: 파일 이름
            file_content: 파일 내용
            user_query: 사용자 질문
            model: 사용할 모델
            temperature: temperature
        
        Returns:
            AI 분석 결과
        """
        try:
            import json
            
            # file_content는 이미 구조화된 요약 정보 (content_preview)
            # content_preview를 직접 사용
            if isinstance(file_content, dict) and "content_preview" in file_content:
                # command_executor에서 전달된 구조화된 요약 정보 사용
                content_str = file_content.get("content_preview", "")
            elif isinstance(file_content, dict):
                content_str = json.dumps(file_content, indent=2, ensure_ascii=False)
            else:
                content_str = str(file_content)
            
            # content_str이 너무 크면 일부만 사용 (이미 요약된 정보이므로 거의 발생하지 않음)
            if len(content_str) > 20000:
                content_str = content_str[:20000] + "\n\n... (내용이 길어 일부만 표시했습니다)"
            
            system_prompt = f"""당신은 Flexa 공항 시뮬레이션 시스템의 데이터 분석가입니다. 일반 사용자에게 친화적이고 구체적으로 설명하세요.

현재 시나리오 ID: {scenario_id}
분석할 파일: {filename}

파일 내용 (구조화된 요약):
{content_str[:20000]}

**중요 지침:**

**Parquet 파일 공통 규칙 (모든 .parquet 파일에 적용):**
- 다음 컬럼들은 **절대 사용하지 마세요** (분석에서 완전히 무시):
  * 모든 _icao로 끝나는 컬럼 (예: operating_carrier_icao, departure_airport_icao, arrival_airport_icao, aircraft_type_icao, marketing_carrier_icao)
  * marketing_carrier_iata (대신 operating_carrier_iata만 사용)
  * data_source
  * 값이 모두 None인 컬럼 (예: flight_number, departure_timezone, arrival_timezone, first_class_seat_count 등)

- **nationality, profile 컬럼 처리:**
  * 값이 None → "국적(또는 프로필) 설정이 되어있지 않습니다" 라고 설명
  * 값이 있음 → 해당 값을 분석에 사용하여 구체적으로 설명

- **사용해야 하는 핵심 컬럼:**
  * 항공사: operating_carrier_iata, operating_carrier_name
  * 공항: departure_airport_iata, arrival_airport_iata
  * 도시: departure_city, arrival_city
  * 시간: scheduled_departure_local, scheduled_arrival_local, show_up_time
  * 좌석: total_seats
  * 항공기: aircraft_type_iata

1. 기술 용어나 JSON 키 이름을 절대 사용하지 마세요. 항상 일반 사용자가 이해하기 쉬운 자연스러운 한국어로 설명하세요.
   - "savedAt" → "이 파일은 [날짜/시간]에 저장되었습니다"
   - "process_flow" → "프로세스 흐름" 또는 "처리 단계" (키 이름 자체는 언급하지 않음)
   - "zones" → "구역" 또는 "영역"
   - "facilities" → "시설" 또는 "카운터"
   - "time_blocks" → "운영 시간대"

2. 반드시 구체적인 숫자와 정보를 포함하세요:
   - 프로세스가 정확히 몇 개인지
   - 각 프로세스의 실제 이름 (예: "체크인", "보안검색")
   - 각 프로세스에 구역이 몇 개인지
   - 각 구역에 시설이 몇 개인지
   - 총 시설 개수
   - 운영 시간 정보 (있는 경우)

3. 사용자가 질문한 내용에 정확히 답변하세요. "무슨 내용있는지 요약해"라고 했으면:
   - 파일에 어떤 정보가 들어있는지 구체적으로 나열
   - 각 정보의 의미를 설명
   - 숫자와 통계를 포함

4. JSON 구조나 키 이름을 설명하는 것이 아니라, 실제 데이터의 의미와 내용을 설명하세요.

5. **simulation-pax.parquet 전용 지침:**
   - 이 파일에는 "항공편별_분석" 섹션이 포함되어 있어야 합니다
   - Lambda 시뮬레이션은 show-up-passenger.parquet의 모든 컬럼(arrival_city, carrier, flight_number 등)을 유지하므로, 항공편별 통계가 가능합니다
   - 사용자가 특정 목적지(예: 제주도, 부산)로 가는 항공편에 대해 질문하면, "항공편별_분석" > "목적지별_분석" 섹션을 활용하세요
   - 각 목적지별로 항공편 수, 출발 시각, 승객 수, 각 프로세스별 평균 대기 시간이 제공됩니다
   - 예시 질문: "제주도 가는 항공편 몇 개야?" → "목적지별_분석"에서 제주도 항목의 "항공편_수"와 "항공편_목록" 확인
   - 예시 질문: "부산행 비행기 승객들 웨이팅 얼마나 겪었어?" → 해당 목적지의 각 항공편별로 "xxx_평균대기_분" 데이터 활용
   - **만약 "항공편별_분석"에 "에러" 키가 있다면**: 컬럼 누락이나 데이터 문제로 분석이 불가능하다는 것을 사용자에게 명확히 설명하고, "에러" 및 "설명" 내용을 전달하세요
   - **만약 "목적지별_분석"이 비어있다면**: 유효한 목적지 데이터가 없다는 것을 설명하세요

6. 예시:
   ❌ 나쁜 답변: "process_flow에 1개의 아이템이 있습니다"
   ✅ 좋은 답변: "현재 1개의 처리 단계가 설정되어 있습니다. '체크인' 프로세스가 있으며, 12개의 구역(A, B, C 등)에 총 144개의 카운터가 배치되어 있습니다."

사용자의 질문에 대해 파일 내용을 바탕으로 정확하고 상세하게, 일반 사용자가 이해하기 쉽게 답변하세요."""

            messages = [
                Message(role="system", content=system_prompt),
                Message(role="user", content=user_query)
            ]
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
            
            payload = {
                "model": model,
                "messages": [msg.model_dump() for msg in messages],
                "temperature": temperature,
                "max_tokens": 2048,
            }
            
            logger.info(f"Calling OpenAI API for file analysis: {filename}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"OpenAI API error: {error_text}")
                        return {
                            "success": False,
                            "error": f"OpenAI API error: {error_text}",
                        }
                    
                    result = await response.json()
                    content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                    
                    return {
                        "success": True,
                        "content": content,
                        "model": result.get("model"),
                        "usage": result.get("usage", {}),
                    }
        
        except Exception as e:
            logger.error(f"Failed to analyze file content: {str(e)}")
            return {
                "success": False,
                "error": str(e),
            }

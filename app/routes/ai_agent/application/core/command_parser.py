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
                "description": """시뮬레이션 데이터를 읽고 분석합니다. 사용자가 파일명을 명시하지 않아도 질문 의도에 맞는 파일을 자동으로 선택하세요.

**파일별 정보:**
- **show-up-passenger.parquet**: 승객들이 공항에 언제 도착하는지, 어느 항공편을 타는지, 목적지가 어디인지 등의 정보
  * 질문 예: "승객들이 언제 도착해?", "제주도 가는 항공편 몇 개야?", "승객들 언제 와?", "항공편 정보 알려줘"

- **simulation-pax.parquet**: 승객들이 각 프로세스(체크인, 보안검색 등)에서 얼마나 대기했는지 시뮬레이션 결과
  * 질문 예: "대기시간 얼마나 걸렸어?", "체크인에서 몇 분 기다렸어?", "프로세스별 대기시간 알려줘"

- **flight-schedule.parquet**: 항공편 스케줄 정보 (출발시각, 도착시각, 항공사 등)
  * 질문 예: "항공편 스케줄 보여줘", "몇 시에 출발해?", "항공편 시간표 알려줘"

- **metadata-for-frontend.json**: 사용자가 설정한 시뮬레이션 설정값 전체 (공항, 날짜, 항공편, 승객 설정, 프로세스 구성, 시설 배치, 운영 시간 등)
  * **공항/날짜 정보**: "어느 공항이야?", "공항 코드가 뭐야?", "언제 날짜야?", "시뮬레이션 날짜는?", "어느 공항의 언제 데이터야?"
  * **항공편 설정**: "총 몇 편이야?", "어느 항공사야?", "어디로 가는 항공편들이야?", "항공편 몇 시에 출발해?", "좌석 수는?"
  * **승객 설정**: "승객 몇 명 예상해?", "적재율은?", "승객들 평균 몇 분 전에 와?", "승객 도착 패턴은?"
  * **시설/프로세스 설정**: "체크인 시설 몇 개야?", "시설 설정 어떻게 되어있어?", "체크인 몇 시부터 운영해?", "A구역에 시설 몇 개 있어?", "처리 시간은 얼마로 설정했어?"

**중요:** 사용자가 파일명을 언급하지 않으면 질문 내용을 보고 가장 적절한 파일을 선택하세요.""",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "filename": {
                            "type": "string",
                            "description": "읽을 파일 이름. 사용자가 명시하지 않으면 질문 의도에 맞는 파일을 선택하세요: show-up-passenger.parquet (승객 개별 도착 시간), simulation-pax.parquet (실제 대기시간 결과), flight-schedule.parquet (항공편 스케줄), metadata-for-frontend.json (공항, 날짜, 항공편 설정, 승객 설정, 시설 설정, 프로세스 구성, 운영 시간)"
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
        conversation_history: list = None,
        model: str = "gpt-4o-2024-08-06",
        temperature: float = 0.1
    ) -> Dict[str, Any]:
        """
        사용자 명령을 파싱하여 실행 가능한 액션으로 변환

        Args:
            user_content: 사용자 명령 (예: "checkin 프로세스 추가해줘")
            scenario_id: 시나리오 ID
            conversation_history: 이전 대화 이력 (옵션)
            model: 사용할 OpenAI 모델
            temperature: temperature 설정

        Returns:
            파싱된 명령 정보
        """
        try:
            # 1. 시나리오 컨텍스트 조회
            context = await self.command_executor.get_scenario_context(scenario_id)
            
            # 2. System Prompt 구성
            system_prompt = f"""You are an AI assistant for the Flexa airport simulation system.

**LANGUAGE RULES:**
- Respond in English by default
- If the user asks in Korean, respond in Korean
- If the user asks in another language, respond in that language
- Match the language of the user's question

Current scenario information:
- Scenario ID: {scenario_id}
- Process count: {context.get('process_count', 0)}
- Current processes: {', '.join(context.get('process_names', [])) or 'None'}

Available commands:
1. Add process: "add checkin process", "보안검색 단계 추가"
2. Remove process: "remove checkin process", "보안검색 단계 제거"
3. List processes: "show process list", "프로세스 목록 보여줘"
4. List files: "list files", "무슨 파일 있는지 확인해"
5. Read/analyze file: "analyze metadata-for-frontend.json", "파일 내용 보여줘"

Important rules:
- Process names are normalized to English (e.g., "체크인" -> "check_in", "checkin" -> "check_in")
- Step numbers are automatically assigned
- Zones start as empty objects and are configured later in the UI

Analyze the user's command and call the appropriate function."""

            # 3. 메시지 구성
            messages = [Message(role="system", content=system_prompt)]

            # 대화 이력 추가 (최근 20개만, 토큰 제한 고려)
            if conversation_history:
                # 시스템 메시지와 환영 메시지 제외하고 실제 대화만 추가
                filtered_history = [
                    msg for msg in conversation_history
                    if msg.role != "system" and not (msg.role == "assistant" and "Ask me anything" in msg.content)
                ]
                # 최근 20개만 사용 (약 10턴)
                recent_history = filtered_history[-20:] if len(filtered_history) > 20 else filtered_history
                messages.extend(recent_history)

            # 현재 사용자 메시지 추가
            messages.append(Message(role="user", content=user_content))
            
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
            
            # content_str이 너무 크면 일부만 사용 (복잡한 시나리오 대응)
            if len(content_str) > 60000:
                content_str = content_str[:60000] + "\n\n... (내용이 길어 일부만 표시했습니다)"

            system_prompt = f"""You are a data analyst for the Flexa airport simulation system. Explain things in a user-friendly and specific way.

**LANGUAGE RULES (HIGHEST PRIORITY):**
- Respond in English by default
- If the user asks in Korean, respond in Korean
- If the user asks in another language, respond in that language
- Match the language of the user's question

Current scenario ID: {scenario_id}

Simulation data:
{content_str[:60000]}

**Core principles:**
- **Never mention file names**: Don't include file names like "show-up-passenger.parquet", "simulation-pax.parquet", "metadata-for-frontend.json" in your answers
- **No technical jargon**: Instead of expressions like "file analysis results", "this file contains", "according to the data", answer naturally
- **Direct answers**: Answer as if you directly ran the simulation

Examples:
❌ Bad answer: "Based on show-up-passenger.parquet analysis, passengers..."
✅ Good answer: "Passengers arrived at the airport an average of 2 hours early..."

❌ Bad answer: "This file has 2 flights to Jeju"
✅ Good answer: "There are 2 flights to Jeju"

**Important guidelines:**

**Common rules for Parquet files (applies to all .parquet files):**
- **Never use these columns** (completely ignore in analysis):
  * All columns ending with _icao (e.g., operating_carrier_icao, departure_airport_icao, arrival_airport_icao, aircraft_type_icao, marketing_carrier_icao)
  * marketing_carrier_iata (use only operating_carrier_iata instead)
  * data_source
  * Columns with all None values (e.g., flight_number, departure_timezone, arrival_timezone, first_class_seat_count, etc.)

- **nationality, profile column handling:**
  * If None → Explain "Nationality (or profile) is not configured"
  * If has value → Use the value in analysis and explain specifically

- **Key columns to use:**
  * Airlines: operating_carrier_iata, operating_carrier_name
  * Airports: departure_airport_iata, arrival_airport_iata
  * Cities: departure_city, arrival_city
  * Times: scheduled_departure_local, scheduled_arrival_local, show_up_time
  * Seats: total_seats
  * Aircraft: aircraft_type_iata

1. Never use technical terms or JSON key names. Always explain in natural language that regular users can understand.
   - "savedAt" → "This file was saved on [date/time]"
   - "process_flow" → "process flow" or "processing steps" (don't mention the key name itself)
   - "zones" → "zones" or "areas"
   - "facilities" → "facilities" or "counters"
   - "time_blocks" → "operating hours"

2. Always include specific numbers and information:
   - Exactly how many processes there are
   - The actual names of each process (e.g., "check-in", "security screening")
   - How many zones in each process
   - How many facilities in each zone
   - Total number of facilities
   - Operating hours information (if available)

3. Answer exactly what the user asked. If they asked "summarize the contents":
   - List specifically what information is in the file
   - Explain the meaning of each piece of information
   - Include numbers and statistics

4. Don't explain JSON structure or key names - explain the actual meaning and content of the data.

5. **simulation-pax.parquet specific guidelines:**
   - This file should include a "flight_analysis" section
   - Lambda simulation preserves all columns from show-up-passenger.parquet (arrival_city, carrier, flight_number, etc.), so per-flight statistics are possible
   - When users ask about flights to specific destinations (e.g., Jeju, Busan), use the "flight_analysis" > "destination_analysis" section
   - Each destination provides: number of flights, departure times, passenger counts, average wait times for each process
   - Example question: "How many flights to Jeju?" → Check "flight_count" and "flight_list" in the Jeju item under "destination_analysis"
   - Example question: "How long did passengers wait for Busan flights?" → Use "xxx_avg_wait_min" data for each flight to that destination
   - **If "flight_analysis" has an "error" key**: Clearly explain to users that analysis is not possible due to missing columns or data issues, and convey the "error" and "description" content
   - **If "destination_analysis" is empty**: Explain that there is no valid destination data

6. Examples:
   ❌ Bad answer: "There is 1 item in process_flow"
   ✅ Good answer: "There is currently 1 processing step configured. The 'check-in' process has a total of 144 counters deployed across 12 zones (A, B, C, etc.)."

Answer the user's question accurately and in detail based on the file content, in a way that regular users can easily understand."""

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

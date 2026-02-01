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
                "description": """시뮬레이션 결과 데이터를 읽고 분석합니다.

⚠️ IMPORTANT - When to use this function:
- Configuration data (airport, date, flights, passengers, processes) → ALWAYS use simulation_state, NOT read_file
- Result data (waiting times, arrival records, schedules) → Use read_file for .parquet files

**Available Files:**

- **show-up-passenger.parquet**: 시뮬레이션 실행 후 생성된 승객별 도착 시간 기록
  * 질문 예: "승객들이 실제로 언제 도착했어?", "제주도 가는 항공편에 배정된 승객 몇 명이야?"
  * ⚠️ This is RESULT data from simulation execution

- **simulation-pax.parquet**: 시뮬레이션 실행 결과 (각 프로세스에서의 실제 대기시간, 처리시간)
  * 질문 예: "대기시간 얼마나 걸렸어?", "체크인에서 몇 분 기다렸어?", "프로세스별 대기시간 분석해줘"
  * ⚠️ This is RESULT data from simulation execution

- **flight-schedule.parquet**: 항공편 스케줄 정보 (출발시각, 도착시각, 항공사 등)
  * 질문 예: "항공편 스케줄 보여줘", "몇 시에 출발하는 항공편이야?", "항공편 시간표 분석해줘"
  * ⚠️ This is RESULT data from simulation execution

⚠️ DO NOT use read_file for configuration questions:
- "어느 공항이야?" → Use simulation_state['airport']
- "승객 몇 명 생성돼?" → Use simulation_state['passenger']['total']
- "탑승률이 뭐야?" → Use simulation_state['passenger']['pax_generation']
- "프로세스가 몇 개야?" → Use simulation_state['process_count']

Only use read_file when the user asks about simulation RESULTS (.parquet files).""",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "filename": {
                            "type": "string",
                            "description": "읽을 파일 이름: show-up-passenger.parquet (승객 도착 시간 결과), simulation-pax.parquet (시뮬레이션 대기시간 결과), flight-schedule.parquet (항공편 스케줄 정보). ⚠️ Configuration questions should use simulation_state, NOT read_file."
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
        simulation_state: dict = None,
        model: str = "gpt-4o-2024-08-06",
        temperature: float = 0.1
    ) -> Dict[str, Any]:
        """
        사용자 명령을 파싱하여 실행 가능한 액션으로 변환

        Args:
            user_content: 사용자 명령 (예: "checkin 프로세스 추가해줘")
            scenario_id: 시나리오 ID
            conversation_history: 이전 대화 이력 (옵션)
            simulation_state: 현재 시뮬레이션 상태 (Zustand store에서 추출)
            model: 사용할 OpenAI 모델
            temperature: temperature 설정

        Returns:
            파싱된 명령 정보
        """
        try:
            # 1. 시나리오 컨텍스트 조회
            context = await self.command_executor.get_scenario_context(scenario_id)
            
            # 2. System Prompt 구성
            # 현재 시뮬레이션 상태 정보 추가
            simulation_status = ""
            if simulation_state:
                # 항공사 이름 리스트 생성
                airline_names = simulation_state.get('airline_names', [])
                airline_str = ', '.join(airline_names[:5]) if airline_names else 'None'
                if len(airline_names) > 5:
                    airline_str += f' and {len(airline_names) - 5} more'

                # Passenger 데이터 추출
                passenger_data = simulation_state.get('passenger', {})
                passenger_total = passenger_data.get('total', 0)
                pax_gen = passenger_data.get('pax_generation', {})
                pax_demo = passenger_data.get('pax_demographics', {})
                pax_arrival = passenger_data.get('pax_arrival_patterns', {})
                chart_result = passenger_data.get('chartResult', {})

                # 탑승률 요약
                load_factor = pax_gen.get('default', {}).get('load_factor', 'Not set')

                # 국적 요약
                nationality_default = pax_demo.get('nationality', {}).get('default', {})
                nationality_str = ', '.join([f"{k}: {v}%" for k, v in nationality_default.items() if k != 'flightCount']) if nationality_default else 'Not set'

                # 프로필 요약
                profile_default = pax_demo.get('profile', {}).get('default', {})
                profile_str = ', '.join([f"{k}: {v}%" for k, v in profile_default.items() if k != 'flightCount']) if profile_default else 'Not set'

                # 도착 패턴 요약
                arrival_mean = pax_arrival.get('default', {}).get('mean', 'Not set')

                simulation_status = f"""

**CURRENT SIMULATION STATE (Real-time from browser):**

**Basic Info:**
- Airport: {simulation_state.get('airport', 'Not set')}
- Date: {simulation_state.get('date', 'Not set')}

**Flights:**
- Total available: {simulation_state.get('flight_total', 0)} flights (loaded from database)
- Selected: {simulation_state.get('flight_selected', 0)} flights (after applying filters)
- Airlines: {airline_str}
  ⚠️ ALWAYS use full airline NAMES (e.g., "American Airlines"), NEVER codes (e.g., "AA")
  ⚠️ Airlines mapping available in simulation_state['airlines_mapping']

⚠️ **If flight_selected = 0:**
This means the user hasn't applied any filters yet. Guide them:
"Currently 0 flights are selected. To select flights:
1. Go to the Flight Schedule tab
2. Choose your filtering criteria (Type, Terminal, Location, etc.)
3. Click the 'Filter Flights' button
4. Once filtered, I will be able to recognize the selected flight information.

There are {simulation_state.get('flight_total', 0)} flights available in the database."

**Passengers (Summary):**
- Total: {passenger_total} passengers
- Load factor: {load_factor}%
- Nationality: {nationality_str}
- Profile: {profile_str}
- Arrival pattern: Mean {arrival_mean} min before departure

**Passengers (Full Data Available):**
You have access to detailed passenger data in simulation_state['passenger']:

**Data Flow: Configuration → Generation → Simulation**
```
1. pax_generation (config) → determines passenger count per flight
2. pax_demographics (config) → assigns nationality, profile to each passenger
3. pax_arrival_patterns (config) → assigns show_up_time to each passenger
4. chartResult (generated data) → summary of created passengers
5. Simulation → uses passenger fields for facility assignment
```

**1. pax_generation** - Passenger count generation (Load Factor)
```
{{
  "default": {{"load_factor": 83}},
  "rules": [
    {{
      "conditions": {{"operating_carrier_iata": ["AA"]}},
      "value": {{"load_factor": 90}}
    }}
  ]
}}
```
- **Purpose**: Determines how many passengers per flight
- **load_factor**: Percentage of seats filled (e.g., 83% = 83 passengers per 100 seats)
- **default**: Base load factor for all flights
- **rules**: Override load factor for specific conditions
  - Example: AA airline has 90% load factor
- **Result**: Each flight generates N passengers based on (seats × load_factor)

**2. pax_demographics** - Passenger attributes distribution

**2a. nationality** - Nationality distribution
```
{{
  "available_values": ["Domestic", "Foreign"],
  "default": {{"Domestic": 59, "Foreign": 41}},
  "rules": [
    {{
      "conditions": {{"flight_type": ["International"]}},
      "value": {{"Domestic": 20, "Foreign": 80}}
    }}
  ]
}}
```
- **Purpose**: Assigns nationality to each passenger
- **available_values**: Possible nationality options
- **default**: Base distribution (59% Domestic, 41% Foreign)
- **rules**: Override for specific flight types
  - Example: International flights → 80% Foreign
- **Result**: Each passenger gets a `nationality` field
- **Simulation use**: Checked in entry_conditions, passenger_conditions
  - Example: travel_tax process only for Filipino passengers

**2b. profile** - Passenger profile/type distribution
```
{{
  "available_values": ["Regular", "Fast track", "Prm", "Ofw", "Crew", "Normal"],
  "default": {{"Regular": 57, "Fast track": 43}},
  "rules": []
}}
```
- **Purpose**: Assigns passenger type/category
- **default**: Base distribution (57% Regular, 43% Fast track)
- **Result**: Each passenger gets a `profile` field
- **Simulation use**: Determines which zone/facility to use
  - Example: "PRIORITY" zone for Fast track passengers
  - Example: "REGULAR" zone for Regular passengers

**3. pax_arrival_patterns** - Airport arrival timing
```
{{
  "default": {{"mean": 180, "std": 30}},
  "rules": [
    {{
      "conditions": {{"flight_type": ["International"]}},
      "value": {{"mean": 150, "std": 30}}
    }}
  ]
}}
```
- **Purpose**: Determines when passengers arrive at airport (before flight departure)
- **mean**: Average arrival time in minutes before departure
  - Example: 180 = passengers arrive 3 hours before flight
- **std**: Standard deviation (time variance)
  - Example: std=30 means most arrive 150-210 minutes before
- **default**: Base arrival pattern (domestic flights)
- **rules**: Override for specific flight types
  - Example: International flights → arrive 150 min (2.5 hours) before
- **Result**: Each passenger gets a `show_up_time` field
- **Simulation use**: Starting point of simulation (passenger arrival event)

**4. chartResult** - Generated passenger data summary
```
{{
  "total": 3731,
  "chart_x_data": ["00:00", "01:00", "02:00", ...],
  "chart_y_data": {{
    "airline": [{{"name": "American Airlines", "y": [0, 0, 5, 10, ...]}}],
    "nationality": [{{"name": "Domestic", "y": [...]}}],
    "profile": [{{"name": "Regular", "y": [...]}}]
  }},
  "summary": {{"flights": 11, "avg_seats": 178.82, "load_factor": 83}}
}}
```
- **Purpose**: Summary of generated passengers (NOT used in simulation, just reporting)
- **total**: Total number of passengers created
- **chart_x_data**: Time slots (hourly)
- **chart_y_data**: Breakdown by category over time
  - airline: How many passengers per hour per airline
  - nationality: Distribution over time
  - profile: Distribution over time
- **summary**: Statistics
  - flights: Number of flights
  - avg_seats: Average seats per flight
  - load_factor: Actual load factor achieved
  - min_arrival_minutes: Earliest arrival time

**How Passenger Fields Are Used in Simulation:**
```
Passenger row in simulation has fields:
  - nationality: "Domestic" or "Foreign" or "Filipino" etc.
  - profile: "Regular" or "Fast track" or "Prm" etc.
  - operating_carrier_iata: "AA", "G3", etc.
  - flight_type: "Domestic" or "International"
  - show_up_time: timestamp of airport arrival

These fields are checked against:
  - entry_conditions: Who must go through this process?
  - passenger_conditions: Who can use this facility at this time?

Example:
  entry_conditions: [{{"field": "nationality", "values": ["Filipino"]}}]
  → Only passengers with nationality="Filipino" go through this process
```

**HOW TO ANSWER TIME-BASED QUESTIONS:**
Example: "아메리칸 에어라인 승객이 몇시부터 몇시까지 몇명씩 와?"

1. Find airline in chartResult.chart_y_data.airline:
   ```
   airline_data = next(item for item in chartResult['chart_y_data']['airline'] if item['name'] == 'American Airlines')
   y_values = airline_data['y']  # [0, 0, 5, 10, 20, ...]
   x_times = chartResult['chart_x_data']  # ["00:00", "01:00", "02:00", ...]
   ```

2. Analyze non-zero time slots:
   ```
   for i, count in enumerate(y_values):
       if count > 0:
           print(f"{{x_times[i]}} ~ {{x_times[i+1]}}: {{count}}명")
   ```

3. Answer in natural language: "아메리칸 에어라인 승객은 02:00~03:00에 5명, 03:00~04:00에 10명..."

**Process Flow (Summary):**
- Total: {simulation_state.get('process_count', 0)} processes
- Names: {', '.join(simulation_state.get('process_names', [])) or 'None'}

**Process Flow (Full Data Available):**
You have access to detailed process data in simulation_state['process_flow']:

**Structure:**
```
process_flow: [
  {{
    "step": 0,
    "name": "check_in",
    "travel_time_minutes": 1,
    "process_time_seconds": 180,
    "entry_conditions": [],
    "zones": {{
      "EAST KIOSK1": {{
        "facilities": [{{
          "id": "EAST KIOSK1_1",
          "operating_schedule": {{
            "time_blocks": [{{
              "period": "2026-03-01 09:30:00-2026-03-01 14:00:00",
              "process_time_seconds": 180,
              "passenger_conditions": [],
              "activate": true
            }}]
          }}
        }}]
      }}
    }}
  }}
]
```

**Key Field Meanings:**

1. **step**: Process order (0, 1, 2, ...)

2. **name**: Process name (check_in, security, passport, immigration, travel_tax, etc.)

3. **travel_time_minutes**: Walking time from PREVIOUS process to THIS process
   - Example: 1 minute to walk from check_in to security
   - Used to calculate arrival time: on_pred = prev_done_time + travel_time_minutes

4. **process_time_seconds**: Time to pass through the facility
   - ⚠️ IMPORTANT: time_block level value is actually used in simulation
   - Process level value is "default" (for UI display)
   - Each time block can have different processing times

5. **entry_conditions**: Who must go through this process
   - Example: {{"field": "nationality", "values": ["Filipino"]}} → Only Filipino passengers
   - Example: {{"field": "flight_type", "values": ["International"]}} → Only international flights
   - If matched → process proceeds
   - If not matched → status = "skipped"

6. **zones**: Physical/logical area groups
   - Example: "EAST KIOSK1", "WEST MANNED", "PRIORITY", "REGULAR"
   - Each zone contains multiple facilities

7. **facilities**: Actual service counters/machines
   - Example: "EAST KIOSK1_1", "EAST KIOSK1_2" → Kiosk machine numbers
   - Each has operating_schedule with time_blocks

8. **time_blocks**: Time-specific operating policies
   - Why needed? Different policies by time:
     * Peak hours → longer processing time
     * Specific hours → specific airlines only
     * Lunch break → facility closed
   - Each block has:
     * **period**: Operating time range (e.g., "09:30:00-14:00:00")
     * **process_time_seconds**: Processing time for THIS time period
     * **passenger_conditions**: Who can use this facility at this time
     * **activate**: true = operating, false = closed (excluded from simulation)

9. **passenger_conditions** (facility level):
   - Who can use THIS facility at THIS time
   - Example: {{"field": "operating_carrier_iata", "values": ["G3"]}} → G3 airline only
   - **Difference from entry_conditions:**
     * entry_conditions: Process-wide access (process level)
     * passenger_conditions: Facility-specific access (time_block level)

**How Simulation Works:**
```
Passenger arrives at process
  ↓
Check entry_conditions (if fail → skipped)
  ↓
Arrival time = prev_done_time + travel_time_minutes
  ↓
Find available facilities:
  - Is current time in period range?
  - Is activate = true?
  - Do passenger_conditions match?
  ↓
Select fastest facility
  ↓
Processing: start_time + process_time_seconds = done_time
  ↓
Move to next process
```

**Example Questions You Can Answer:**
- "check_in 시설 통과하는 데 얼마나 걸려?" → Look at time_blocks[].process_time_seconds
- "security까지 이동하는 데 시간 얼마나?" → travel_time_minutes
- "Filipino만 거치는 프로세스 뭐야?" → Check entry_conditions
- "G3 항공사 승객은 어느 시설 사용해?" → Check passenger_conditions
- "09:30-14:00에 운영하는 시설은?" → Check period and activate=true

**Workflow:**
- Flights tab: {'✅ Completed' if simulation_state.get('workflow', {}).get('flights_completed') else '❌ Not completed'}
- Passengers tab: {'✅ Completed' if simulation_state.get('workflow', {}).get('passengers_completed') else '❌ Not completed'}
- Current step: {simulation_state.get('workflow', {}).get('current_step', 1)}

**🎯 WORKFLOW GUIDE - Help Users Complete Each Tab Sequentially:**

**Tab 1: Flights (Flight Schedule)**
Goal: Select which flights to simulate
Steps:
1. Load flight data (airport + date) → Click "Load Data" button
2. Choose filter criteria (Type, Terminal, Location) → Optional
3. Click "Filter Flights" button → Required (even if no filters selected)
Status: {'✅ Completed - 19 flights selected' if simulation_state.get('flight_selected', 0) > 0 else '❌ Not completed - Need to click "Filter Flights" button'}

**Tab 2: Passengers (Configure Passenger Data)**
Goal: Configure passenger generation settings
4 Sub-tabs (all must be completed):
1. ✅ Nationality - Define nationality types (e.g., Filipino, Foreigner) and distribution %
   Example: {{"Filipino": 80, "Foreigner": 20}}
2. ✅ Pax Profile - Define passenger types (e.g., Normal, Prm, Ofw, Crew) and distribution %
   Example: {{"Normal": 63, "Prm": 10, "Ofw": 22, "Crew": 5}}
3. ❌ Load Factor - Click to set default seat occupancy rate (e.g., 85%)
   Default value is automatically set when clicked
4. ❌ Show-up-Time - Click to set passenger arrival time distribution (mean, std)
   Example: {{"mean": 120, "std": 30}} (arrive 120 min before departure)
   Default values are automatically set when clicked

After all 4 sub-tabs: Click "Generate Pax" button to create passengers
Status: {'✅ Completed - ' + str(passenger_total) + ' passengers generated' if passenger_total > 0 else '❌ Not completed - Need to complete all 4 sub-tabs and click "Generate Pax"'}

**Tab 3: Facilities (Process Flow)**
Goal: Add airport processes (check-in, security, etc.)
Steps:
1. Click "Add Process" or use AI chat to add processes
2. Configure zones and facilities for each process
3. Set operating hours and conditions
Status: {'✅ Completed - ' + str(simulation_state.get('process_count', 0)) + ' processes configured' if simulation_state.get('process_count', 0) > 0 else '❌ Not completed - Need to add at least 1 process'}

**BUTTON CONDITIONS:**
- Run Simulation: {'✅ Enabled' if simulation_state.get('process_count', 0) > 0 else '❌ Disabled (Need ≥1 process)'}
- Save: ✅ Always enabled
- Delete: ✅ Always enabled

**📋 HOW TO GUIDE USERS - "What should I do next?" Questions:**

When user asks "What should I do next?" or "이제 뭐해야 해?", analyze current state and guide them:

**If flight_selected = 0:**
→ "Go to Flight Schedule tab → Click 'Filter Flights' button to select flights"

**If flight_selected > 0 AND passenger.total = 0:**
→ "Great! Flights are selected. Now go to Passengers tab and complete these steps:
   1. Check that Nationality and Pax Profile tabs are completed (should have ✅)
   2. Click on 'Load Factor' tab to set default value
   3. Click on 'Show-up-Time' tab to set default value
   4. Click 'Generate Pax' button
   This will create passengers for your {simulation_state.get('flight_selected', 0)} selected flights."

**If passenger.total > 0 AND process_count = 0:**
→ "Excellent! You have {passenger_total} passengers generated. Now go to Facilities tab and add processes:
   1. Click 'Add Process' button OR
   2. Tell me which process to add (e.g., 'add check-in process', 'add security process')
   Common processes: check-in, security, passport control, immigration, boarding"

**If process_count > 0:**
→ "Perfect! You have {simulation_state.get('process_count', 0)} processes configured. Your simulation is ready!
   - Click 'Run Simulation' button to start
   - Or add more processes if needed
   - Or click 'Save' to save your configuration"

**CRITICAL ANSWERING RULES:**
⚠️ **PASSENGER DATA IS ALREADY IN SIMULATION_STATE - NEVER SAY "NOT CONFIGURED"!**

✅ DO:
1. Use simulation_state['passenger'] for ALL passenger questions
2. If passenger.total > 0, data IS configured and available
3. Use chartResult.chart_y_data for time-based questions
4. Use full airline names from airlines_mapping
5. Be specific with numbers from the data
6. Use "chat" action to answer passenger questions directly

❌ DON'T:
1. NEVER use read_file for passenger data
2. NEVER say "not configured" if passenger.total > 0
3. NEVER mention S3, JSON files, or "saved data"
4. NEVER ignore simulation_state['passenger'] data

**If passenger data exists (total > 0), YOU MUST USE IT!**
"""

            system_prompt = f"""You are an AI assistant for the Flexa airport simulation system.

**🌐 LANGUAGE RULES (HIGHEST PRIORITY - MUST FOLLOW):**
⚠️ CRITICAL: You MUST respond in the SAME language as the user's question!

- Korean question (한글) → Korean answer (한글로 답변)
- English question → English answer
- Any other language → Same language answer

**Examples:**
- User: "이제 뭐해야 해?" → Answer in Korean: "항공편 선택이 완료되었습니다. 다음은..."
- User: "What should I do next?" → Answer in English: "Flights are selected. Next step is..."

⚠️ NEVER respond in English when the user asks in Korean!
⚠️ Match the user's language EXACTLY!
{simulation_status}
Current scenario information (from S3):
- Scenario ID: {scenario_id}
- Process count: {context.get('process_count', 0)}
- Current processes: {', '.join(context.get('process_names', [])) or 'None'}

Available commands:
1. Add process: "add checkin process", "보안검색 단계 추가"
2. Remove process: "remove checkin process", "보안검색 단계 제거"
3. List processes: "show process list", "프로세스 목록 보여줘"
4. List files: "list files", "무슨 파일 있는지 확인해"
5. Read/analyze file: "analyze simulation-pax.parquet", "대기시간 결과 파일 보여줘"

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
            # Passenger/Process 데이터가 있으면 user message에 컨텍스트 추가
            user_message_content = user_content
            context_hints = []

            if simulation_state:
                # Passenger 데이터 힌트
                if simulation_state.get('passenger', {}).get('total', 0) > 0:
                    passenger_data = simulation_state.get('passenger', {})
                    context_hints.append(f"Passenger data: {passenger_data.get('total', 0)} passengers with full details (chartResult, demographics, etc.)")

                # Process flow 데이터 힌트
                if simulation_state.get('process_flow'):
                    process_flow = simulation_state.get('process_flow', [])
                    process_count = len(process_flow)
                    process_names = [p.get('name', '') for p in process_flow]
                    context_hints.append(f"Process flow data: {process_count} processes ({', '.join(process_names)}) with zones, facilities, time_blocks, entry_conditions")

            if context_hints:
                context_str = "\n".join([f"- {hint}" for hint in context_hints])
                user_message_content = f"""{user_content}

[CONTEXT: Real-time simulation data available:
{context_str}
Use simulation_state to answer process-related questions.]"""

            messages.append(Message(role="user", content=user_message_content))
            
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
        simulation_state: dict = None,
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
            simulation_state: 현재 시뮬레이션 상태 (Zustand store에서 추출)
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

            # 🆕 현재 시뮬레이션 상태 정보 추가
            simulation_status = ""
            if simulation_state:
                simulation_status = f"""

**CURRENT SIMULATION STATE (Real-time from browser):**
- Airport: {simulation_state.get('airport', 'Not set')}
- Date: {simulation_state.get('date', 'Not set')}
- Flights configured: {simulation_state.get('flight_count', 0)} flights
- Passengers configured: {'Yes' if simulation_state.get('passenger_configured') else 'No'}
- Process flow: {simulation_state.get('process_count', 0)} processes ({', '.join(simulation_state.get('process_names', [])) or 'None'})
- Workflow status:
  * Flights tab completed: {'Yes' if simulation_state.get('workflow', {}).get('flights_completed') else 'No'}
  * Passengers tab completed: {'Yes' if simulation_state.get('workflow', {}).get('passengers_completed') else 'No'}

**IMPORTANT:** This is the current state in the user's browser. The file data below might be outdated if the user hasn't saved recently.
"""

            system_prompt = f"""You are a data analyst for the Flexa airport simulation system. Explain things in a user-friendly and specific way.

**🌐 LANGUAGE RULES (HIGHEST PRIORITY - MUST FOLLOW):**
⚠️ CRITICAL: You MUST respond in the SAME language as the user's question!

- Korean question (한글) → Korean answer (한글로 답변)
- English question → English answer
- Any other language → Same language answer

**Examples:**
- User: "승객 몇 명이야?" → Answer in Korean: "총 3,731명의 승객이 생성되었습니다..."
- User: "How many passengers?" → Answer in English: "A total of 3,731 passengers were generated..."

⚠️ NEVER respond in English when the user asks in Korean!
⚠️ Match the user's language EXACTLY!
{simulation_status}
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

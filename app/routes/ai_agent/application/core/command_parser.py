"""
명령 파싱 서비스 - Function Calling을 사용하여 사용자 명령을 파싱
"""
import json
import math
import os
import re
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
        model: str = "gpt-4o-mini",
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

                # Passenger 데이터 추출 (None 안전 처리)
                passenger_data = simulation_state.get('passenger') or {}
                passenger_total = passenger_data.get('total', 0)
                pax_gen = passenger_data.get('pax_generation') or {}
                pax_demo = passenger_data.get('pax_demographics') or {}
                pax_arrival = passenger_data.get('pax_arrival_patterns') or {}
                chart_result = passenger_data.get('chartResult') or {}

                # 탑승률 요약 (default + rules)
                load_factor = (pax_gen.get('default') or {}).get('load_factor', 'Not set')
                load_factor_str = f"{load_factor}%"
                load_factor_rules = pax_gen.get('rules') or []
                if load_factor_rules:
                    load_factor_str += " (default)"
                    for rule in load_factor_rules:
                        rule_lf = rule.get('load_factor', '')
                        rule_conditions = rule.get('conditions', {})
                        cond_parts = []
                        for field, values in rule_conditions.items():
                            if isinstance(values, list):
                                cond_parts.append(f"{field}={','.join(str(v) for v in values)}")
                            else:
                                cond_parts.append(f"{field}={values}")
                        cond_str = ', '.join(cond_parts) if cond_parts else 'unknown condition'
                        load_factor_str += f" / When {cond_str} → {rule_lf}%"

                # 국적 요약 (default + rules)
                nationality_data_raw = pax_demo.get('nationality') or {}
                nationality_default = nationality_data_raw.get('default') or {}
                nationality_values = {k: v for k, v in nationality_default.items() if k != 'flightCount'}
                nationality_str = ', '.join([f"{k}: {v}%" for k, v in nationality_values.items()]) if nationality_values else 'Not configured'
                nationality_rules = nationality_data_raw.get('rules') or []
                if nationality_rules:
                    nationality_str += f" (default)"
                    for rule in nationality_rules:
                        rule_values = {k: v for k, v in rule.items() if k not in ('conditions', 'flightCount')}
                        rule_conditions = rule.get('conditions', {})
                        cond_parts = []
                        for field, values in rule_conditions.items():
                            if isinstance(values, list):
                                cond_parts.append(f"{field}={','.join(str(v) for v in values)}")
                            else:
                                cond_parts.append(f"{field}={values}")
                        cond_str = ', '.join(cond_parts) if cond_parts else 'unknown condition'
                        dist_str = ', '.join([f"{k}: {v}%" for k, v in rule_values.items()])
                        nationality_str += f" / When {cond_str} → {dist_str}"

                # 프로필 요약 (default + rules)
                profile_data_raw = pax_demo.get('profile') or {}
                profile_default = profile_data_raw.get('default') or {}
                profile_values = {k: v for k, v in profile_default.items() if k != 'flightCount'}
                profile_str = ', '.join([f"{k}: {v}%" for k, v in profile_values.items()]) if profile_values else 'Not configured'
                profile_rules = profile_data_raw.get('rules') or []
                if profile_rules:
                    profile_str += f" (default)"
                    for rule in profile_rules:
                        rule_values = {k: v for k, v in rule.items() if k not in ('conditions', 'flightCount')}
                        rule_conditions = rule.get('conditions', {})
                        cond_parts = []
                        for field, values in rule_conditions.items():
                            if isinstance(values, list):
                                cond_parts.append(f"{field}={','.join(str(v) for v in values)}")
                            else:
                                cond_parts.append(f"{field}={values}")
                        cond_str = ', '.join(cond_parts) if cond_parts else 'unknown condition'
                        dist_str = ', '.join([f"{k}: {v}%" for k, v in rule_values.items()])
                        profile_str += f" / When {cond_str} → {dist_str}"

                # 도착 패턴 요약 (default + rules)
                arrival_mean = (pax_arrival.get('default') or {}).get('mean', 'Not set')
                arrival_std = (pax_arrival.get('default') or {}).get('std', 'Not set')
                arrival_str = f"Mean {arrival_mean} min before departure (std: {arrival_std})"
                arrival_rules = pax_arrival.get('rules') or []
                if arrival_rules:
                    arrival_str += " (default)"
                    for rule in arrival_rules:
                        rule_mean = rule.get('mean', '')
                        rule_std = rule.get('std', '')
                        rule_conditions = rule.get('conditions', {})
                        cond_parts = []
                        for field, values in rule_conditions.items():
                            if isinstance(values, list):
                                cond_parts.append(f"{field}={','.join(str(v) for v in values)}")
                            else:
                                cond_parts.append(f"{field}={values}")
                        cond_str = ', '.join(cond_parts) if cond_parts else 'unknown condition'
                        arrival_str += f" / When {cond_str} → mean {rule_mean} min (std: {rule_std})"

                # 실제 데이터를 JSON으로 직렬화 (하드코딩 예시 대신 사용)
                pax_gen_json = json.dumps(pax_gen, ensure_ascii=False, indent=2) if pax_gen else '{}'
                nationality_data = (pax_demo.get('nationality') or {})
                nationality_json = json.dumps(nationality_data, ensure_ascii=False, indent=2) if nationality_data else '{}'
                profile_data = (pax_demo.get('profile') or {})
                profile_json = json.dumps(profile_data, ensure_ascii=False, indent=2) if profile_data else '{}'
                pax_arrival_json = json.dumps(pax_arrival, ensure_ascii=False, indent=2) if pax_arrival else '{}'
                chart_result_summary = (chart_result.get('summary') or {})
                chart_result_summary_json = json.dumps(chart_result_summary, ensure_ascii=False, indent=2) if chart_result_summary else '{}'
                chart_result_json = json.dumps(chart_result, ensure_ascii=False, indent=2) if chart_result else '{}'

                # 🆕 프로세스/시설 요약 생성 (실제 데이터에서 추출)
                process_flow = simulation_state.get('process_flow') or []
                process_summary_lines = []
                total_facilities = 0
                for proc in process_flow:
                    proc_name = proc.get('name', 'Unknown')
                    travel_time = proc.get('travel_time_minutes', 0)
                    process_time = proc.get('process_time_seconds', 'Not set')
                    entry_conditions = proc.get('entry_conditions', [])
                    zones = proc.get('zones', {})
                    zone_count = len(zones)
                    zone_names = list(zones.keys())[:5]
                    zone_str = ', '.join(zone_names) if zone_names else 'No zones'
                    if len(zones) > 5:
                        zone_str += f' +{len(zones) - 5} more'

                    # Extract operating hours, passenger_conditions, and count active/closed facilities
                    operating_hours = set()
                    all_passenger_conditions = []
                    active_facility_count = 0
                    closed_facility_count = 0
                    closed_facility_ids = []

                    for zone_data in zones.values():
                        for facility in zone_data.get('facilities', []):
                            facility_id = facility.get('id', 'Unknown')
                            # Check if facility has any active time_block
                            has_active_block = False
                            for block in (facility.get('operating_schedule') or {}).get('time_blocks', []):
                                if block.get('activate', True):  # Default is True (operating)
                                    has_active_block = True
                                    period = block.get('period', '')
                                    if period:
                                        operating_hours.add(period)
                                    # Collect passenger_conditions
                                    pax_conds = block.get('passenger_conditions', [])
                                    if pax_conds:
                                        all_passenger_conditions.extend(pax_conds)

                            if has_active_block:
                                active_facility_count += 1
                            else:
                                closed_facility_count += 1
                                closed_facility_ids.append(facility_id)

                    total_facilities += active_facility_count

                    # Format operating hours (period format: "2026-03-01 05:00:00-2026-03-01 06:00:00")
                    if operating_hours:
                        # Get min start and max end from all periods
                        all_starts = []
                        all_ends = []
                        for period in operating_hours:
                            if '-' in period:
                                parts = period.split('-')
                                # Format: YYYY-MM-DD HH:MM:SS-YYYY-MM-DD HH:MM:SS (6 parts when split by '-')
                                if len(parts) >= 6:
                                    start_datetime = f"{parts[0]}-{parts[1]}-{parts[2]}"
                                    end_datetime = f"{parts[3]}-{parts[4]}-{parts[5]}"
                                    start_time = start_datetime.split(' ')[1][:5] if ' ' in start_datetime else ''
                                    end_time = end_datetime.split(' ')[1][:5] if ' ' in end_datetime else ''
                                    if start_time:
                                        all_starts.append(start_time)
                                    if end_time:
                                        all_ends.append(end_time)
                        if all_starts and all_ends:
                            hours_str = f"{min(all_starts)} ~ {max(all_ends)}"
                        else:
                            hours_str = 'All day (no time restrictions)'
                    else:
                        hours_str = 'All day (no time restrictions)'

                    # Format entry_conditions
                    if entry_conditions:
                        entry_str = ', '.join([f"{c.get('field')}={c.get('values')}" for c in entry_conditions])
                    else:
                        entry_str = 'All passengers (no restrictions)'

                    # Format passenger_conditions
                    if all_passenger_conditions:
                        unique_conds = {f"{c.get('field')}={c.get('values')}" for c in all_passenger_conditions}
                        pax_cond_str = ', '.join(unique_conds)
                    else:
                        pax_cond_str = 'All passengers (no restrictions)'

                    # Format facility status line
                    if closed_facility_count > 0:
                        closed_ids_str = ', '.join(closed_facility_ids[:5])
                        if closed_facility_count > 5:
                            closed_ids_str += f' +{closed_facility_count - 5} more'
                        facility_status = f"{active_facility_count} active, {closed_facility_count} closed ({closed_ids_str})"
                    else:
                        facility_status = f"{active_facility_count} active"

                    # 🆕 Helper function to translate field names to human-readable descriptions
                    airlines_mapping = simulation_state.get('airlines_mapping', {})

                    def translate_condition(field, values):
                        """Translate field/values to human-readable description"""
                        field_translations = {
                            'arrival_airport_iata': 'Destination',
                            'departure_airport_iata': 'Origin',
                            'operating_carrier_iata': 'Airline',
                            'flight_type': 'Flight type',
                            'nationality': 'Nationality',
                            'profile': 'Passenger type',
                            'terminal': 'Terminal',
                            'flight_number': 'Flight number',
                        }
                        field_name = field_translations.get(field, field)

                        # Format values nicely
                        if isinstance(values, list):
                            if field == 'operating_carrier_iata':
                                values_str = ', '.join(f"{airlines_mapping.get(v, v)} ({v})" for v in values)
                            else:
                                values_str = ', '.join(str(v) for v in values)
                        else:
                            if field == 'operating_carrier_iata':
                                values_str = f"{airlines_mapping.get(values, values)} ({values})"
                            else:
                                values_str = str(values)

                        return f"{field_name}: {values_str}"

                    # 🆕 Generate per-facility details with time-block breakdown
                    facility_details_lines = []
                    for zone_name, zone_data in zones.items():
                        for facility in zone_data.get('facilities', []):
                            fac_id = facility.get('id', 'Unknown')
                            time_blocks = (facility.get('operating_schedule') or {}).get('time_blocks', [])

                            # Collect detailed info per time block
                            block_details = []
                            has_any_active = False

                            for block in time_blocks:
                                activate = block.get('activate', True)
                                proc_time_block = block.get('process_time_seconds', process_time)
                                period = block.get('period', '')
                                pax_conds = block.get('passenger_conditions', [])

                                # Extract time range (format: "2026-03-01 05:00:00-2026-03-01 06:00:00")
                                time_range = ''
                                if period:
                                    try:
                                        # Split by the datetime separator (find the middle '-' that separates two datetimes)
                                        # Format: YYYY-MM-DD HH:MM:SS-YYYY-MM-DD HH:MM:SS
                                        parts = period.split('-')
                                        # Reconstruct: first 3 parts are start date, rest are end datetime
                                        if len(parts) >= 6:
                                            start_datetime = f"{parts[0]}-{parts[1]}-{parts[2]}"  # YYYY-MM-DD HH:MM:SS
                                            end_datetime = f"{parts[3]}-{parts[4]}-{parts[5]}"    # YYYY-MM-DD HH:MM:SS
                                            start_t = start_datetime.split(' ')[1][:5] if ' ' in start_datetime else ''
                                            end_t = end_datetime.split(' ')[1][:5] if ' ' in end_datetime else ''
                                            time_range = f"{start_t}~{end_t}"
                                    except:
                                        time_range = 'unknown'

                                # Translate passenger conditions
                                if pax_conds:
                                    conds_translated = [translate_condition(c.get('field'), c.get('values')) for c in pax_conds]
                                    conds_str = ' AND '.join(conds_translated)
                                else:
                                    conds_str = 'All passengers'

                                status_str = "OPEN" if activate else "CLOSED"
                                if activate:
                                    has_any_active = True

                                block_details.append({
                                    'time': time_range,
                                    'status': status_str,
                                    'process_time': proc_time_block,
                                    'conditions': conds_str,
                                    'activate': activate
                                })

                            # Format facility output
                            fac_status = "CLOSED (all time blocks)" if not has_any_active else "ACTIVE"

                            # Build detailed time block info
                            block_lines = []
                            for bd in block_details:
                                if bd['activate']:
                                    block_lines.append(f"          [{bd['time']}] {bd['process_time']}s, {bd['conditions']}")
                                else:
                                    block_lines.append(f"          [{bd['time']}] CLOSED")

                            block_info = '\n'.join(block_lines) if block_lines else '          (no time blocks)'

                            facility_details_lines.append(
                                f"      - {fac_id} [{fac_status}]:\n{block_info}"
                            )

                    facility_details = '\n'.join(facility_details_lines) if facility_details_lines else '      (No facilities)'

                    process_summary_lines.append(
                        f"  - {proc_name}: {zone_count} zone(s), {facility_status}\n"
                        f"    * Zones: {zone_str}\n"
                        f"    * Travel time: {travel_time} min (time for passengers to reach this facility)\n"
                        f"    * Process time (default): {process_time} sec\n"
                        f"    * Operating hours: {hours_str}\n"
                        f"    * Entry conditions: {entry_str}\n"
                        f"    * Facility Details (per-facility conditions and status):\n{facility_details}"
                    )

                process_summary = '\n'.join(process_summary_lines) if process_summary_lines else '  (No processes configured)'

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
- Load factor: {load_factor_str}
- Nationality: {nationality_str}
- Profile: {profile_str}
- Arrival pattern: {arrival_str}

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

**1. pax_generation** - Passenger count generation (Load Factor / Boarding Rate)
Current configuration:
```
{pax_gen_json}
```
- **Purpose**: Determines how many passengers per flight
- **load_factor**: Boarding rate (탑승률). Percentage of seats filled. e.g., 83 means 83 passengers for a 100-seat flight
- **default**: Base boarding rate for all flights
- **rules**: Override boarding rate for specific conditions (by airline, flight type, etc.)
- **Result**: Each flight generates N passengers based on (seats × load_factor / 100)

**💡 WHY CONFIGURE LOAD FACTOR?** (When users ask "왜 설정해?", "Why configure?")
→ "Load Factor determines how many passengers to generate per flight.
   You can set different boarding rates per flight condition like real airports."

**2. pax_demographics** - Passenger attributes distribution

**2a. nationality** - Nationality distribution
Current configuration:
```
{nationality_json}
```
- **Purpose**: Assigns nationality to each passenger
- **available_values**: Possible nationality options
- **default**: Base distribution percentages (must sum to 100)
- **rules**: Override for specific conditions (by flight type, airline, etc.)
- **Result**: Each passenger gets a `nationality` field
- **Simulation use**: Checked in entry_conditions, passenger_conditions (e.g., immigration process only for foreign passengers)

**💡 WHY CONFIGURE NATIONALITY?** (When users ask "nationality 왜 설정해?")
→ "Nationality is used when domestic and foreign passengers need to go through different processes.
   For example, entry_conditions can route only foreign passengers through immigration.
   The system checks each passenger's nationality field to determine which processes they must go through."

**2b. profile** - Passenger profile/type distribution (Pax Profile)
Current configuration:
```
{profile_json}
```
- **Purpose**: Assigns passenger type/category based on their characteristics
- **available_values**: Possible profile options
- **default**: Base distribution percentages (must sum to 100)
- **rules**: Override for specific conditions (by flight type, airline, destination, etc.)
- **Result**: Each passenger gets a `profile` field
- **Simulation use**: Checked in entry_conditions, passenger_conditions (determines which processes to go through and which facilities to use)

**💡 WHY CONFIGURE PAX PROFILE?** (When users ask "Pax Profile이 뭐야?", "profile 왜 설정해?")
→ "Pax Profile represents passenger characteristics that determine which facilities they use and which processes they go through.
   In real airports, different passengers use different facilities based on seat class, passenger type, mobility needs, etc.
   Set entry_conditions and passenger_conditions to route different profiles to different facilities or set different processing times."

**3. pax_arrival_patterns** - Airport arrival timing
Current configuration:
```
{pax_arrival_json}
```
- **Purpose**: Determines when passengers arrive at airport (before flight departure)
- **mean**: Average arrival time in minutes before departure
- **std**: Standard deviation (time variance)
- **default**: Base arrival pattern for all flights
- **rules**: Override for specific conditions (by flight type, airline, etc.)
- **Result**: Each passenger gets a `show_up_time` field
- **Simulation use**: Starting point of simulation (passenger arrival event)

**💡 WHY CONFIGURE SHOW-UP-TIME?** (When users ask "show-up-time 왜 설정해?")
→ "Show-up-Time determines when passengers arrive at the airport.
   mean is the average minutes before departure, std is variance.
   This becomes the starting point of simulation. You can set different values per flight type or airline."

**4. chartResult** - Generated passenger data summary
Current data:
```
{chart_result_json}
```
- **Purpose**: Summary of generated passengers (NOT used in simulation, just reporting)
- **total**: Total number of passengers created
- **chart_x_data**: Time slots (hourly)
- **chart_y_data**: Breakdown by category over time (airline, nationality, profile)
- **summary**: Statistics (flights count, avg_seats, load_factor, min_arrival_minutes)

**How Passenger Fields Are Used in Simulation:**

**Available Passenger Fields for Conditions:**
| Field | Source | Description |
|-------|--------|-------------|
| `nationality` | pax_demographics | Passenger nationality (values from nationality config above) |
| `profile` | pax_demographics | Passenger profile/type (values from profile config above) |
| `operating_carrier_iata` | Flight data | Airline IATA code from flight schedule |
| `flight_type` | Flight data | "Domestic" or "International" |
| `show_up_time` | pax_arrival_patterns | Passenger arrival timestamp |
| `flight_number` | Flight data | Flight number from schedule |
| `terminal` | Flight data | Terminal from flight schedule |
| `destination` | Flight data | Destination from flight schedule |

**Condition Matching Logic (from simulator):**
```
If conditions is EMPTY [] → ALL passengers match (open to everyone)
If conditions has values → Check each condition:
  - field: Which passenger attribute to check
  - values: List of allowed values
  - ALL conditions must match (AND logic)
```

**Example Conditions:**
```
entry_conditions: [{{"field": "nationality", "values": ["Foreign"]}}]
→ Only passengers with nationality="Foreign" go through this process
→ Domestic passengers get status="skipped"

passenger_conditions: [{{"field": "profile", "values": ["Business", "First"]}}]
→ Only Business or First class passengers can use this facility
→ Economy passengers must use other facilities

Multiple conditions (AND logic):
passenger_conditions: [
  {{"field": "operating_carrier_iata", "values": ["AA"]}},
  {{"field": "profile", "values": ["Business"]}}
]
→ Only AA airline AND Business class passengers can use this facility
```

**💡 WHAT FIELDS CAN BE USED IN CONDITIONS?** (When users ask "어떤 필드 쓸 수 있어?")
→ "You can use any passenger attribute in entry_conditions or passenger_conditions:
   - **nationality**: Values from the nationality configuration above
   - **profile**: Values from the profile configuration above
   - **operating_carrier_iata**: Airline IATA codes from the flight schedule
   - **flight_type**: Domestic or International (from flight data)
   - And other flight attributes: terminal, destination, flight_number, etc.
   Refer to the actual configuration data above for the specific values available in this simulation."

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
- Total: {simulation_state.get('process_count', 0)} process(es), {total_facilities} facility(ies)
- Details:
{process_summary}

**Process Flow (Full Data Available):**
You have access to detailed process data in simulation_state['process_flow']:

**Structure:**
```
process_flow: [
  {{
    "step": 0,
    "name": "check_in",
    "travel_time_minutes": 5,
    "process_time_seconds": 100,
    "entry_conditions": [],
    "zones": {{
      "A": {{
        "facilities": [{{
          "id": "A_01",
          "operating_schedule": {{
            "time_blocks": [{{
              "period": "<START_DATETIME>-<END_DATETIME>",  // e.g., "2026-03-01 05:00:00-2026-03-02 00:00:00"
              "process_time_seconds": 100,
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
⚠️ **IMPORTANT**: The actual values are in the Process Flow Summary above. Use those REAL values, not this example structure!

**Key Field Meanings:**

1. **step**: Process order (0, 1, 2, ...)

2. **name**: Process name (check_in, security, passport, immigration, boarding, etc.)

**💡 WHY ADD PROCESS?** (When users ask "프로세스 왜 추가해?", "Why add process?")
→ "Process represents an airport procedure that passengers must go through.
   For example, if you add check_in → security → boarding in this order,
   passengers will go through processes in this sequence during simulation.
   You need at least 1 process to enable the 'Run Simulation' button."

3. **travel_time_minutes**: Time for a passenger to reach this facility
   - For the FIRST process: Time to walk from airport entrance to this facility after arrival
   - For SUBSEQUENT processes: Time to walk from the previous process to this facility
   - Example: 5 minutes to walk from check_in to security

**💡 WHY CONFIGURE TRAVEL TIME?** (When users ask "travel time이 뭐야?", "이동 시간이 뭐야?")
→ "Travel Time is the time it takes for a passenger to reach this facility.

   For example:
   - First process (check-in) travel time = 5 min: Time to walk from airport entrance to check-in counter
   - Second process (security) travel time = 3 min: Time to walk from check-in to security checkpoint

   In simulation: Previous process completion time + Travel time = Arrival time at next process"

4. **process_time_seconds**: Time for ONE passenger to complete this process
   - ⚠️ IMPORTANT: The value inside time_block is actually used in simulation
   - Process-level value is the default for UI display

**💡 WHY CONFIGURE PROCESS TIME?** (When users ask "처리 시간이 뭐야?", "process time이 뭐야?")
→ "Process Time is the time it takes for ONE passenger to complete this process.

   For example:
   - Check-in process time = 180 seconds: Each passenger takes 3 minutes to check in
   - Security process time = 120 seconds: Each passenger takes 2 minutes to pass security

   You can set different processing times by time period:
   - Peak hours (08:00-10:00): 180 seconds (slower due to congestion)
   - Off-peak hours (14:00-16:00): 120 seconds (faster)

   In simulation: Start time + Process time = Completion time"

5. **entry_conditions**: Who must go through this process
   - **If EMPTY [] or not set → ALL passengers go through this process (open to everyone)**
   - Example: {{"field": "nationality", "values": ["Foreign"]}} → Only foreign passengers
   - Example: {{"field": "flight_type", "values": ["International"]}} → Only international flights
   - If matched → process proceeds
   - If not matched → status = "skipped"

**💡 WHY CONFIGURE ENTRY CONDITIONS?** (When users ask "entry_conditions 왜 설정해?")
→ "Entry Conditions determine which passengers must go through this process.

   **If empty or not set: ALL passengers go through (open to everyone).**
   This is the default - having no conditions means the facility is available to all passengers.

   If you set conditions, only matching passengers will proceed:
   For example, if you set entry_conditions as nationality='Foreign' for the 'immigration' process,
   only foreign passengers will go through this process, and domestic passengers will skip it (status=skipped).
   You can use fields like nationality, profile, etc. that were configured in the Passenger tab."

6. **zones**: Physical/logical area groups
   - Example: "EAST KIOSK1", "WEST MANNED", "PRIORITY", "REGULAR"
   - Each zone contains multiple facilities

**💡 WHY CONFIGURE ZONES?** (When users ask "zone이 뭐야?")
→ "Zone is a logical/physical grouping of facilities.
   For example, if you divide check-in counters into 'PRIORITY zone' and 'REGULAR zone',
   you can later configure Fast track passengers to use only PRIORITY zone facilities.
   You can manage facilities by zone like real airports."

7. **facilities**: Actual service counters/machines
   - Example: "EAST KIOSK1_1", "EAST KIOSK1_2" → Kiosk machine numbers
   - Each has operating_schedule with time_blocks

**💡 WHY CONFIGURE FACILITIES?** (When users ask "시설 왜 추가해?")
→ "Facility is the actual service location. Check-in counters, security checkpoints, kiosks, etc. are facilities.
   For example, you can distinguish them by kiosk machine numbers like 'EAST KIOSK1_1', 'EAST KIOSK1_2',
   or by counter numbers like 'Counter A', 'Counter B'.
   During simulation, passengers automatically select the fastest available facility (considering queues)."

8. **time_blocks**: Time-specific operating policies
   - Why needed? Different policies by time:
     * Peak hours → longer processing time
     * Specific hours → specific airlines only
     * Lunch break → facility closed
   - Each block has:
     * **period**: Operating time range (check actual values in Process Flow Summary above)
     * **process_time_seconds**: Processing time for THIS time period
     * **passenger_conditions**: Who can use this facility at this time
     * **activate**: true = operating, false = closed (excluded from simulation)

**💡 WHY CONFIGURE TIME BLOCKS?** (When users ask "time blocks 왜 필요해?")
→ "Time Blocks allow you to set different operating policies by time period.
   For example:
   - Peak hours (08:00-10:00): process_time 180 seconds → congested
   - Off-peak hours (14:00-16:00): process_time 120 seconds → fast
   - Lunch break (12:00-13:00): activate=false → facility closed
   - Specific hours (09:00-12:00): G3 airline only (passenger_conditions)
   During simulation, blocks with activate=false are excluded, and only facilities matching current time (period) and conditions are used."

9. **passenger_conditions** (facility level):
   - Who can use THIS facility at THIS time
   - **If EMPTY [] or not set → ALL passengers can use this facility (open to everyone)**
   - Example: {{"field": "operating_carrier_iata", "values": ["G3"]}} → G3 airline only
   - Example: {{"field": "profile", "values": ["Fast track"]}} → Fast track passengers only
   - **Difference from entry_conditions:**
     * entry_conditions: Process-wide access (process level)
     * passenger_conditions: Facility-specific access (time_block level)

**💡 WHY CONFIGURE PASSENGER CONDITIONS?** (When users ask "passenger_conditions 차이가 뭐야?")
→ "Passenger Conditions determine who can use a specific facility.

   **If empty or not set: ALL passengers can use this facility (open to everyone).**
   This is the default - having no conditions means the facility is available to all passengers.

   **Entry Conditions vs Passenger Conditions difference:**
   - Entry Conditions (process level): Who must go through this process?
     Example) immigration process is only for foreign passengers
   - Passenger Conditions (facility level): Who can use this facility?
     Example) PRIORITY zone counters are only for Fast track passengers

   Example scenario:
   - Check-in process: All passengers go through (no entry_conditions)
   - But PRIORITY zone facilities: Fast track only (passenger_conditions)
   - REGULAR zone facilities: Regular passengers only (passenger_conditions)

   During simulation, passengers select the fastest available facility among those matching their passenger_conditions."

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

**Simulation Output Columns (Generated per process):**
For each process (e.g., check_in, security), the simulation generates these columns per passenger:

| Column | Meaning | Example Value |
|--------|---------|---------------|
| `{{process}}_on_pred` | Predicted arrival time at facility | 2024-01-01 08:05:00 |
| `{{process}}_facility` | Assigned facility ID | A_01 |
| `{{process}}_zone` | Assigned zone name | PRIORITY |
| `{{process}}_start_time` | When service actually starts | 2024-01-01 08:10:00 |
| `{{process}}_done_time` | When service is completed | 2024-01-01 08:13:00 |
| `{{process}}_open_wait_time` | Time waiting for facility to open | 00:00:00 (if already open) |
| `{{process}}_queue_wait_time` | Time waiting in queue | 00:05:00 (5 min queue) |
| `{{process}}_queue_length` | Number of people ahead in queue at arrival | 3 |
| `{{process}}_status` | Result of this process | completed/failed/skipped |

**Status Values:**
- **completed**: Successfully processed at a facility
- **failed**: Could not be assigned (no available facility, conditions not met)
- **skipped**: Did not need to go through (entry_conditions not matched)

**💡 WHY ARE THERE TWO WAIT TIMES?** (When users ask "대기 시간 왜 두 개야?")
→ "There are two types of waiting time:
   1. **open_wait_time**: If a passenger arrives before the facility opens (e.g., arrives at 07:50 but facility opens at 08:00), they wait for it to open = 10 min open_wait
   2. **queue_wait_time**: Once the facility is open, if there are other passengers ahead, they wait in queue = queue_wait
   Total wait = open_wait + queue_wait"

**Example Questions You Can Answer:**
- "check_in 시설 통과하는 데 얼마나 걸려?" → Look at time_blocks[].process_time_seconds
- "security까지 이동하는 데 시간 얼마나?" → travel_time_minutes
- "외국인만 거치는 프로세스 뭐야?" → Check entry_conditions
- "G3 항공사 승객은 어느 시설 사용해?" → Check passenger_conditions
- "What facilities operate during morning hours?" → Check period and activate=true in Process Flow Summary
- "시뮬레이션 결과 어떻게 해석해?" → See Simulation Output Columns above
- "대기 시간이 왜 두 가지야?" → open_wait_time vs queue_wait_time explained above

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
1. ✅ Nationality - Define nationality types (e.g., Domestic, Foreign) and distribution %
   Example: {{"Domestic": 60, "Foreign": 40}}
2. ✅ Pax Profile - Define passenger types based on characteristics (seat class, wheelchair users, crew, etc.)
   Example: {{"Economy": 70, "Business": 20, "First": 5, "Wheelchair": 3, "Crew": 2}}
   → Different profiles use different facilities and may have different processing times
3. ❌ Load Factor - Click to set default boarding rate (e.g., 85%)
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

**📦 HOW TO ANSWER FACILITY/PROCESS QUESTIONS:**

When users ask about facilities or processes (e.g., "시설 어떻게 설정되어있어?", "프로세스 확인해줘", "현재 설정 알려줘"):

⚠️ **CRITICAL: Use ONLY the ACTUAL data from "Process Flow (Summary)" section above!**
- DO NOT use example values from the "Structure" section
- DO NOT make up or guess any values
- All real data (travel time, process time, operating hours, zones, facilities) is in the Summary

**ALWAYS provide a friendly, detailed summary using the REAL values from Process Flow Summary:**

Example answer format (fill in with ACTUAL values from Summary):
```
Here's the current facility configuration! 😊

📋 **Process Overview**: [ACTUAL process count] process(es) configured.

**1. [ACTUAL process name]**
- Travel Time: [ACTUAL travel_time from Summary] min
  → Time for passengers to reach this facility (from airport entrance or previous process)
- Process Time: [ACTUAL process_time from Summary] sec
  → Time for ONE passenger to complete this process
- Zone: [ACTUAL zone names from Summary]
- Facilities: [ACTUAL facility count from Summary]
- Operating Hours: [ACTUAL operating hours from Summary]
- Entry Conditions: [ACTUAL entry_conditions from Summary]
  → If "All passengers (no restrictions)": Everyone goes through this process
- Passenger Conditions: [ACTUAL passenger_conditions from Summary]
  → If "All passengers (no restrictions)": All passengers can use these facilities

💡 [Helpful context using ACTUAL passenger count]
```

**IMPORTANT - Understanding "No Restrictions":**
- Empty conditions [] = "All passengers (no restrictions)" = OPEN TO EVERYONE
- This is the DEFAULT behavior - having no conditions means the facility/process is available to all
- Only when conditions are SET do you need to specify who can use it

**Key information to include (ALL from Process Flow Summary):**
1. Total number of processes (from Summary)
2. For each process:
   - Process name (from Summary)
   - travel_time_minutes (from Summary): Time for passengers to reach this facility
   - process_time_seconds (from Summary): Time for ONE passenger to complete this process
   - Zone names and count (from Summary)
   - Facility count per zone (from Summary)
   - Operating hours (from Summary - extracted from time_blocks)
   - Entry conditions (if any)
   - Passenger conditions (if any)
3. Helpful context about what this means for simulation
4. **Always explain what travel_time and process_time mean in simple terms**

**If NO processes configured:**
→ "No processes have been configured yet. Please click the 'Add Process' button in the Facilities tab, or tell me 'add check-in process' and I'll help you set it up!"

**CRITICAL ANSWERING RULES:**
⚠️ **ALL DATA IS ALREADY IN SIMULATION_STATE - USE IT DIRECTLY!**

✅ DO:
1. Use simulation_state['passenger'] for ALL passenger questions
2. Use simulation_state['process_flow'] for ALL facility/process questions
3. **Use ONLY the values shown in "Process Flow (Summary)" section** - these are the REAL values!
4. If passenger.total > 0, data IS configured and available
5. If process_count > 0, processes ARE configured - describe them using Summary values!
6. Use chartResult.chart_y_data for time-based questions
7. Use full airline names from airlines_mapping
8. Be specific with numbers from the data
9. Use "chat" action to answer questions directly (don't call list_processes function for simple queries)
10. Be friendly and helpful - use emojis sparingly to make responses engaging
11. Convert technical values to human-readable format (e.g., 180 seconds → 3 min)

❌ DON'T:
1. NEVER use read_file for configuration data (passenger, process, facility)
2. NEVER say "not configured" or "no information available" if data exists in simulation_state
3. NEVER mention S3, JSON files, or "saved data"
4. NEVER ignore simulation_state data
5. NEVER give vague answers when specific data is available
6. **NEVER use example values from the "Structure" section - use ONLY "Process Flow (Summary)" values!**
7. **NEVER make up operating hours, travel times, or process times - read them from Summary!**

**If data exists in simulation_state, YOU MUST USE IT to give detailed, helpful answers!**
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
                if (simulation_state.get('passenger') or {}).get('total', 0) > 0:
                    passenger_data = simulation_state.get('passenger') or {}
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

                        try:
                            error_data = json.loads(error_text)
                            if error_data.get("error", {}).get("code") == "rate_limit_exceeded":
                                retry_seconds = 5
                                match = re.search(r"Please try again in ([\d.]+)s", error_data.get("error", {}).get("message", ""))
                                if match:
                                    retry_seconds = math.ceil(float(match.group(1)))
                                return {
                                    "action": "chat",
                                    "content": f"잠깐만요, 현재 토큰 제한으로 잠시 대기 중입니다. 약 {retry_seconds}초 후에 다시 질문해 주세요.",
                                    "model": None,
                                    "usage": {},
                                }
                        except (json.JSONDecodeError, KeyError):
                            pass

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
        model: str = "gpt-4o-mini",
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

                        try:
                            error_data = json.loads(error_text)
                            if error_data.get("error", {}).get("code") == "rate_limit_exceeded":
                                retry_seconds = 5
                                match = re.search(r"Please try again in ([\d.]+)s", error_data.get("error", {}).get("message", ""))
                                if match:
                                    retry_seconds = math.ceil(float(match.group(1)))
                                return {
                                    "success": True,
                                    "content": f"잠깐만요, 현재 토큰 제한으로 잠시 대기 중입니다. 약 {retry_seconds}초 후에 다시 질문해 주세요.",
                                    "model": None,
                                    "usage": {},
                                }
                        except (json.JSONDecodeError, KeyError):
                            pass

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

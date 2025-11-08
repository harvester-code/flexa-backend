from typing import Any, Dict, Optional, List
import json
import urllib.request

import numpy as np
import pandas as pd


class HomeAnalyzer:
    def __init__(
        self,
        pax_df: pd.DataFrame,
        percentile: int | None = None,
        process_flow: Optional[List[dict]] = None,
        metadata: Optional[Dict[str, Any]] = None,
        country_to_airports_path: Optional[str] = None,
        interval_minutes: int = 60,
    ):
        # 전체 데이터를 유지 - 각 함수에서 status 기준으로 필터링
        self.pax_df = pax_df.copy()

        self.percentile = percentile
        self.interval_minutes = interval_minutes
        self.process_list = self._get_process_list()
        self.process_flow_map = self._build_process_flow_map(process_flow)
        self.metadata = metadata  # facility_metrics 계산을 위해 추가
        self.country_to_airports_path = country_to_airports_path
        self._gdp_cache = {}  # GDP 조회 결과 캐싱

    # ===============================
    # 헬퍼 함수들
    # ===============================

    def _get_airport_gdp(self) -> Optional[Dict[str, Any]]:
        """공항 코드로 GDP 정보 가져오기 (캐싱 지원)"""
        if not self.metadata or not self.country_to_airports_path:
            print(f"GDP 조회 실패: metadata={self.metadata is not None}, country_path={self.country_to_airports_path}")
            return None

        # 1. metadata에서 airport 코드 가져오기
        airport_code = self.metadata.get('context', {}).get('airport')
        if not airport_code:
            print(f"GDP 조회 실패: airport_code가 metadata.context에 없음")
            return None

        print(f"GDP 조회 시작: airport_code={airport_code}")

        # 캐시 확인
        if airport_code in self._gdp_cache:
            print(f"GDP 캐시에서 반환: {airport_code}")
            return self._gdp_cache[airport_code]

        try:
            # 2. country_to_airports.json에서 국가 코드 찾기
            with open(self.country_to_airports_path, 'r', encoding='utf-8') as f:
                country_data = json.load(f)

            country_code = None
            country_name = None
            for code, info in country_data.items():
                if airport_code in info.get('airports', []):
                    country_code = code
                    country_name = info.get('name')
                    break

            if not country_code:
                print(f"GDP 조회 실패: {airport_code}가 country_to_airports.json에 없음")
                return None

            print(f"국가 찾음: {country_name} ({country_code})")

            # 3. World Bank API로 GDP, GDP PPP, 인구 조회
            result = {
                "airport_code": airport_code,
                "country_code": country_code,
                "country_name": country_name,
                "gdp": None,
                "gdp_ppp": None,
                "population": None,
                "gdp_per_capita": None,
                "gdp_ppp_per_capita": None,
            }

            # GDP (current US$)
            try:
                url_gdp = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.CD?format=json&per_page=1&date=2020:2025"
                with urllib.request.urlopen(url_gdp, timeout=10) as response:
                    data = json.loads(response.read().decode())
                    if len(data) > 1 and data[1] and len(data[1]) > 0:
                        latest = data[1][0]
                        if latest.get('value'):
                            result["gdp"] = {
                                "year": latest['date'],
                                "value": latest['value'],
                                "value_billions": round(latest['value'] / 1e9, 2),
                                "formatted": f"${latest['value'] / 1e9:.2f}B",
                                "indicator": "GDP (current US$)"
                            }
            except Exception as e:
                print(f"GDP 조회 실패: {e}")

            # GDP PPP (current international $)
            try:
                url_ppp = f"https://api.worldbank.org/v2/country/{country_code}/indicator/NY.GDP.MKTP.PP.CD?format=json&per_page=1&date=2020:2025"
                with urllib.request.urlopen(url_ppp, timeout=10) as response:
                    data = json.loads(response.read().decode())
                    if len(data) > 1 and data[1] and len(data[1]) > 0:
                        latest = data[1][0]
                        if latest.get('value'):
                            result["gdp_ppp"] = {
                                "year": latest['date'],
                                "value": latest['value'],
                                "value_billions": round(latest['value'] / 1e9, 2),
                                "formatted": f"${latest['value'] / 1e9:.2f}B",
                                "indicator": "GDP, PPP (current international $)"
                            }
            except Exception as e:
                print(f"GDP PPP 조회 실패: {e}")

            # 인구 (Population, total)
            try:
                url_pop = f"https://api.worldbank.org/v2/country/{country_code}/indicator/SP.POP.TOTL?format=json&per_page=1&date=2020:2025"
                with urllib.request.urlopen(url_pop, timeout=10) as response:
                    data = json.loads(response.read().decode())
                    if len(data) > 1 and data[1] and len(data[1]) > 0:
                        latest = data[1][0]
                        if latest.get('value'):
                            result["population"] = {
                                "year": latest['date'],
                                "value": latest['value'],
                                "formatted": f"{latest['value']:,.0f}",
                                "indicator": "Population, total"
                            }
            except Exception as e:
                print(f"인구 조회 실패: {e}")

            # 1인당 GDP 계산
            if result["gdp"] and result["population"]:
                gdp_per_capita = result["gdp"]["value"] / result["population"]["value"]
                result["gdp_per_capita"] = round(gdp_per_capita, 2)

            if result["gdp_ppp"] and result["population"]:
                gdp_ppp_per_capita = result["gdp_ppp"]["value"] / result["population"]["value"]
                result["gdp_ppp_per_capita"] = round(gdp_ppp_per_capita, 2)

            # 최소 하나라도 데이터가 있으면 반환
            if result["gdp"] or result["gdp_ppp"]:
                # 캐시 저장
                self._gdp_cache[airport_code] = result
                return result
        except Exception as e:
            print(f"GDP 조회 실패 ({airport_code}): {e}")

        return None

    # ===============================
    # 메인 함수들
    # ===============================

    def _add_is_boarded_column(self, working_df: pd.DataFrame) -> pd.DataFrame:
        """
        is_boarded 열을 추가합니다.
        탑승 조건:
        1. 모든 프로세스에서 failed가 없음 (skipped는 괜찮음)
        2. 마지막 프로세스 완료 시간이 출발 시간보다 빠름
        """
        def calculate_boarded(row):
            # 조건 1: failed가 없는가?
            for process in self.process_list:
                status = row.get(f'{process}_status')
                if status == 'failed':
                    return False

            # 조건 2: 마지막 completed 프로세스 찾기
            last_completed_process = None
            for process in reversed(self.process_list):
                status = row.get(f'{process}_status')
                if status == 'completed':
                    last_completed_process = process
                    break

            if last_completed_process is None:
                return False

            done_time = row.get(f'{last_completed_process}_done_time')
            scheduled_departure = row.get('scheduled_departure_local')

            if pd.notna(done_time) and pd.notna(scheduled_departure):
                return done_time < scheduled_departure

            return False

        df_copy = working_df.copy()
        df_copy['is_boarded'] = df_copy.apply(calculate_boarded, axis=1)
        return df_copy

    def _calculate_time_metrics_and_dwell_times(self) -> Optional[Dict[str, Any]]:
        """
        time_metrics와 dwell_times를 계산합니다.
        - percentile이 None이면 평균 계산
        - percentile이 있으면 Total Wait Time 기준 상위 N% 승객의 값 사용
        """
        try:
            # is_boarded 열 추가
            working_df = self._add_is_boarded_column(self.pax_df)

            # 계산 방식 결정
            metric = f"p{self.percentile}" if self.percentile is not None else "mean"

            if self.percentile is not None:
                # Percentile 모드: 각 승객의 completed 프로세스 합산 후 상위 N% 승객 찾기
                total_open_wait_per_pax = pd.Series([pd.Timedelta(0)] * len(working_df), index=working_df.index)
                total_queue_wait_per_pax = pd.Series([pd.Timedelta(0)] * len(working_df), index=working_df.index)
                total_process_time_per_pax = pd.Series([pd.Timedelta(0)] * len(working_df), index=working_df.index)

                for process in self.process_list:
                    status_col = f"{process}_status"
                    open_wait_col = f"{process}_open_wait_time"
                    queue_wait_col = f"{process}_queue_wait_time"
                    start_time_col = f"{process}_start_time"
                    done_time_col = f"{process}_done_time"

                    # 해당 프로세스를 completed한 승객만 시간 합산
                    if status_col in working_df.columns:
                        completed_mask = working_df[status_col] == 'completed'

                        # open_wait_time 합산
                        if open_wait_col in working_df.columns:
                            open_wait_values = pd.to_timedelta(working_df[open_wait_col], errors='coerce').fillna(pd.Timedelta(0))
                            total_open_wait_per_pax = total_open_wait_per_pax + open_wait_values.where(completed_mask, pd.Timedelta(0))

                        # queue_wait_time 합산
                        if queue_wait_col in working_df.columns:
                            queue_wait_values = pd.to_timedelta(working_df[queue_wait_col], errors='coerce').fillna(pd.Timedelta(0))
                            total_queue_wait_per_pax = total_queue_wait_per_pax + queue_wait_values.where(completed_mask, pd.Timedelta(0))

                        # process_time 합산: done_time - start_time
                        if start_time_col in working_df.columns and done_time_col in working_df.columns:
                            start_times = pd.to_datetime(working_df[start_time_col], errors='coerce')
                            done_times = pd.to_datetime(working_df[done_time_col], errors='coerce')
                            process_duration = (done_times - start_times).fillna(pd.Timedelta(0))
                            # 음수는 0으로
                            process_duration = process_duration.apply(lambda x: x if x.total_seconds() >= 0 else pd.Timedelta(0))
                            total_process_time_per_pax = total_process_time_per_pax + process_duration.where(completed_mask, pd.Timedelta(0))

                # 각 승객의 전체 대기시간 계산 (open + queue)
                total_wait_per_pax = total_open_wait_per_pax + total_queue_wait_per_pax

                # total_wait_per_pax 기준으로 상위 N% 승객 찾기
                q = 1 - (self.percentile / 100)
                target_wait_value = total_wait_per_pax.dt.total_seconds().quantile(q)
                diff = (total_wait_per_pax.dt.total_seconds() - target_wait_value).abs()
                target_pax_idx = diff.idxmin()

                # 그 승객의 모든 시간 메트릭 사용
                total_open_wait_seconds = total_open_wait_per_pax.loc[target_pax_idx].total_seconds()
                total_queue_wait_seconds = total_queue_wait_per_pax.loc[target_pax_idx].total_seconds()
                total_wait_seconds = total_wait_per_pax.loc[target_pax_idx].total_seconds()
                total_process_time_seconds = total_process_time_per_pax.loc[target_pax_idx].total_seconds()

                # 그 승객의 commercial_dwell_time 계산
                target_pax = working_df.loc[target_pax_idx]
                last_completed_process = None
                for process in reversed(self.process_list):
                    status_col = f"{process}_status"
                    if status_col in working_df.columns and target_pax[status_col] == 'completed':
                        last_completed_process = process
                        break

                if last_completed_process and 'scheduled_departure_local' in working_df.columns:
                    last_done_col = f"{last_completed_process}_done_time"
                    if last_done_col in working_df.columns:
                        done_t = pd.to_datetime(target_pax[last_done_col], errors='coerce')
                        depart_t = pd.to_datetime(target_pax['scheduled_departure_local'], errors='coerce')
                        if pd.notna(done_t) and pd.notna(depart_t):
                            commercial_dwell_value = max(0, (depart_t - done_t).total_seconds())
                        else:
                            commercial_dwell_value = 0
                    else:
                        commercial_dwell_value = 0
                else:
                    commercial_dwell_value = 0

                # airport_dwell_time = total_wait + process_time + commercial_dwell
                airport_dwell_value = total_wait_seconds + total_process_time_seconds + commercial_dwell_value

            else:
                # Mean 모드: 각 프로세스별로 completed한 승객들의 평균 시간을 구하고 합산
                total_open_wait_seconds = 0
                total_queue_wait_seconds = 0
                total_process_time_seconds = 0

                for process in self.process_list:
                    status_col = f"{process}_status"
                    open_wait_col = f"{process}_open_wait_time"
                    queue_wait_col = f"{process}_queue_wait_time"
                    start_time_col = f"{process}_start_time"
                    done_time_col = f"{process}_done_time"

                    if status_col not in working_df.columns:
                        continue

                    # 해당 프로세스에서 completed된 승객만 필터링
                    completed_df = working_df[working_df[status_col] == 'completed']

                    if len(completed_df) == 0:
                        continue

                    # open_wait_time 평균
                    if open_wait_col in completed_df.columns:
                        open_wait_values = pd.to_timedelta(completed_df[open_wait_col], errors='coerce').dropna()
                        if len(open_wait_values) > 0:
                            total_open_wait_seconds += open_wait_values.dt.total_seconds().mean()

                    # queue_wait_time 평균
                    if queue_wait_col in completed_df.columns:
                        queue_wait_values = pd.to_timedelta(completed_df[queue_wait_col], errors='coerce').dropna()
                        if len(queue_wait_values) > 0:
                            total_queue_wait_seconds += queue_wait_values.dt.total_seconds().mean()

                    # process_time 평균
                    if start_time_col in completed_df.columns and done_time_col in completed_df.columns:
                        start_times = pd.to_datetime(completed_df[start_time_col], errors='coerce')
                        done_times = pd.to_datetime(completed_df[done_time_col], errors='coerce')

                        # 둘 다 valid한 경우만
                        valid_mask = start_times.notna() & done_times.notna()
                        if valid_mask.sum() > 0:
                            process_duration = (done_times[valid_mask] - start_times[valid_mask])
                            # 음수 제거
                            process_duration = process_duration[process_duration.dt.total_seconds() >= 0]
                            if len(process_duration) > 0:
                                total_process_time_seconds += process_duration.dt.total_seconds().mean()

                # 전체 대기시간
                total_wait_seconds = total_open_wait_seconds + total_queue_wait_seconds

                # dwell_times 계산 (평균)
                commercial_dwell_per_pax = []
                for idx, row in working_df.iterrows():
                    if row['is_boarded']:
                        last_completed_process = None
                        for process in reversed(self.process_list):
                            status_col = f"{process}_status"
                            if status_col in working_df.columns and row[status_col] == 'completed':
                                last_completed_process = process
                                break

                        if last_completed_process and 'scheduled_departure_local' in working_df.columns:
                            last_done_col = f"{last_completed_process}_done_time"
                            if last_done_col in working_df.columns:
                                done_t = pd.to_datetime(row[last_done_col], errors='coerce')
                                depart_t = pd.to_datetime(row['scheduled_departure_local'], errors='coerce')
                                if pd.notna(done_t) and pd.notna(depart_t):
                                    dwell = (depart_t - done_t).total_seconds()
                                    commercial_dwell_per_pax.append(max(0, dwell))
                                else:
                                    commercial_dwell_per_pax.append(0)
                            else:
                                commercial_dwell_per_pax.append(0)
                        else:
                            commercial_dwell_per_pax.append(0)
                    else:
                        commercial_dwell_per_pax.append(0)

                commercial_dwell_value = sum(commercial_dwell_per_pax) / len(commercial_dwell_per_pax) if commercial_dwell_per_pax else 0

                # airport_dwell_time: total_wait + total_process_time + commercial_dwell
                airport_dwell_value = total_wait_seconds + total_process_time_seconds + commercial_dwell_value

            # HMS 변환
            open_wait = self._format_waiting_time(total_open_wait_seconds)
            queue_wait = self._format_waiting_time(total_queue_wait_seconds)
            total_wait = self._format_waiting_time(total_wait_seconds)
            process_time = self._format_waiting_time(total_process_time_seconds)
            commercial_dwell_time = self._format_waiting_time(commercial_dwell_value)
            airport_dwell_time = self._format_waiting_time(airport_dwell_value)

            return {
                "timeMetrics": {
                    "metric": metric,
                    "open_wait": open_wait,
                    "queue_wait": queue_wait,
                    "total_wait": total_wait,
                    "process_time": process_time,
                },
                "dwellTimes": {
                    "commercial_dwell_time": commercial_dwell_time,
                    "airport_dwell_time": airport_dwell_time,
                }
            }
        except Exception as e:
            print(f"Time metrics 계산 중 오류 발생: {e}")
            return None

    def _calculate_opened_counts(self) -> Dict[str, Any]:
        """
        metadata의 process_flow에서 각 프로세스/존별 opened 정보 계산

        Returns:
            {
                "travel_tax": {
                    "total": 12,
                    "opened": 9,
                    "zones": {
                        "B": {"total": 3, "opened": 3},
                        ...
                    }
                },
                ...
            }
        """
        result = {}

        if not self.metadata or 'process_flow' not in self.metadata:
            return result

        process_flow = self.metadata['process_flow']

        for process_info in process_flow:
            process_name = process_info.get('name')
            if not process_name:
                continue

            zones = process_info.get('zones', {})

            process_total = 0
            process_opened = 0
            zone_data = {}

            for zone_name, zone_info in zones.items():
                facilities = zone_info.get('facilities', [])

                zone_total = len(facilities)
                zone_opened = 0

                for facility in facilities:
                    # 해당 시설이 한 번이라도 운영했는지 확인
                    time_blocks = facility.get('operating_schedule', {}).get('time_blocks', [])
                    has_activated = any(block.get('activate', False) for block in time_blocks)

                    if has_activated:
                        zone_opened += 1

                zone_data[zone_name] = {
                    'total': zone_total,
                    'opened': zone_opened
                }

                process_total += zone_total
                process_opened += zone_opened

            result[process_name] = {
                'total': process_total,
                'opened': process_opened,
                'zones': zone_data
            }

        return result

    def _calculate_facility_metrics(self) -> Optional[List[Dict[str, Any]]]:
        """
        facility_metrics를 계산합니다.
        v2.py의 로직을 따릅니다.
        """
        try:
            if not self.metadata or 'process_flow' not in self.metadata:
                return None

            facility_metrics_list = []
            process_flow = self.metadata['process_flow']

            # 시뮬레이션 기간 계산
            simulation_hours = 24.0
            simulation_start = None
            simulation_end = None

            try:
                for process_info in process_flow:
                    zones = process_info.get('zones', {})
                    for zone_name, zone_data in zones.items():
                        facilities = zone_data.get('facilities', [])
                        for facility in facilities:
                            time_blocks = facility.get('operating_schedule', {}).get('time_blocks', [])
                            for block in time_blocks:
                                if block.get('activate', False):
                                    period = block.get('period', '')
                                    parts = period.split('-')
                                    if len(parts) >= 6:
                                        start_str = f"{parts[0]}-{parts[1]}-{parts[2]}"
                                        end_str = f"{parts[3]}-{parts[4]}-{parts[5]}"
                                        start = pd.to_datetime(start_str.strip())
                                        end = pd.to_datetime(end_str.strip())

                                        if simulation_start is None or start < simulation_start:
                                            simulation_start = start
                                        if simulation_end is None or end > simulation_end:
                                            simulation_end = end

                if simulation_start and simulation_end:
                    simulation_hours = (simulation_end - simulation_start).total_seconds() / 3600
            except:
                pass

            # 각 프로세스 순회
            for process_info in process_flow:
                process_name = process_info.get('name')
                if not process_name:
                    continue

                zones = process_info.get('zones', {})

                for zone_name, zone_data in zones.items():
                    facilities = zone_data.get('facilities', [])

                    for facility in facilities:
                        facility_id = facility.get('id')
                        if not facility_id:
                            continue

                        # 1. operating_rate 계산
                        operating_hours = 0
                        operating_schedule = facility.get('operating_schedule', {})
                        time_blocks = operating_schedule.get('time_blocks', [])
                        process_time_seconds = None

                        for block in time_blocks:
                            if block.get('activate', False):
                                if process_time_seconds is None:
                                    process_time_seconds = block.get('process_time_seconds', 30)

                                period = block.get('period', '')
                                parts = period.split('-')
                                if len(parts) >= 6:
                                    try:
                                        start_str = f"{parts[0]}-{parts[1]}-{parts[2]}"
                                        end_str = f"{parts[3]}-{parts[4]}-{parts[5]}"
                                        start_time = pd.to_datetime(start_str.strip())
                                        end_time = pd.to_datetime(end_str.strip())
                                        duration = (end_time - start_time).total_seconds() / 3600
                                        operating_hours += duration
                                    except:
                                        pass

                        operating_rate = operating_hours / simulation_hours if operating_hours > 0 and simulation_hours > 0 else 0

                        # 2. utilization_rate 계산
                        facility_col = f"{process_name}_facility"
                        status_col = f"{process_name}_status"

                        if facility_col in self.pax_df.columns and status_col in self.pax_df.columns:
                            facility_pax = self.pax_df[
                                (self.pax_df[facility_col] == facility_id) &
                                (self.pax_df[status_col] == 'completed')
                            ]

                            actual_count = len(facility_pax)

                            if operating_hours > 0 and process_time_seconds:
                                max_capacity = (operating_hours * 3600) / process_time_seconds
                                utilization_rate = actual_count / max_capacity
                            else:
                                utilization_rate = 0.0
                        else:
                            utilization_rate = 0.0

                        # 3. total_rate 계산
                        total_rate = operating_rate * utilization_rate

                        facility_metrics_list.append({
                            "facility_id": facility_id,
                            "process": process_name,
                            "zone": zone_name,
                            "operating_rate": round(operating_rate, 2),
                            "utilization_rate": round(utilization_rate, 2),
                            "total_rate": round(total_rate, 2)
                        })

            # 단일 패스로 모든 레벨 집계 (최적화)
            facility_metrics_aggregated = []

            if facility_metrics_list:
                from collections import defaultdict

                # 집계용 딕셔너리
                aggregator = {
                    'total': {'op': [], 'util': [], 'tot': []},
                    'by_process': defaultdict(lambda: {'op': [], 'util': [], 'tot': []}),
                    'by_zone': defaultdict(lambda: {'op': [], 'util': [], 'tot': []})
                }

                # 단일 순회로 모든 레벨 동시 집계
                for metric in facility_metrics_list:
                    process = metric['process']
                    zone = metric['zone']
                    op_rate = metric['operating_rate']
                    util_rate = metric['utilization_rate']
                    tot_rate = metric['total_rate']

                    # Total 레벨
                    aggregator['total']['op'].append(op_rate)
                    aggregator['total']['util'].append(util_rate)
                    aggregator['total']['tot'].append(tot_rate)

                    # Process 레벨
                    aggregator['by_process'][process]['op'].append(op_rate)
                    aggregator['by_process'][process]['util'].append(util_rate)
                    aggregator['by_process'][process]['tot'].append(tot_rate)

                    # Zone 레벨 (process:zone 키로 저장)
                    zone_key = f"{process}:{zone}"
                    aggregator['by_zone'][zone_key]['op'].append(op_rate)
                    aggregator['by_zone'][zone_key]['util'].append(util_rate)
                    aggregator['by_zone'][zone_key]['tot'].append(tot_rate)

                # 평균 계산 헬퍼 함수
                def calc_avg(lst):
                    return round(sum(lst) / len(lst), 2) if lst else 0

                # Total 집계
                if aggregator['total']['op']:
                    facility_metrics_aggregated.append({
                        "process": "total",
                        "operating_rate": calc_avg(aggregator['total']['op']),
                        "utilization_rate": calc_avg(aggregator['total']['util']),
                        "total_rate": calc_avg(aggregator['total']['tot'])
                    })

                # Process별 집계 (Zone 데이터 포함)
                for process, metrics in aggregator['by_process'].items():
                    # 해당 프로세스의 Zone별 데이터 구성
                    zones = {}
                    for zone_key, zone_metrics in aggregator['by_zone'].items():
                        p, z = zone_key.split(':', 1)
                        if p == process:
                            zones[z] = {
                                "operating_rate": calc_avg(zone_metrics['op']),
                                "utilization_rate": calc_avg(zone_metrics['util']),
                                "total_rate": calc_avg(zone_metrics['tot'])
                            }

                    facility_metrics_aggregated.append({
                        "process": process,
                        "operating_rate": calc_avg(metrics['op']),
                        "utilization_rate": calc_avg(metrics['util']),
                        "total_rate": calc_avg(metrics['tot']),
                        "zones": zones  # Zone별 세부 데이터 추가
                    })

            return facility_metrics_aggregated if facility_metrics_aggregated else None
        except Exception as e:
            print(f"Facility metrics 계산 중 오류 발생: {e}")
            return None

    def _calculate_passenger_summary(self) -> Dict[str, int]:
        """
        passenger_summary를 계산합니다.
        v2.py의 로직을 따릅니다.
        """
        # is_boarded 열 추가
        working_df = self._add_is_boarded_column(self.pax_df)

        # 전체 여객 수
        total_passengers = len(working_df)

        # completed vs missed 구분 (is_boarded 기준)
        completed_count = int(working_df['is_boarded'].sum())  # True 개수
        missed_count = int((~working_df['is_boarded']).sum())  # False 개수

        return {
            "total": total_passengers,
            "completed": completed_count,
            "missed": missed_count
        }

    def _calculate_economic_impact(self, time_metrics_data: Optional[Dict[str, Any]], passenger_count: int) -> Optional[Dict[str, Any]]:
        """
        시간 가치를 기반으로 경제적 영향을 계산합니다.
        - Total Wait Time: 음수 (손실)
        - Proc. & Queueing Time: 음수 (손실)
        - Commercial Dwell Time: 양수 (이득)
        """
        try:
            if not time_metrics_data:
                return None

            # GDP 정보 조회
            airport_gdp = self._get_airport_gdp()
            if not airport_gdp:
                return None

            # GDP PPP per capita 사용 (없으면 GDP per capita 사용)
            gdp_per_capita = airport_gdp.get('gdp_ppp_per_capita') or airport_gdp.get('gdp_per_capita')

            # GDP 정보가 없으면 계산 불가
            if not gdp_per_capita:
                return None

            # 1인당 시간당 가치 계산 (연간 GDP / 365일 / 24시간)
            hourly_value_per_person = gdp_per_capita / (365 * 24)

            # 시간 메트릭을 시간 단위로 변환
            time_metrics = time_metrics_data.get('timeMetrics', {})
            dwell_times = time_metrics_data.get('dwellTimes', {})

            def hms_to_hours(time_dict):
                """HMS를 시간 단위로 변환"""
                if not time_dict:
                    return 0.0
                return time_dict.get('hour', 0) + time_dict.get('minute', 0) / 60 + time_dict.get('second', 0) / 3600

            total_wait_hours = hms_to_hours(time_metrics.get('total_wait'))
            process_time_hours = hms_to_hours(time_metrics.get('process_time'))
            commercial_dwell_hours = hms_to_hours(dwell_times.get('commercial_dwell_time'))

            # 경제적 가치 계산
            # 음수: 손실 (대기/처리 시간)
            # 양수: 이득 (상업 시간)
            total_wait_value = -(hourly_value_per_person * total_wait_hours * passenger_count)
            process_time_value = -(hourly_value_per_person * process_time_hours * passenger_count)
            commercial_dwell_value = hourly_value_per_person * commercial_dwell_hours * passenger_count

            return {
                "hourly_value_per_person": round(hourly_value_per_person, 2),
                "total_wait_value": round(total_wait_value, 2),
                "process_time_value": round(process_time_value, 2),
                "commercial_dwell_value": round(commercial_dwell_value, 2),
                "currency": "USD",
                "airport_context": airport_gdp  # GDP 정보 포함
            }
        except Exception as e:
            print(f"Economic impact 계산 중 오류 발생: {e}")
            return None

    def get_summary(self):
        """요약 데이터 생성"""
        # 각 프로세스별로 completed된 승객의 경험을 독립적으로 계산
        pax_experience_waiting = {}
        pax_experience_queue = {}

        if self.percentile is not None:
            # Percentile 모드: 모든 프로세스를 합산한 Total Wait Time 기준으로 상위 N% 승객 1명 찾기
            # 각 승객의 모든 프로세스의 total_wait 합산
            total_wait_per_pax = pd.Series([pd.Timedelta(0)] * len(self.pax_df), index=self.pax_df.index)

            for process in self.process_list:
                status_col = f"{process}_status"
                open_wait_col = f"{process}_open_wait_time"
                queue_wait_col = f"{process}_queue_wait_time"

                if status_col in self.pax_df.columns:
                    completed_mask = self.pax_df[status_col] == 'completed'

                    # 해당 프로세스를 completed한 승객의 total_wait (open + queue) 합산
                    process_total_wait = pd.Series([pd.Timedelta(0)] * len(self.pax_df), index=self.pax_df.index)

                    if open_wait_col in self.pax_df.columns:
                        open_wait_values = pd.to_timedelta(self.pax_df[open_wait_col], errors='coerce').fillna(pd.Timedelta(0))
                        process_total_wait = process_total_wait + open_wait_values.where(completed_mask, pd.Timedelta(0))

                    if queue_wait_col in self.pax_df.columns:
                        queue_wait_values = pd.to_timedelta(self.pax_df[queue_wait_col], errors='coerce').fillna(pd.Timedelta(0))
                        process_total_wait = process_total_wait + queue_wait_values.where(completed_mask, pd.Timedelta(0))

                    total_wait_per_pax = total_wait_per_pax + process_total_wait

            # 전체 합산 Total Wait Time 기준으로 상위 N% 승객 찾기
            q = 1 - (self.percentile / 100)
            target_wait_value = total_wait_per_pax.dt.total_seconds().quantile(q)
            diff = (total_wait_per_pax.dt.total_seconds() - target_wait_value).abs()
            target_pax_idx = diff.idxmin()

            # 그 승객의 각 프로세스별 값 사용
            for process in self.process_list:
                status_col = f"{process}_status"
                open_wait_col = f"{process}_open_wait_time"
                queue_wait_col = f"{process}_queue_wait_time"
                queue_col = f"{process}_queue_length"

                # 해당 프로세스를 completed 했는지 확인
                if status_col in self.pax_df.columns and self.pax_df.loc[target_pax_idx, status_col] == 'completed':
                    # 그 승객의 open_wait, queue_wait 값
                    open_wait_value = 0
                    queue_wait_value = 0

                    if open_wait_col in self.pax_df.columns:
                        open_wait_td = pd.to_timedelta(self.pax_df.loc[target_pax_idx, open_wait_col], errors='coerce')
                        open_wait_value = open_wait_td.total_seconds() if pd.notna(open_wait_td) else 0

                    if queue_wait_col in self.pax_df.columns:
                        queue_wait_td = pd.to_timedelta(self.pax_df.loc[target_pax_idx, queue_wait_col], errors='coerce')
                        queue_wait_value = queue_wait_td.total_seconds() if pd.notna(queue_wait_td) else 0

                    total_wait_value = open_wait_value + queue_wait_value

                    pax_experience_waiting[process] = {
                        "total": self._format_waiting_time(total_wait_value),
                        "open_wait": self._format_waiting_time(open_wait_value),
                        "queue_wait": self._format_waiting_time(queue_wait_value)
                    }

                    # 대기열도 같은 승객의 값 사용
                    if queue_col in self.pax_df.columns:
                        queue_length_value = self.pax_df.loc[target_pax_idx, queue_col]
                        pax_experience_queue[process] = int(queue_length_value) if pd.notna(queue_length_value) else 0
                    else:
                        pax_experience_queue[process] = 0
                else:
                    # 이 승객이 해당 프로세스를 완료하지 않았으면 0
                    pax_experience_waiting[process] = {
                        "total": {"hour": 0, "minute": 0, "second": 0},
                        "open_wait": {"hour": 0, "minute": 0, "second": 0},
                        "queue_wait": {"hour": 0, "minute": 0, "second": 0}
                    }
                    pax_experience_queue[process] = 0

        else:
            # Mean 모드: 각 프로세스별로 평균 계산
            for process in self.process_list:
                # 해당 프로세스에서 completed된 승객만 사용
                process_completed_df = self._filter_by_status(self.pax_df, process)

                # 대기시간 계산 (open + queue)
                open_wait = self._get_open_wait_time(process_completed_df, process)
                queue_wait = self._get_waiting_time(process_completed_df, process)
                total_wait = open_wait + queue_wait

                # 각각의 유효한 값 필터링
                valid_open = open_wait.dropna()
                valid_queue = queue_wait.dropna()
                valid_total = total_wait.dropna()

                if len(valid_total) > 0:
                    # Mean 모드: 평균 계산
                    pax_experience_waiting[process] = {
                        "total": self._format_waiting_time(valid_total.mean()),
                        "open_wait": self._format_waiting_time(valid_open.mean() if len(valid_open) > 0 else 0),
                        "queue_wait": self._format_waiting_time(valid_queue.mean() if len(valid_queue) > 0 else 0)
                    }

                    # 대기열 계산 (평균)
                    queue_col = f"{process}_queue_length"
                    if queue_col in process_completed_df.columns:
                        valid_queue_length = process_completed_df[queue_col].dropna()
                        if len(valid_queue_length) > 0:
                            mean_queue = int(valid_queue_length.mean())
                            pax_experience_queue[process] = mean_queue
                        else:
                            pax_experience_queue[process] = 0
                    else:
                        pax_experience_queue[process] = 0
                else:
                    pax_experience_waiting[process] = {
                        "total": {"hour": 0, "minute": 0, "second": 0},
                        "open_wait": {"hour": 0, "minute": 0, "second": 0},
                        "queue_wait": {"hour": 0, "minute": 0, "second": 0}
                    }
                    pax_experience_queue[process] = 0

        # 응답 데이터 구성
        data = {
            "pax_experience": {
                "waiting_time": pax_experience_waiting,
                "queue_length": pax_experience_queue,
            },
        }

        # time_metrics와 dwell_times 추가
        time_metrics_data = self._calculate_time_metrics_and_dwell_times()
        if time_metrics_data:
            data.update(time_metrics_data)

        # facility_metrics 추가
        facility_metrics = self._calculate_facility_metrics()
        if facility_metrics:
            data["facility_metrics"] = facility_metrics

        # passenger_summary 추가
        passenger_summary = self._calculate_passenger_summary()
        if passenger_summary:
            data["passenger_summary"] = passenger_summary

        # economic_impact 추가 (time_metrics와 passenger_count 필요)
        if time_metrics_data and passenger_summary:
            economic_impact = self._calculate_economic_impact(
                time_metrics_data,
                passenger_summary['total']
            )
            if economic_impact:
                data["economic_impact"] = economic_impact

        return data

    def get_flow_chart_data(self, interval_minutes: int = None):
        """플로우 차트 데이터 생성 - 계층 구조로 변경"""
        interval_minutes = interval_minutes or self.interval_minutes
        time_df = self._create_time_df_index(interval_minutes)
        data = {"times": time_df.index.strftime("%Y-%m-%d %H:%M:%S").tolist()}

        for process in self.process_list:
            # 해당 프로세스에서 completed 상태인 승객만 사용
            all_process_data = self._filter_by_status(self.pax_df, process)

            # 프로세스 데이터를 분리: zone이 있는 데이터와 None 데이터
            zone_col = f"{process}_zone"
            has_zone = all_process_data[zone_col].notna()
            no_zone = all_process_data[zone_col].isna()

            facilities = sorted(all_process_data[zone_col].dropna().unique())

            # 계층 구조를 위한 프로세스 정보 생성
            process_info = {
                "process_name": process.replace("_", " ").title(),
                "facilities": [],
                "data": {}
            }

            step_config = self.process_flow_map.get(process) if self.process_flow_map else None

            if facilities:
                process_data = all_process_data[has_zone].copy()
                waiting_series = self._get_waiting_time(process_data, process)
                process_data[f"{process}_waiting_seconds"] = waiting_series.dt.total_seconds()

                # 시간 플로어링을 복사본에서 계산
                time_freq = f"{interval_minutes}min"
                process_data[f"{process}_on_floored"] = process_data[
                    f"{process}_on_pred"
                ].dt.floor(time_freq)
                process_data[f"{process}_done_floored"] = process_data[
                    f"{process}_done_time"
                ].dt.floor(time_freq)

                # 한번에 모든 메트릭 계산
                metrics = {
                    "inflow": process_data.groupby(
                        [f"{process}_on_floored", f"{process}_zone"]
                    ).size(),
                    "outflow": process_data.groupby(
                        [f"{process}_done_floored", f"{process}_zone"]
                    ).size(),
                    "queue_length": process_data.groupby(
                        [f"{process}_on_floored", f"{process}_zone"]
                    )[f"{process}_queue_length"].mean(),
                    "waiting_time": process_data.groupby(
                        [f"{process}_on_floored", f"{process}_zone"]
                    )[f"{process}_waiting_seconds"].mean(),
                }

                # 항공사별 메트릭 계산 (항공사 필터링을 위해)
                airline_col = "operating_carrier_iata"
                airline_name_col = "operating_carrier_name"
                metrics_by_airline = {}
                airline_name_mapping = {}

                if airline_col in process_data.columns:
                    # 항공사 코드-이름 매핑 생성
                    if airline_name_col in process_data.columns:
                        airline_mapping_df = process_data[[airline_col, airline_name_col]].drop_duplicates()
                        airline_name_mapping = dict(zip(
                            airline_mapping_df[airline_col],
                            airline_mapping_df[airline_name_col]
                        ))

                    metrics_by_airline = {
                        "inflow": process_data.groupby(
                            [f"{process}_on_floored", f"{process}_zone", airline_col]
                        ).size(),
                        "outflow": process_data.groupby(
                            [f"{process}_done_floored", f"{process}_zone", airline_col]
                        ).size(),
                        "queue_length": process_data.groupby(
                            [f"{process}_on_floored", f"{process}_zone", airline_col]
                        )[f"{process}_queue_length"].mean(),
                        "waiting_time": process_data.groupby(
                            [f"{process}_on_floored", f"{process}_zone", airline_col]
                        )[f"{process}_waiting_seconds"].mean(),
                    }

                # unstack하고 reindex 한번에
                pivoted = {
                    k: v.unstack(fill_value=0).reindex(time_df.index, fill_value=0)
                    for k, v in metrics.items()
                }

                # 항공사별 데이터도 unstack
                pivoted_by_airline = {}
                if metrics_by_airline:
                    for metric_key, metric_series in metrics_by_airline.items():
                        # MultiIndex: (time, zone, airline) -> DataFrame with MultiIndex columns (zone, airline)
                        unstacked = metric_series.unstack(level=[1, 2], fill_value=0)
                        pivoted_by_airline[metric_key] = unstacked.reindex(time_df.index, fill_value=0)

                # 결과 구성
                process_facility_data = {}
                aggregated = {
                    k: pd.Series(0, index=time_df.index, dtype=float)
                    for k in metrics.keys()
                }

                zone_capacity_map: Dict[str, List[float]] = {}
                if step_config and interval_minutes > 0:
                    zone_capacity_map = self._calculate_step_capacity_series_by_zone(
                        step_config,
                        time_df.index,
                        interval_minutes,
                    )

                for facility_name in facilities:
                    # 원래 facility 이름 보존
                    node_name = facility_name

                    facility_data = {
                        k: pivoted[k].get(facility_name, pd.Series(0, index=time_df.index))
                        for k in metrics.keys()
                    }

                    # 집계
                    for k in facility_data.keys():
                        aggregated[k] += facility_data[k]

                    # facilities 리스트에 추가
                    process_info["facilities"].append(node_name)

                    # 저장 (타입 변환)
                    process_facility_data[node_name] = {
                        k: (
                            facility_data[k].round()
                            if k in ["queue_length", "waiting_time"]
                            else facility_data[k]
                        )
                        .astype(int)
                        .tolist()
                        for k in facility_data.keys()
                    }

                    # 항공사별 데이터 추가
                    if pivoted_by_airline:
                        airlines_data = {}
                        # 해당 zone의 모든 항공사 데이터 추출
                        for metric_key, metric_df in pivoted_by_airline.items():
                            # metric_df.columns는 MultiIndex: (zone, airline)
                            if metric_df.columns.nlevels == 2:
                                # 해당 zone의 항공사들
                                zone_airlines = [col for col in metric_df.columns if col[0] == facility_name]
                                for zone_name, airline_code in zone_airlines:
                                    if airline_code not in airlines_data:
                                        airlines_data[airline_code] = {}
                                    series_data = metric_df[(zone_name, airline_code)]
                                    airlines_data[airline_code][metric_key] = (
                                        series_data.round().astype(int).tolist()
                                        if metric_key in ["queue_length", "waiting_time"]
                                        else series_data.astype(int).tolist()
                                    )

                        if airlines_data:
                            process_facility_data[node_name]["airlines"] = airlines_data

                    if node_name in zone_capacity_map:
                        process_facility_data[node_name]["capacity"] = [
                            int(round(value)) for value in zone_capacity_map[node_name]
                        ]

                    # ===== 개별 facility 레벨 데이터 추가 =====
                    # 해당 zone에 속한 개별 facility 데이터 계산
                    facility_col = f"{process}_facility"
                    if facility_col in process_data.columns:
                        # 해당 zone의 데이터만 필터링
                        zone_process_data = process_data[process_data[f"{process}_zone"] == facility_name].copy()

                        if not zone_process_data.empty:
                            # 개별 facility 목록
                            individual_facilities = sorted(zone_process_data[facility_col].dropna().unique())

                            if individual_facilities:
                                # 개별 facility별 메트릭 계산
                                facility_metrics = {
                                    "inflow": zone_process_data.groupby(
                                        [f"{process}_on_floored", facility_col]
                                    ).size(),
                                    "outflow": zone_process_data.groupby(
                                        [f"{process}_done_floored", facility_col]
                                    ).size(),
                                    "queue_length": zone_process_data.groupby(
                                        [f"{process}_on_floored", facility_col]
                                    )[f"{process}_queue_length"].mean(),
                                    "waiting_time": zone_process_data.groupby(
                                        [f"{process}_on_floored", facility_col]
                                    )[f"{process}_waiting_seconds"].mean(),
                                }

                                # 개별 facility별 항공사별 메트릭 계산
                                facility_metrics_by_airline = {}
                                if airline_col in zone_process_data.columns:
                                    facility_metrics_by_airline = {
                                        "inflow": zone_process_data.groupby(
                                            [f"{process}_on_floored", facility_col, airline_col]
                                        ).size(),
                                        "outflow": zone_process_data.groupby(
                                            [f"{process}_done_floored", facility_col, airline_col]
                                        ).size(),
                                        "queue_length": zone_process_data.groupby(
                                            [f"{process}_on_floored", facility_col, airline_col]
                                        )[f"{process}_queue_length"].mean(),
                                        "waiting_time": zone_process_data.groupby(
                                            [f"{process}_on_floored", facility_col, airline_col]
                                        )[f"{process}_waiting_seconds"].mean(),
                                    }

                                # unstack
                                facility_pivoted = {
                                    k: v.unstack(fill_value=0).reindex(time_df.index, fill_value=0)
                                    for k, v in facility_metrics.items()
                                }

                                # 항공사별 데이터도 unstack
                                facility_pivoted_by_airline = {}
                                if facility_metrics_by_airline:
                                    for metric_key, metric_series in facility_metrics_by_airline.items():
                                        # MultiIndex: (time, facility, airline) -> DataFrame with MultiIndex columns (facility, airline)
                                        unstacked = metric_series.unstack(level=[1, 2], fill_value=0)
                                        facility_pivoted_by_airline[metric_key] = unstacked.reindex(time_df.index, fill_value=0)

                                # 개별 facility capacity 계산
                                facility_capacity_map: Dict[str, List[float]] = {}
                                if step_config and interval_minutes > 0:
                                    facility_capacity_map = self._calculate_step_capacity_series_by_facility(
                                        step_config,
                                        facility_name,
                                        time_df.index,
                                        interval_minutes,
                                    )

                                # 개별 facility 데이터 구성
                                sub_facility_data = {}
                                for individual_facility in individual_facilities:
                                    ind_fac_data = {
                                        k: facility_pivoted[k].get(individual_facility, pd.Series(0, index=time_df.index))
                                        for k in facility_metrics.keys()
                                    }

                                    sub_facility_data[individual_facility] = {
                                        k: (
                                            ind_fac_data[k].round()
                                            if k in ["queue_length", "waiting_time"]
                                            else ind_fac_data[k]
                                        )
                                        .astype(int)
                                        .tolist()
                                        for k in ind_fac_data.keys()
                                    }

                                    # 항공사별 데이터 추가
                                    if facility_pivoted_by_airline:
                                        airlines_data = {}
                                        # 해당 facility의 모든 항공사 데이터 추출
                                        for metric_key, metric_df in facility_pivoted_by_airline.items():
                                            # metric_df.columns는 MultiIndex: (facility, airline)
                                            if metric_df.columns.nlevels == 2:
                                                # 해당 facility의 항공사들
                                                facility_airlines = [col for col in metric_df.columns if col[0] == individual_facility]
                                                for facility_name_col, airline_code in facility_airlines:
                                                    if airline_code not in airlines_data:
                                                        airlines_data[airline_code] = {}
                                                    series_data = metric_df[(facility_name_col, airline_code)]
                                                    airlines_data[airline_code][metric_key] = (
                                                        series_data.round().astype(int).tolist()
                                                        if metric_key in ["queue_length", "waiting_time"]
                                                        else series_data.astype(int).tolist()
                                                    )

                                        if airlines_data:
                                            sub_facility_data[individual_facility]["airlines"] = airlines_data

                                    # capacity 추가
                                    if individual_facility in facility_capacity_map:
                                        sub_facility_data[individual_facility]["capacity"] = [
                                            int(round(value)) for value in facility_capacity_map[individual_facility]
                                        ]

                                # zone 데이터에 추가
                                process_facility_data[node_name]["sub_facilities"] = individual_facilities
                                process_facility_data[node_name]["facility_data"] = sub_facility_data

                # None/Skip 데이터 처리 - 프로세스를 건너뛴 승객
                if no_zone.any():
                    skip_count = no_zone.sum()
                    # Skip 노드 추가
                    process_info["facilities"].append("Skip")
                    process_facility_data["Skip"] = {
                        "inflow": [0] * len(time_df),  # Skip은 고정값
                        "outflow": [0] * len(time_df),
                        "queue_length": [0] * len(time_df),
                        "waiting_time": [0] * len(time_df),
                        "skip_count": skip_count  # 건너뛴 총 인원수 정보 추가
                    }

                # all_zones
                facility_count = max(len(facilities), 1)
                all_zones_data = {
                    "inflow": aggregated["inflow"].astype(int).tolist(),
                    "outflow": aggregated["outflow"].astype(int).tolist(),
                    "queue_length": (aggregated["queue_length"] / facility_count)
                    .round()
                    .astype(int)
                    .tolist(),
                    "waiting_time": (aggregated["waiting_time"] / facility_count)
                    .round()
                    .astype(int)
                    .tolist(),
                }

                if zone_capacity_map:
                    aggregate_capacity = [0.0] * len(time_df.index)
                    for capacity_list in zone_capacity_map.values():
                        aggregate_capacity = [curr + add for curr, add in zip(aggregate_capacity, capacity_list)]
                    all_zones_data["capacity"] = [int(round(value)) for value in aggregate_capacity]

                # process_info에 데이터 추가
                process_info["data"] = {"all_zones": all_zones_data, **process_facility_data}

                # 항공사 이름 매핑 추가
                if airline_name_mapping:
                    process_info["airline_names"] = airline_name_mapping
            else:
                # 이 프로세스에 아무도 가지 않은 경우
                process_info["data"] = {}

            data[process] = process_info
        return data

    def get_facility_details(self):
        """시설 세부 정보 생성"""

        # facility_metrics 계산 (facility_effi, workforce_effi 정보를 위해)
        facility_metrics_list = self._calculate_facility_metrics()

        # process별, zone별 metrics 매핑 생성
        process_metrics_map = {}
        if facility_metrics_list:
            for metric in facility_metrics_list:
                process_name = metric.get('process')
                if process_name == 'total':
                    continue

                # 프로세스 레벨 메트릭 저장
                if process_name not in process_metrics_map:
                    process_metrics_map[process_name] = {
                        'operating_rate': metric.get('operating_rate', 0),
                        'utilization_rate': metric.get('utilization_rate', 0),
                        'zones': metric.get('zones', {})
                    }

        # opened 정보 계산
        opened_counts_map = self._calculate_opened_counts()

        data = []
        for process in self.process_list:
            # 해당 프로세스에서 completed 상태인 승객만 사용
            process_completed_df = self._filter_by_status(self.pax_df, process)

            base_fields = ["zone", "facility", "queue_length", "on_pred", "done_time"]
            wait_fields = [
                suffix
                for suffix in ["queue_wait_time"]
                if f"{process}_{suffix}" in process_completed_df.columns
            ]
            cols = [f"{process}_{field}" for field in base_fields] + [f"{process}_{field}" for field in wait_fields]
            cols = [col for col in cols if col in process_completed_df.columns]
            process_df = process_completed_df[cols].copy()

            # Overview 계산
            waiting_time = self._calculate_waiting_time(process_df, process)

            # 프로세스 레벨 metrics 가져오기
            process_metrics = process_metrics_map.get(process, {})

            # 프로세스 레벨 opened 정보 가져오기
            process_opened_info = opened_counts_map.get(process, {})
            process_opened = process_opened_info.get('opened', 0)
            process_total = process_opened_info.get('total', 0)

            overview = {
                "throughput": len(process_df),
                "queuePax": int(
                    process_df[f"{process}_queue_length"].quantile(1 - self.percentile / 100)
                    if self.percentile is not None
                    else process_df[f"{process}_queue_length"].mean()
                ),
                "waitTime": self._format_waiting_time(
                    waiting_time.quantile(1 - self.percentile / 100)
                    if self.percentile is not None
                    else waiting_time.mean()
                ),
                "facility_effi": process_metrics.get('operating_rate', 0) * 100,  # Facility Effi. = Facility Effi.
                "workforce_effi": process_metrics.get('utilization_rate', 0) * 100,  # Workforce Effi. = Workforce Effi.
                "opened": [process_opened, process_total],  # [운영한 시설 수, 전체 시설 수]
            }

            # Components 계산
            components = []
            zone_metrics_map = process_metrics.get('zones', {})
            zone_opened_map = process_opened_info.get('zones', {})

            for facility in sorted(process_df[f"{process}_zone"].unique()):
                facility_df = process_df[process_df[f"{process}_zone"] == facility]
                waiting_time = self._calculate_waiting_time(facility_df, process)

                # 존 레벨 metrics 가져오기
                zone_metrics = zone_metrics_map.get(facility, {})

                # 존 레벨 opened 정보 가져오기
                zone_opened_info = zone_opened_map.get(facility, {})
                zone_opened = zone_opened_info.get('opened', 0)
                zone_total = zone_opened_info.get('total', 0)

                components.append(
                    {
                        "title": facility,
                        "throughput": len(facility_df),
                        "queuePax": int(
                        facility_df[f"{process}_queue_length"].quantile(
                            1 - self.percentile / 100
                        )
                            if self.percentile is not None
                            else facility_df[f"{process}_queue_length"].mean()
                        ),
                        "waitTime": self._format_waiting_time(
                            waiting_time.quantile(1 - self.percentile / 100)
                            if self.percentile is not None
                            else waiting_time.mean()
                        ),
                        "facility_effi": zone_metrics.get('operating_rate', 0) * 100,  # Facility Effi.
                        "workforce_effi": zone_metrics.get('utilization_rate', 0) * 100,  # Workforce Effi.
                        "opened": [zone_opened, zone_total],  # [운영한 시설 수, 전체 시설 수]
                    }
                )

            data.append(
                {"category": process, "overview": overview, "components": components}
            )
        return data


    def get_histogram_data(self):
        """시설별, 그리고 그 안의 구역별 통계 데이터 생성 (all_zones 포함)"""
        # 상수
        WT_BINS = [0, 15, 30, 45, 60, float("inf")]
        WT_LABELS = [
            "00:00-15:00",
            "15:00-30:00",
            "30:00-45:00",
            "45:00-60:00",
            "60:00-",
        ]
        QL_BINS = [0, 50, 100, 150, 200, 250, float("inf")]
        QL_LABELS = ["0-50", "50-100", "100-150", "150-200", "200-250", "250+"]

        data = {}

        for process in self.process_list:
            # 해당 프로세스에서 completed 상태인 승객만 사용
            process_completed_df = self._filter_by_status(self.pax_df, process)

            facilities = sorted(process_completed_df[f"{process}_zone"].dropna().unique())
            wt_collection, ql_collection = [], []
            facility_data = {}

            for facility in facilities:
                df = process_completed_df[process_completed_df[f"{process}_zone"] == facility].copy()

                # 대기시간 분포 (초를 분으로 변환)
                wt_mins = self._get_waiting_time(df, process).dt.total_seconds() / 60
                wt_bins = self._get_distribution(wt_mins, WT_BINS, WT_LABELS)

                # 대기열 분포
                ql_bins = []
                if f"{process}_queue_length" in df.columns and not df[f"{process}_queue_length"].empty:
                    ql_bins = self._get_distribution(
                        df[f"{process}_queue_length"], QL_BINS, QL_LABELS
                    )

                # 데이터 저장
                short_name = facility.split("_")[-1]
                facility_data[short_name] = {
                    "waiting_time": self._create_bins_data(wt_bins, "min", True),
                    "queue_length": self._create_bins_data(ql_bins, "pax", False),
                }

                if wt_bins:
                    wt_collection.append(wt_bins)
                if ql_bins:
                    ql_collection.append(ql_bins)

            # all_zones 생성
            if wt_collection and ql_collection:
                all_zones = {
                    "waiting_time": self._create_bins_data(
                        self._calc_avg_bins(wt_collection), "min", True
                    ),
                    "queue_length": self._create_bins_data(
                        self._calc_avg_bins(ql_collection), "pax", False
                    ),
                }
                data[process] = {"all_zones": all_zones, **facility_data}
            else:
                data[process] = facility_data

        return data

    def get_sankey_diagram_data(self):
        """산키 다이어그램 데이터 생성 - Completed, Skipped, Failed 모두 표시"""
        # 모든 승객 포함 (completed, skipped, failed)
        all_pax_df = self.pax_df.copy()

        # 각 프로세스별로 zone이 None인 경우 status 값을 zone으로 매핑
        for process in self.process_list:
            zone_col = f"{process}_zone"
            status_col = f"{process}_status"

            if zone_col in all_pax_df.columns and status_col in all_pax_df.columns:
                # zone이 None이고 status가 있는 경우
                mask = all_pax_df[zone_col].isna() & all_pax_df[status_col].notna()

                # status 값을 첫 글자 대문자로 변환하여 zone에 할당
                all_pax_df.loc[mask, zone_col] = all_pax_df.loc[mask, status_col].str.capitalize()

        # operating_carrier_name 컬럼을 첫 번째 레이어로 추가
        target_columns = []

        # 첫 번째 레이어: Airline (operating_carrier_name)
        if "operating_carrier_name" in all_pax_df.columns:
            # operating_carrier_name이 있는 데이터만 필터링
            all_pax_df = all_pax_df[all_pax_df["operating_carrier_name"].notna()].copy()

            # 항공사별 승객 수 카운트
            airline_counts = all_pax_df["operating_carrier_name"].value_counts()

            # 상위 10개 항공사 추출
            top_10_airlines = airline_counts.head(10).index.tolist()

            # 11번째부터는 "ETC"로 변경
            all_pax_df.loc[~all_pax_df["operating_carrier_name"].isin(top_10_airlines), "operating_carrier_name"] = "ETC"

            target_columns.append("operating_carrier_name")

        # zone 기반으로 승객 플로우 생성 (process_list 순서 사용)
        # process_list 순서대로 zone 컬럼 정렬
        zone_target_columns = [f"{process}_zone" for process in self.process_list
                               if f"{process}_zone" in all_pax_df.columns]

        # target_columns에 zone 컬럼들 추가
        target_columns.extend(zone_target_columns)

        # 빈 컬럼 리스트 처리
        if not target_columns:
            return {
                "label": [],
                "link": {"source": [], "target": [], "value": []},
            }

        # 전체 데이터에서 groupby (이제 Skipped, Failed도 포함되므로 dropna=False 사용하지 않음)
        # zone 값이 모두 매핑되었으므로 dropna=True 유지
        flow_df = all_pax_df.groupby(target_columns, dropna=True).size().reset_index(name="count")

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

        # 라벨 생성 시 프로세스명 포함하여 고유하게 만들기
        labels = []
        label_mapping = {}  # 원본 라벨 → 표시용 라벨

        for col in target_columns:
            # operating_carrier_name인 경우 특별 처리
            if col == "operating_carrier_name":
                process_name = "airline"
            else:
                process_name = col.replace("_zone", "")

            for facility in sorted(flow_df[col].unique()):
                # 원본: facility, 표시용: facility (프로세스 정보는 process_info에서 관리)
                label_mapping[facility] = facility
                labels.append(facility)

        # 프로세스 정보 생성 (계층 구조를 위해)
        process_info = {}
        for col in target_columns:
            # operating_carrier_name인 경우 특별 처리
            if col == "operating_carrier_name":
                process_name = "airline"
                display_name = "Airline"
            else:
                process_name = col.replace("_zone", "")
                display_name = process_name.replace("_", " ").title()

            facilities = sorted(flow_df[col].unique())

            # Failed와 Skipped를 제외한 승객 수 계산
            pax_count = flow_df[
                ~flow_df[col].isin(["Failed", "Skipped"])
            ]["count"].sum()

            process_info[process_name] = {
                "process_name": display_name,
                "facilities": facilities,
                "pax_count": int(pax_count)
            }

        return {
            "label": labels,
            "link": {"source": sources, "target": targets, "value": values},
            "process_info": process_info  # 계층 구조 정보 추가
        }

    # ===============================
    # 서브 함수들 (헬퍼 메소드)
    # ===============================

    def _get_process_list(self):
        """프로세스 리스트 추출"""
        return [
            col.replace("_on_pred", "")
            for col in self.pax_df.columns
            if "on_pred" in col
        ]

    def _filter_by_status(self, df: pd.DataFrame, process: str) -> pd.DataFrame:
        """특정 프로세스에서 status가 'completed'인 행만 반환"""
        status_col = f"{process}_status"
        if status_col in df.columns:
            return df[df[status_col] == "completed"].copy()
        # status 컬럼이 없는 경우 원본 반환 (하위 호환성)
        return df.copy()

    def _extract_block_period(self, block: dict) -> Optional[tuple[pd.Timestamp, pd.Timestamp]]:
        """운영 스케줄 블록의 시작/종료 시간 추출"""
        period = block.get("period", "")
        if not period:
            return None

        if len(period) > 19 and period[19] == "-":
            start_str = period[:19]
            end_str = period[20:]
        else:
            parts = period.split(" - ")
            if len(parts) != 2:
                return None
            start_str, end_str = parts

        block_start = pd.to_datetime(start_str.strip(), errors="coerce")
        block_end = pd.to_datetime(end_str.strip(), errors="coerce")
        if pd.isna(block_start) or pd.isna(block_end):
            return None
        return block_start, block_end

    def _calculate_capacity_for_slot(
        self,
        facility_config: dict,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> float:
        """특정 시간 슬롯에 대한 시설 용량 계산"""
        slot_capacity = 0.0
        for block in facility_config.get("operating_schedule", {}).get("time_blocks", []):
            if not block.get("activate", True):
                continue

            period_bounds = self._extract_block_period(block)
            if not period_bounds:
                continue
            block_start, block_end = period_bounds

            if start >= block_end or end <= block_start:
                continue

            overlap_start = max(start, block_start)
            overlap_end = min(end, block_end)
            overlap_minutes = max((overlap_end - overlap_start).total_seconds() / 60.0, 0)
            if overlap_minutes == 0:
                continue

            process_time_seconds = block.get("process_time_seconds")
            if not process_time_seconds:
                continue

            capacity_per_hour = 3600.0 / process_time_seconds
            slot_capacity += (overlap_minutes / 60.0) * capacity_per_hour
        return slot_capacity

    def _calculate_step_capacity_series_by_zone(
        self,
        step_config: dict,
        time_range: pd.DatetimeIndex,
        interval_minutes: int,
    ) -> Dict[str, List[float]]:
        """프로세스 스텝의 zone별 시간대별 용량 계산"""
        zone_capacity: Dict[str, List[float]] = {}
        if time_range.empty:
            return zone_capacity

        for zone_name, zone in step_config.get("zones", {}).items():
            total_capacity = [0.0] * len(time_range)
            for facility in zone.get("facilities", []):
                facility_capacity: List[float] = []
                for start in time_range:
                    end = start + pd.Timedelta(minutes=interval_minutes)
                    facility_capacity.append(
                        self._calculate_capacity_for_slot(facility, start, end)
                    )
                total_capacity = [curr + add for curr, add in zip(total_capacity, facility_capacity)]
            zone_capacity[zone_name] = total_capacity
        return zone_capacity

    def _calculate_step_capacity_series_by_facility(
        self,
        step_config: dict,
        zone_name: str,
        time_range: pd.DatetimeIndex,
        interval_minutes: int,
    ) -> Dict[str, List[float]]:
        """프로세스 스텝의 특정 zone 내 개별 facility별 시간대별 용량 계산"""
        facility_capacity_map: Dict[str, List[float]] = {}
        if time_range.empty:
            return facility_capacity_map

        zone = step_config.get("zones", {}).get(zone_name)
        if not zone:
            return facility_capacity_map

        for facility in zone.get("facilities", []):
            facility_id = facility.get("id")
            if not facility_id:
                continue

            facility_capacity: List[float] = []
            for start in time_range:
                end = start + pd.Timedelta(minutes=interval_minutes)
                facility_capacity.append(
                    self._calculate_capacity_for_slot(facility, start, end)
                )
            facility_capacity_map[facility_id] = facility_capacity

        return facility_capacity_map

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

    def _get_pax_experience_data(self, df, data_type):
        """승객 경험 데이터 계산"""
        df["total"] = df.sum(axis=1)
        if self.percentile is None:
            target_value = df["total"].mean()
        else:
            target_value = np.percentile(df["total"], 100 - self.percentile)

        closest_idx = (df["total"] - target_value).abs().idxmin()
        result_row = df.loc[closest_idx]

        result_dict = {}
        for col in df.columns:
            if data_type == "time":
                result_dict[col] = self._format_waiting_time(result_row[col])
            else:
                result_dict[col] = int(result_row[col])

        return result_dict

    def _create_time_df_index(self, interval_minutes: int):
        """실제 데이터 기반 동적 시간 범위 생성"""
        # 모든 프로세스의 시간 컬럼에서 timestamp 수집
        all_timestamps = []

        for process in self.process_list:
            for col_suffix in ['on_pred', 'start_time', 'done_time']:
                col = f"{process}_{col_suffix}"
                if col in self.pax_df.columns:
                    ts = pd.to_datetime(self.pax_df[col], errors='coerce').dropna()
                    if not ts.empty:
                        all_timestamps.append(ts)

        # 폴백: 시간 데이터가 없으면 show_up_time 기준으로 00:00~23:59 사용
        if not all_timestamps:
            last_date = self.pax_df["show_up_time"].dt.date.unique()[-1]
            time_index = pd.date_range(
                start=f"{last_date} 00:00:00",
                end=f"{last_date} 23:59:59",
                freq=f"{interval_minutes}min"
            )
            return pd.DataFrame(index=time_index)

        # 실제 데이터의 최소/최대 시간으로 범위 설정
        min_ts = min(s.min() for s in all_timestamps).floor(f"{interval_minutes}min")
        max_ts = max(s.max() for s in all_timestamps).ceil(f"{interval_minutes}min")
        time_index = pd.date_range(min_ts, max_ts, freq=f"{interval_minutes}min")
        return pd.DataFrame(index=time_index)

    def _build_process_flow_map(self, process_flow: Optional[List[dict]]) -> Dict[str, dict]:
        if not process_flow:
            return {}
        mapping: Dict[str, dict] = {}
        for step in process_flow:
            name = step.get("name")
            if name:
                mapping[name] = step
        return mapping

    def _get_waiting_time(self, df, process):
        """순수 queue 대기시간 반환"""
        queue_col = f"{process}_queue_wait_time"

        if queue_col in df.columns:
            queue_series = pd.to_timedelta(df[queue_col])
            return queue_series

        return pd.Series(pd.NaT, index=df.index, dtype="timedelta64[ns]")

    def _get_open_wait_time(self, df, process):
        """Open wait 대기시간 반환 (시설이 열리기를 기다리는 시간)"""
        open_col = f"{process}_open_wait_time"

        if open_col in df.columns:
            open_series = pd.to_timedelta(df[open_col])
            return open_series

        return pd.Series(pd.NaT, index=df.index, dtype="timedelta64[ns]")

    def _get_total_wait_time(self, df, process):
        """Total wait time = open_wait + queue_wait"""
        open_wait = self._get_open_wait_time(df, process)
        queue_wait = self._get_waiting_time(df, process)
        return open_wait + queue_wait


    def _calculate_waiting_time(self, process_df, process):
        """대기 시간 계산"""
        return self._get_waiting_time(process_df, process)







    def _get_distribution(self, values, bins, labels):
        """값들의 분포를 백분율로 계산"""
        if values.empty:
            return []
        groups = pd.cut(values, bins=bins, labels=labels, right=False)
        counts = groups.value_counts().reindex(labels, fill_value=0)
        total = counts.sum()
        percentages = ((counts / total) * 100).round(0) if total > 0 else counts
        return [
            {"title": label, "value": int(percentages[label]), "unit": "%"}
            for label in labels
        ]

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
            "bins": [
                {
                    "range": self._parse_range(item["title"], is_time),
                    "value": item["value"],
                }
                for item in bins_list
            ],
        }

    def _calc_avg_bins(self, bins_collection):
        """여러 시설의 평균 계산"""
        if not bins_collection:
            return []
        agg = {}
        for bin_list in bins_collection:
            for item in bin_list:
                agg.setdefault(item["title"], []).append(item["value"])
        return [
            {
                "title": item["title"],
                "value": int(round(np.mean(agg.get(item["title"], [0])))),
            }
            for item in bins_collection[0]
        ]

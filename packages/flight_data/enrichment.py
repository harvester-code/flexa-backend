"""
항공편 데이터 보강 모듈 (Flight Data Enrichment)

Snowflake MASTER_LOCATION_TRIAL은 Trial 뷰로 데이터가 제한적.
매핑 누락 시 국가코드(KR,JP)가 그대로 노출되고, 지역(region)은 NULL로 남는 문제를 해결.

이 모듈은 Snowflake 쿼리 결과를 후처리하여:
  1. 항공사 코드 → 항공사명: packages/oag_ref.xlsx (Airline Code)에서 비행일(flight_date) 기준 유효 행 매칭
  2. 엑셀에 없을 때만 CARRIER_CODE_NAME 및 스케줄의 SAD_NAME 기반 보조 매핑
  3. 엑셀 매칭 실패 시 ALL CAPS 항공사명 → title case 정규화 (ASIANA AIRLINES → Asiana Airlines)
  4. 2글자 국가코드 → 국가명 변환 (CN → China)
  5. MASTER 테이블의 ALL CAPS 국가명 → 정규화 (JAPAN → Japan)
  6. 국가 기반 지역(region) 자동 할당 (Japan → Asia)
  7. 국가코드(country_code) 역매핑 보정
"""

import re
from datetime import date
from typing import Dict, List, Optional, Tuple
from loguru import logger

from .oag_airline_reference import lookup_airline_name, parse_flight_date
from .oag_aircraft_reference import lookup_aircraft_name
from .oag_airport_reference import lookup_airport


# ============================================================
# 항공사 코드 → 항공사명 매핑
# ============================================================
# IATA 2-letter carrier code → 표시명
# MASTER_CARRIER_TRIAL에 없는 항공사를 보완
CARRIER_CODE_NAME: Dict[str, str] = {
    # ── Korean carriers ──
    "KE": "Korean Air",
    "OZ": "Asiana Airlines",
    "7C": "Jeju Air",
    "LJ": "Jin Air",
    "TW": "T'way Air",
    "ZE": "Eastar Jet",
    "RS": "Air Seoul",
    "BX": "Air Busan",
    "4V": "Fly Gangwon",
    "RF": "Aero K",
    "4H": "Hi Air",
    "YP": "Air Premia",

    # ── Chinese carriers ──
    "CA": "Air China",
    "MU": "China Eastern Airlines",
    "CZ": "China Southern Airlines",
    "HU": "Hainan Airlines",
    "ZH": "Shenzhen Airlines",
    "MF": "Xiamen Airlines",
    "SC": "Shandong Airlines",
    "FM": "Shanghai Airlines",
    "3U": "Sichuan Airlines",
    "9C": "Spring Airlines",
    "GS": "Tianjin Airlines",
    "GJ": "Loong Air",
    "TV": "Tibet Airlines",
    "BK": "Okay Airways",
    "PN": "West Air",
    "G5": "China Express Airlines",
    "NS": "Hebei Airlines",
    "HO": "Juneyao Airlines",
    "8L": "Lucky Air",
    "DR": "Ruili Airlines",
    "EU": "Chengdu Airlines",

    # ── Japanese carriers ──
    "NH": "All Nippon Airways",
    "JL": "Japan Airlines",
    "BC": "Skymark Airlines",
    "MM": "Peach Aviation",
    "GK": "Jetstar Japan",
    "7G": "Star Flyer",
    "IJ": "Spring Japan",
    "HD": "Airdo",
    "NU": "Japan Transocean Air",

    # ── Southeast Asian carriers ──
    "SQ": "Singapore Airlines",
    "TR": "Scoot",
    "3K": "Jetstar Asia",
    "TG": "Thai Airways",
    "WE": "Thai Smile",
    "VZ": "Thai Vietjet Air",
    "SL": "Thai Lion Air",
    "FD": "Thai AirAsia",
    "VN": "Vietnam Airlines",
    "VJ": "Vietjet Air",
    "QH": "Bamboo Airways",
    "5J": "Cebu Pacific",
    "PR": "Philippine Airlines",
    "Z2": "AirAsia Philippines",
    "MH": "Malaysia Airlines",
    "AK": "AirAsia",
    "D7": "AirAsia X",
    "OD": "Batik Air Malaysia",
    "GA": "Garuda Indonesia",
    "QZ": "Indonesia AirAsia",
    "JT": "Lion Air",
    "ID": "Batik Air",
    "K6": "Cambodia Angkor Air",
    "QV": "Lao Airlines",
    "8M": "Myanmar Airways International",
    "UB": "Myanmar National Airlines",
    "BI": "Royal Brunei Airlines",
    "MI": "SilkAir",

    # ── Chinese Taipei / Hong Kong / Macao ──
    "CI": "China Airlines",
    "BR": "EVA Air",
    "IT": "Tigerair Taiwan",
    "B7": "Uni Air",
    "ZV": "Mandarin Airlines",
    "CX": "Cathay Pacific",
    "UO": "Hong Kong Express",
    "HX": "Hong Kong Airlines",
    "NX": "Air Macau",

    # ── Middle Eastern carriers ──
    "EK": "Emirates",
    "QR": "Qatar Airways",
    "EY": "Etihad Airways",
    "SV": "Saudi Arabian Airlines",
    "GF": "Gulf Air",
    "WY": "Oman Air",
    "RJ": "Royal Jordanian",
    "ME": "Middle East Airlines",
    "KU": "Kuwait Airways",
    "PC": "Pegasus Airlines",

    # ── European carriers ──
    "LH": "Lufthansa",
    "AF": "Air France",
    "BA": "British Airways",
    "KL": "KLM Royal Dutch Airlines",
    "TK": "Turkish Airlines",
    "SK": "Scandinavian Airlines",
    "AY": "Finnair",
    "LO": "LOT Polish Airlines",
    "LX": "Swiss International Air Lines",
    "OS": "Austrian Airlines",
    "SN": "Brussels Airlines",
    "IB": "Iberia",
    "AZ": "ITA Airways",
    "EI": "Aer Lingus",
    "TP": "TAP Air Portugal",
    "SU": "Aeroflot",
    "S7": "S7 Airlines",
    "OK": "Czech Airlines",

    # ── North American carriers ──
    "AA": "American Airlines",
    "DL": "Delta Air Lines",
    "UA": "United Airlines",
    "AC": "Air Canada",
    "WS": "WestJet",
    "AS": "Alaska Airlines",
    "HA": "Hawaiian Airlines",
    "WN": "Southwest Airlines",
    "B6": "JetBlue Airways",
    "NK": "Spirit Airlines",
    "F9": "Frontier Airlines",
    "AM": "Aeromexico",

    # ── Oceanian carriers ──
    "QF": "Qantas",
    "JQ": "Jetstar Airways",
    "NZ": "Air New Zealand",
    "FJ": "Fiji Airways",
    "VA": "Virgin Australia",

    # ── African carriers ──
    "ET": "Ethiopian Airlines",
    "SA": "South African Airways",
    "MS": "EgyptAir",
    "AT": "Royal Air Maroc",
    "KQ": "Kenya Airways",

    # ── Central Asian carriers ──
    "HY": "Uzbekistan Airways",
    "KC": "Air Astana",
    "DV": "Scat Airlines",
    "QR": "Qatar Airways",

    # ── Other ──
    "AI": "Air India",
    "C6": "Cebgo",
    "XJ": "Thai AirAsia X",
    "QW": "Qingdao Airlines",
    "OM": "Miat Mongolian Airlines",
    "ZA": "Sky Angkor Airlines",
    "ZG": "Gol Linhas Aereas",
}


# title case 변환 시 소문자 유지해야 하는 단어 (전치사, 관사 등)
_LOWERCASE_WORDS = {"of", "the", "and", "de", "du", "des", "la", "le", "al", "el", "da", "do"}

# title case 변환 시 대문자 유지해야 하는 약어/고유명사
_UPPERCASE_WORDS = {"KLM", "TAP", "SAS", "LOT", "ITA", "EVA"}


def _normalize_carrier_name(name: str) -> str:
    """ALL CAPS 항공사명을 title case로 변환 (첫 글자 + 스페이스 뒤 첫 글자만 대문자)."""
    if not name or not isinstance(name, str):
        return name

    # 이미 mixed case면 그대로 반환 (수동 매핑에서 온 값)
    if name != name.upper():
        return name

    words = name.split()
    result = []
    for i, word in enumerate(words):
        if word in _UPPERCASE_WORDS:
            result.append(word)
        elif i > 0 and word.lower() in _LOWERCASE_WORDS:
            result.append(word.lower())
        else:
            result.append(word.capitalize())
    return " ".join(result)


# ISO 3166-1 alpha-2 → (표시명, 지역)
# 표시명은 배포 서비스(PostgreSQL)의 기존 표기와 일치시킴
COUNTRY_CODE_INFO: Dict[str, Tuple[str, str]] = {
    # ── Asia ──
    "KR": ("Korea", "Asia"),
    "JP": ("Japan", "Asia"),
    "CN": ("China", "Asia"),
    "TW": ("Chinese Taipei", "Asia"),
    "HK": ("Hong Kong (sar) China", "Asia"),
    "MO": ("Macao (sar) China", "Asia"),
    "MN": ("Mongolia", "Asia"),
    "VN": ("Viet Nam", "Asia"),
    "TH": ("Thailand", "Asia"),
    "PH": ("Philippines", "Asia"),
    "SG": ("Singapore", "Asia"),
    "MY": ("Malaysia", "Asia"),
    "ID": ("Indonesia", "Asia"),
    "KH": ("Cambodia", "Asia"),
    "LA": ("Laos", "Asia"),
    "MM": ("Myanmar", "Asia"),
    "BN": ("Brunei", "Asia"),
    "TL": ("Timor-Leste", "Asia"),
    "IN": ("India", "Asia"),
    "LK": ("Sri Lanka", "Asia"),
    "BD": ("Bangladesh", "Asia"),
    "NP": ("Nepal", "Asia"),
    "PK": ("Pakistan", "Asia"),
    "MV": ("Maldives", "Asia"),
    "UZ": ("Uzbekistan", "Asia"),
    "KZ": ("Kazakhstan", "Asia"),
    "KG": ("Kyrgyzstan", "Asia"),
    "TJ": ("Tajikistan", "Asia"),
    "TM": ("Turkmenistan", "Asia"),

    # ── Oceania ──
    "AU": ("Australia", "Oceania"),
    "NZ": ("New Zealand", "Oceania"),
    "FJ": ("Fiji", "Oceania"),
    "PG": ("Papua New Guinea", "Oceania"),
    "GU": ("Guam", "Oceania"),
    "NC": ("New Caledonia", "Oceania"),
    "PF": ("French Polynesia", "Oceania"),
    "WS": ("Samoa", "Oceania"),
    "TO": ("Tonga", "Oceania"),
    "VU": ("Vanuatu", "Oceania"),
    "MP": ("Northern Mariana Islands", "Oceania"),
    "PW": ("Palau", "Oceania"),

    # ── Middle East ──
    "AE": ("United Arab Emirates", "Middle East"),
    "SA": ("Saudi Arabia", "Middle East"),
    "QA": ("Qatar", "Middle East"),
    "OM": ("Oman", "Middle East"),
    "BH": ("Bahrain", "Middle East"),
    "KW": ("Kuwait", "Middle East"),
    "IL": ("Israel", "Middle East"),
    "JO": ("Jordan", "Middle East"),
    "LB": ("Lebanon", "Middle East"),
    "IQ": ("Iraq", "Middle East"),
    "IR": ("Iran", "Middle East"),
    "YE": ("Yemen", "Middle East"),

    # ── Europe ──
    "GB": ("United Kingdom", "Europe"),
    "FR": ("France", "Europe"),
    "DE": ("Germany", "Europe"),
    "IT": ("Italy", "Europe"),
    "ES": ("Spain", "Europe"),
    "PT": ("Portugal", "Europe"),
    "NL": ("Netherlands", "Europe"),
    "BE": ("Belgium", "Europe"),
    "CH": ("Switzerland", "Europe"),
    "AT": ("Austria", "Europe"),
    "SE": ("Sweden", "Europe"),
    "NO": ("Norway", "Europe"),
    "DK": ("Denmark", "Europe"),
    "FI": ("Finland", "Europe"),
    "IE": ("Ireland", "Europe"),
    "PL": ("Poland", "Europe"),
    "CZ": ("Czech Republic", "Europe"),
    "HU": ("Hungary", "Europe"),
    "RO": ("Romania", "Europe"),
    "GR": ("Greece", "Europe"),
    "TR": ("Turkey", "Europe"),
    "RU": ("Russia", "Europe"),
    "UA": ("Ukraine", "Europe"),
    "HR": ("Croatia", "Europe"),
    "BG": ("Bulgaria", "Europe"),
    "RS": ("Serbia", "Europe"),
    "SK": ("Slovakia", "Europe"),
    "SI": ("Slovenia", "Europe"),
    "LT": ("Lithuania", "Europe"),
    "LV": ("Latvia", "Europe"),
    "EE": ("Estonia", "Europe"),
    "IS": ("Iceland", "Europe"),
    "LU": ("Luxembourg", "Europe"),
    "MT": ("Malta", "Europe"),
    "CY": ("Cyprus", "Europe"),
    "GE": ("Georgia", "Europe"),
    "AM": ("Armenia", "Europe"),
    "AZ": ("Azerbaijan", "Europe"),
    "BY": ("Belarus", "Europe"),
    "MD": ("Moldova", "Europe"),
    "AL": ("Albania", "Europe"),
    "ME": ("Montenegro", "Europe"),
    "MK": ("North Macedonia", "Europe"),
    "BA": ("Bosnia and Herzegovina", "Europe"),
    "XK": ("Kosovo", "Europe"),

    # ── North America ──
    "US": ("United States", "North America"),
    "CA": ("Canada", "North America"),
    "MX": ("Mexico", "North America"),

    # ── Latin America ──
    "BR": ("Brazil", "Latin America"),
    "AR": ("Argentina", "Latin America"),
    "CL": ("Chile", "Latin America"),
    "CO": ("Colombia", "Latin America"),
    "PE": ("Peru", "Latin America"),
    "VE": ("Venezuela", "Latin America"),
    "EC": ("Ecuador", "Latin America"),
    "BO": ("Bolivia", "Latin America"),
    "PY": ("Paraguay", "Latin America"),
    "UY": ("Uruguay", "Latin America"),
    "CR": ("Costa Rica", "Latin America"),
    "PA": ("Panama", "Latin America"),
    "CU": ("Cuba", "Latin America"),
    "DO": ("Dominican Republic", "Latin America"),
    "GT": ("Guatemala", "Latin America"),
    "HN": ("Honduras", "Latin America"),
    "SV": ("El Salvador", "Latin America"),
    "NI": ("Nicaragua", "Latin America"),
    "JM": ("Jamaica", "Latin America"),
    "TT": ("Trinidad and Tobago", "Latin America"),
    "HT": ("Haiti", "Latin America"),
    "BS": ("Bahamas", "Latin America"),
    "BB": ("Barbados", "Latin America"),
    "PR": ("Puerto Rico", "Latin America"),
    "AW": ("Aruba", "Latin America"),
    "CW": ("Curacao", "Latin America"),
    "BZ": ("Belize", "Latin America"),
    "SR": ("Suriname", "Latin America"),
    "GY": ("Guyana", "Latin America"),

    # ── Africa ──
    "ZA": ("South Africa", "Africa"),
    "EG": ("Egypt", "Africa"),
    "MA": ("Morocco", "Africa"),
    "KE": ("Kenya", "Africa"),
    "ET": ("Ethiopia", "Africa"),
    "NG": ("Nigeria", "Africa"),
    "TZ": ("Tanzania", "Africa"),
    "GH": ("Ghana", "Africa"),
    "TN": ("Tunisia", "Africa"),
    "DZ": ("Algeria", "Africa"),
    "MU": ("Mauritius", "Africa"),
    "SN": ("Senegal", "Africa"),
    "CI": ("Cote d'Ivoire", "Africa"),
    "CM": ("Cameroon", "Africa"),
    "UG": ("Uganda", "Africa"),
    "RW": ("Rwanda", "Africa"),
    "MG": ("Madagascar", "Africa"),
    "LY": ("Libya", "Africa"),
    "AO": ("Angola", "Africa"),
    "MZ": ("Mozambique", "Africa"),
    "NA": ("Namibia", "Africa"),
    "BW": ("Botswana", "Africa"),
    "ZW": ("Zimbabwe", "Africa"),
    "SC": ("Seychelles", "Africa"),
    "RE": ("Reunion", "Africa"),
}

# MASTER_LOCATION_TRIAL OAG_COUNTRY_NAME (ALL CAPS) → 국가코드 역매핑
# COALESCE가 MASTER에서 이름을 가져왔을 때 정규화용
_MASTER_NAME_TO_CODE: Dict[str, str] = {
    "JAPAN": "JP",
    "CHINA": "CN",
    "KOREA, REPUBLIC OF": "KR",
    "KOREA REPUBLIC OF": "KR",
    "SINGAPORE": "SG",
    "THAILAND": "TH",
    "VIET NAM": "VN",
    "VIETNAM": "VN",
    "PHILIPPINES": "PH",
    "INDONESIA": "ID",
    "MALAYSIA": "MY",
    "CAMBODIA": "KH",
    "MYANMAR": "MM",
    "BRUNEI DARUSSALAM": "BN",
    "INDIA": "IN",
    "SRI LANKA": "LK",
    "BANGLADESH": "BD",
    "NEPAL": "NP",
    "PAKISTAN": "PK",
    "MALDIVES": "MV",
    "MONGOLIA": "MN",
    "CHINESE TAIPEI": "TW",
    "HONG KONG (SAR), CHINA": "HK",
    "HONG KONG SAR CHINA": "HK",
    "HONG KONG": "HK",
    "MACAO (SAR), CHINA": "MO",
    "MACAO SAR CHINA": "MO",
    "MACAU": "MO",
    "UZBEKISTAN": "UZ",
    "KAZAKHSTAN": "KZ",
    "KYRGYZSTAN": "KG",
    "TAJIKISTAN": "TJ",
    "TURKMENISTAN": "TM",
    "LAOS": "LA",
    "LAO PEOPLE'S DEMOCRATIC REPUBLIC": "LA",
    "TIMOR-LESTE": "TL",
    "AUSTRALIA": "AU",
    "NEW ZEALAND": "NZ",
    "FIJI": "FJ",
    "PAPUA NEW GUINEA": "PG",
    "GUAM": "GU",
    "PALAU": "PW",
    "UNITED ARAB EMIRATES": "AE",
    "SAUDI ARABIA": "SA",
    "QATAR": "QA",
    "OMAN": "OM",
    "BAHRAIN": "BH",
    "KUWAIT": "KW",
    "ISRAEL": "IL",
    "JORDAN": "JO",
    "LEBANON": "LB",
    "IRAQ": "IQ",
    "IRAN": "IR",
    "IRAN, ISLAMIC REPUBLIC OF": "IR",
    "UNITED KINGDOM": "GB",
    "FRANCE": "FR",
    "GERMANY": "DE",
    "ITALY": "IT",
    "SPAIN": "ES",
    "PORTUGAL": "PT",
    "NETHERLANDS": "NL",
    "BELGIUM": "BE",
    "SWITZERLAND": "CH",
    "AUSTRIA": "AT",
    "SWEDEN": "SE",
    "NORWAY": "NO",
    "DENMARK": "DK",
    "FINLAND": "FI",
    "IRELAND": "IE",
    "POLAND": "PL",
    "CZECH REPUBLIC": "CZ",
    "CZECHIA": "CZ",
    "HUNGARY": "HU",
    "ROMANIA": "RO",
    "GREECE": "GR",
    "TURKEY": "TR",
    "RUSSIA": "RU",
    "RUSSIAN FEDERATION": "RU",
    "UKRAINE": "UA",
    "CROATIA": "HR",
    "BULGARIA": "BG",
    "SERBIA": "RS",
    "GEORGIA": "GE",
    "ARMENIA": "AM",
    "AZERBAIJAN": "AZ",
    "UNITED STATES OF AMERICA": "US",
    "UNITED STATES": "US",
    "CANADA": "CA",
    "MEXICO": "MX",
    "BRAZIL": "BR",
    "ARGENTINA": "AR",
    "CHILE": "CL",
    "COLOMBIA": "CO",
    "PERU": "PE",
    "SOUTH AFRICA": "ZA",
    "EGYPT": "EG",
    "MOROCCO": "MA",
    "KENYA": "KE",
    "ETHIOPIA": "ET",
    "NIGERIA": "NG",
    "NEW CALEDONIA": "NC",
    "FRENCH POLYNESIA": "PF",
    "NORTHERN MARIANA ISLANDS": "MP",
    "YEMEN": "YE",
    "ICELAND": "IS",
    "LUXEMBOURG": "LU",
    "MALTA": "MT",
    "CYPRUS": "CY",
    "BELARUS": "BY",
    "MOLDOVA": "MD",
    "MOLDOVA, REPUBLIC OF": "MD",
    "ALBANIA": "AL",
    "MONTENEGRO": "ME",
    "NORTH MACEDONIA": "MK",
    "BOSNIA AND HERZEGOVINA": "BA",
    "LITHUANIA": "LT",
    "LATVIA": "LV",
    "ESTONIA": "EE",
    "SLOVAKIA": "SK",
    "SLOVENIA": "SI",
    "COSTA RICA": "CR",
    "PANAMA": "PA",
    "CUBA": "CU",
    "DOMINICAN REPUBLIC": "DO",
    "PUERTO RICO": "PR",
    "JAMAICA": "JM",
    "TRINIDAD AND TOBAGO": "TT",
    "BAHAMAS": "BS",
    "SEYCHELLES": "SC",
    "MAURITIUS": "MU",
    "SAMOA": "WS",
    "TONGA": "TO",
    "VANUATU": "VU",
}


def _resolve_country(value: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    국가 값을 (표시명, 국가코드, 지역)으로 변환.

    입력 가능한 형태:
      - 2글자 코드: "JP", "CN"  (MASTER에 없어서 COALESCE가 원본 코드를 반환)
      - ALL CAPS 이름: "JAPAN"  (MASTER에서 OAG_COUNTRY_NAME을 반환)
      - 정상 이름: "Japan"       (이미 정규화된 경우)

    Returns:
        (display_name, country_code, region)
    """
    if not value or not isinstance(value, str):
        return (value, None, None)

    stripped = value.strip()

    # Case 1: 2글자 국가코드
    if len(stripped) == 2 and stripped.upper() in COUNTRY_CODE_INFO:
        code = stripped.upper()
        name, region = COUNTRY_CODE_INFO[code]
        return (name, code, region)

    # Case 2: MASTER 테이블의 ALL CAPS 이름
    upper = stripped.upper()
    if upper in _MASTER_NAME_TO_CODE:
        code = _MASTER_NAME_TO_CODE[upper]
        name, region = COUNTRY_CODE_INFO[code]
        return (name, code, region)

    # Case 3: 이미 정규화된 이름인지 확인 (title case 등)
    for code, (name, region) in COUNTRY_CODE_INFO.items():
        if name.upper() == upper:
            return (name, code, region)

    # Fallback: 원본 유지, region 없음
    return (stripped, None, None)


def _resolve_carrier(code: Optional[str], name: Optional[str]) -> Optional[str]:
    """
    항공사명 보강.

    우선순위:
      1. CARRIER_CODE_NAME 매핑에서 찾기 (가장 정확)
      2. MASTER에서 온 ALL CAPS 이름 → title case 정규화
      3. 원본 그대로 반환
    """
    if code and code.strip() in CARRIER_CODE_NAME:
        return CARRIER_CODE_NAME[code.strip()]

    if name and isinstance(name, str) and name.strip():
        return _normalize_carrier_name(name.strip())

    return name


def enrich_flight_data(flights: List[dict]) -> List[dict]:
    """
    항공편 데이터 리스트의 항공사명/국가명/지역을 보강.

    처리 대상 필드:
      - operating_carrier_name → 항공사명 매핑 + title case 정규화
      - departure_country → 정규화된 국가명
      - departure_country_code → 2글자 코드 보정
      - departure_region → 지역 할당
      - arrival_country / arrival_country_code / arrival_region → 동일 처리
    """
    enriched_count = 0

    for flight in flights:
        changed = False

        # 항공사명 보강 (OAG 엑셀 비행일 기준 → 기존 fallback)
        carrier_code = flight.get("marketing_carrier_iata") or flight.get("operating_carrier_iata")
        carrier_name_raw = flight.get("operating_carrier_name")
        as_of = parse_flight_date(flight.get("flight_date")) or date.today()
        oag_name = lookup_airline_name(carrier_code, as_of)
        resolved_name = oag_name or _resolve_carrier(carrier_code, carrier_name_raw)
        if resolved_name and resolved_name != carrier_name_raw:
            flight["operating_carrier_name"] = resolved_name
            changed = True

        # 항공기종명 보강 (OAG 엑셀 Aircraft Code)
        acft_iata = flight.get("aircraft_type_iata")
        if acft_iata:
            acft_name = lookup_aircraft_name(acft_iata, as_of)
            if acft_name:
                flight["aircraft_type_name"] = acft_name
                changed = True

        # 출발지 보강 (OAG 엑셀 공항 코드 → 기존 _resolve_country fallback)
        dep_iata = flight.get("departure_airport_iata")
        dep_oag = lookup_airport(dep_iata, as_of) if dep_iata else None
        if dep_oag:
            if dep_oag.city:
                flight["departure_city"] = dep_oag.city
                changed = True
            if dep_oag.country:
                resolved = _resolve_country(dep_oag.country)
                flight["departure_country"] = resolved[0] or dep_oag.country
                flight["departure_country_code"] = resolved[1]
                changed = True
            if dep_oag.region:
                flight["departure_region"] = dep_oag.region
                changed = True
        else:
            dep_country_raw = flight.get("departure_country")
            if dep_country_raw:
                dep_name, dep_code, dep_region = _resolve_country(dep_country_raw)
                if dep_name and dep_name != dep_country_raw:
                    flight["departure_country"] = dep_name
                    changed = True
                if dep_code:
                    flight["departure_country_code"] = dep_code
                if dep_region:
                    flight["departure_region"] = dep_region
                    changed = True

        # 도착지 보강 (OAG 엑셀 공항 코드 → 기존 _resolve_country fallback)
        arr_iata = flight.get("arrival_airport_iata")
        arr_oag = lookup_airport(arr_iata, as_of) if arr_iata else None
        if arr_oag:
            if arr_oag.city:
                flight["arrival_city"] = arr_oag.city
                changed = True
            if arr_oag.country:
                resolved = _resolve_country(arr_oag.country)
                flight["arrival_country"] = resolved[0] or arr_oag.country
                flight["arrival_country_code"] = resolved[1]
                changed = True
            if arr_oag.region:
                flight["arrival_region"] = arr_oag.region
                changed = True
        else:
            arr_country_raw = flight.get("arrival_country")
            if arr_country_raw:
                arr_name, arr_code, arr_region = _resolve_country(arr_country_raw)
                if arr_name and arr_name != arr_country_raw:
                    flight["arrival_country"] = arr_name
                    changed = True
                if arr_code:
                    flight["arrival_country_code"] = arr_code
                if arr_region:
                    flight["arrival_region"] = arr_region
                    changed = True

        if changed:
            enriched_count += 1

    if enriched_count > 0:
        logger.info(f"Flight data enrichment: {enriched_count}/{len(flights)} flights updated")

    return flights

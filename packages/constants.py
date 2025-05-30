ALLOW_ORIGINS_MAP = {
    "local": ["*"],
    "dev": ["https://preview.flexa.expert", "http://localhost:3000"],
    "prod": ["https://www.flexa.expert"],
}

API_PREFIX = "/api/v1"

COL_FILTER_MAP = {
    "International/Domestic": "flight_type",
    "I/D": "flight_type",
    "Airline": "operating_carrier_iata",
    "Region": "region_name",
    "Country": "country_code",
    "international/domestic": "flight_type",
    "i/d": "flight_type",
    "airline": "operating_carrier_iata",
    "region": "region_name",
    "country": "country_code",
    "operating_carrier_name": "operating_carrier_name",
}

CRITERIA_MAP = {
    "operating_carrier_name": "Airline",
    "departure_terminal": "Terminal",
    "flight_type": "I/D",
    "country_code": "Country",
    "region_name": "Region",
}

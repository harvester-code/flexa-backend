from packages.secrets import get_secret

ALLOW_ORIGINS_MAP = {
    "local": ["*"],
    "dev": [
        "https://preview.flexa.expert",
        "http://localhost:3943",
    ],
    "prod": [
        "https://www.flexa.expert",
        "http://localhost:3943",
    ],
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
    "Flight": "flight_number",
    "flight": "flight_number",
}

CRITERIA_MAP = {
    "operating_carrier_name": "airline",
    "departure_terminal": "terminal",
    "flight_type": "type",
    "country_code": "country",
    "region_name": "region",
}

S3_BUCKET_NAME = get_secret("AWS_S3_BUCKET_NAME")

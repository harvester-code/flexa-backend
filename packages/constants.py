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


S3_BUCKET_NAME = get_secret("AWS_S3_BUCKET_NAME")

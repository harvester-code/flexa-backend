import json
import os


def get_secret(key):
    if "DOPPLER_SECRETS" in os.environ:
        return json.loads(os.environ["DOPPLER_SECRETS"])[key]
    return os.environ[key]

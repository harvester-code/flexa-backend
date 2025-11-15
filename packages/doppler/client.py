import json
import os


def get_secret(key, default=None):
    """Doppler secrets에서 환경변수를 가져옵니다.
    
    Args:
        key: 환경변수 키
        default: 키가 없을 때 반환할 기본값 (선택)
    
    Returns:
        환경변수 값 또는 기본값
    """
    try:
        if "DOPPLER_SECRETS" in os.environ:
            secrets = json.loads(os.environ["DOPPLER_SECRETS"])
            return secrets.get(key, default) if default is not None else secrets[key]
        return os.environ.get(key, default) if default is not None else os.environ[key]
    except (KeyError, json.JSONDecodeError):
        if default is not None:
            return default
        raise

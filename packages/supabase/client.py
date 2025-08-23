from supabase import create_client

from packages.doppler.client import get_secret


def get_supabase_client():
    url = get_secret("SUPABASE_PROJECT_URL")
    key = get_secret("SUPABASE_PUBLIC_KEY")

    if not url or not key:
        raise ValueError(
            "Supabase project URL and public key must be set in environment variables."
        )

    # Supabase 클라이언트 생성
    return create_client(url, key)

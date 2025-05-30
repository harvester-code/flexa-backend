import os

from supabase import create_client


def get_supabase_client():
    url = os.getenv("SUPABASE_PROJECT_URL")
    key = os.getenv("SUPABASE_PUBLIC_KEY")

    if not url or not key:
        raise ValueError(
            "Supabase project URL and public key must be set in environment variables."
        )

    # Supabase 클라이언트 생성
    return create_client(url, key)

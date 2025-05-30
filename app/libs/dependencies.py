from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from packages.supabase.auth import decode_supabase_token

security = HTTPBearer()


async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify the token for testing at the "/docs" endpoint.

    Args:
        credentials (HTTPAuthorizationCredentials, optional): The credentials from the request header.
    """

    return decode_supabase_token(credentials.credentials)

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from supabase import create_client, Client
import os

SUPABASE_JWT_SECRET_KEY = os.getenv("SUPABASE_JWT_SECRET_KEY")
ALGORITHM = "HS256"
AUDIENCE = "authenticated"

security = HTTPBearer()

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Supabase 클라이언트 생성
        supabase: Client = create_client(
            os.getenv("SUPABASE_PROJECT_URL"),
            os.getenv("SUPABASE_PUBLIC_KEY")
        )
        
        # 토큰 검증
        user = supabase.auth.get_user(credentials.credentials)
        if not user:
            raise credentials_exception
            
        return user.user.id
        
    except Exception as e:
        raise credentials_exception 
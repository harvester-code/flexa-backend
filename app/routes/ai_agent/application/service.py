import os
import aiohttp
from typing import List, Dict, Any

from fastapi import HTTPException, status
from loguru import logger

from app.routes.ai_agent.interface.schema import Message


class AIAgentService:
    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.base_url = "https://api.openai.com/v1"
        self.local_ai_base_url = os.getenv("LOCAL_AI_BASE_URL", "http://127.0.0.1:8355/v1")

    async def chat(
        self,
        messages: List[Message],
        model: str = "gpt-4",
        max_tokens: int = 1024,
        temperature: float = 1.0,
    ) -> Dict[str, Any]:
        """
        OpenAI API를 호출하여 대화를 생성합니다.
        
        Args:
            messages: 대화 히스토리
            model: 사용할 OpenAI 모델
            max_tokens: 생성할 최대 토큰 수
            temperature: 응답의 창의성 조절
            
        Returns:
            OpenAI API 응답
        """
        if not self.api_key:
            logger.error("OPENAI_API_KEY is not set in environment variables")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="OPENAI_API_KEY must be set in .env file"
            )
        
        try:
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            }
            
            payload = {
                "model": model,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": [msg.model_dump() for msg in messages],
            }
            
            logger.info(f"Calling OpenAI API with model: {model}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"OpenAI API error: {error_text}")
                        raise HTTPException(
                            status_code=status.HTTP_502_BAD_GATEWAY,
                            detail=f"OpenAI API returned error: {error_text}"
                        )
                    
                    result = await response.json()
                    logger.info(f"OpenAI API call successful. Tokens used: {result.get('usage', {})}")
                    return result
                    
        except aiohttp.ClientError as e:
            logger.error(f"Network error calling OpenAI API: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to connect to OpenAI API: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Unexpected error in chat: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {str(e)}"
            )

    async def chat_local(
        self,
        messages: List[Message],
        model: str = "openai/gpt-oss-120b",
        max_tokens: int = 1024,
        temperature: float = 1.0,
    ) -> Dict[str, Any]:
        """
        로컬 DGX Spark AI 서버(TRT-LLM)를 호출하여 대화를 생성합니다.
        
        Args:
            messages: 대화 히스토리
            model: 사용할 로컬 모델 (기본: openai/gpt-oss-120b)
            max_tokens: 생성할 최대 토큰 수
            temperature: 응답의 창의성 조절
            
        Returns:
            로컬 AI 서버 응답 (OpenAI 호환 포맷)
        """
        try:
            headers = {
                "Content-Type": "application/json",
            }
            
            payload = {
                "model": model,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "messages": [msg.model_dump() for msg in messages],
            }
            
            logger.info(f"Calling Local AI (DGX Spark) with model: {model}")
            logger.info(f"Local AI Base URL: {self.local_ai_base_url}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"{self.local_ai_base_url}/chat/completions",
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=120)  # 로컬 서버는 더 긴 타임아웃
                ) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"Local AI server error: {error_text}")
                        raise HTTPException(
                            status_code=status.HTTP_502_BAD_GATEWAY,
                            detail=f"Local AI server returned error: {error_text}"
                        )
                    
                    result = await response.json()
                    logger.info(f"Local AI call successful. Tokens used: {result.get('usage', {})}")
                    return result
                    
        except aiohttp.ClientError as e:
            logger.error(f"Network error calling Local AI server: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Failed to connect to Local AI server at {self.local_ai_base_url}: {str(e)}"
            )
        except Exception as e:
            logger.error(f"Unexpected error in chat_local: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {str(e)}"
            )

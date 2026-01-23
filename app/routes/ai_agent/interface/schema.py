from pydantic import BaseModel, Field
from typing import List, Optional, Literal


class Message(BaseModel):
    role: Literal["user", "assistant", "system"]
    content: str


class ChatRequest(BaseModel):
    messages: List[Message] = Field(
        ...,
        description="대화 히스토리를 포함한 메시지 배열",
        min_length=1
    )
    model: str = Field(
        default="gpt-4",
        description="사용할 OpenAI 모델 (gpt-4, gpt-4-turbo, gpt-3.5-turbo 등)"
    )
    max_tokens: int = Field(
        default=1024,
        description="생성할 최대 토큰 수",
        ge=1,
        le=8192
    )
    temperature: float = Field(
        default=1.0,
        description="응답의 창의성 조절 (0.0 ~ 2.0)",
        ge=0.0,
        le=2.0
    )


class ChatLocalRequest(BaseModel):
    messages: List[Message] = Field(
        ...,
        description="대화 히스토리를 포함한 메시지 배열",
        min_length=1
    )
    model: str = Field(
        default="openai/gpt-oss-120b",
        description="사용할 로컬 AI 모델 (openai/gpt-oss-120b 등)"
    )
    max_tokens: int = Field(
        default=1024,
        description="생성할 최대 토큰 수",
        ge=1,
        le=32768
    )
    temperature: float = Field(
        default=1.0,
        description="응답의 창의성 조절 (0.0 ~ 2.0)",
        ge=0.0,
        le=2.0
    )


class ChatResponse(BaseModel):
    content: str = Field(..., description="AI의 응답 메시지")
    model: str = Field(..., description="사용된 모델명")
    usage: dict = Field(..., description="토큰 사용량 정보")

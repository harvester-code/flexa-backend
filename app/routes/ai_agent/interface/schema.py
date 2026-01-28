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


class CommandRequest(BaseModel):
    """명령 실행 요청 - 사용자가 content만 보냄"""
    content: str = Field(..., description="사용자 명령 (예: 'checkin 카운터 프로세스 추가해줘')")
    model: str = Field(
        default="gpt-4o-2024-08-06",
        description="사용할 OpenAI 모델 (Structured Outputs 지원 모델 권장)"
    )
    temperature: float = Field(
        default=0.1,
        description="응답의 일관성을 위한 낮은 temperature",
        ge=0.0,
        le=2.0
    )


class CommandResponse(BaseModel):
    """명령 실행 응답"""
    success: bool = Field(..., description="명령 실행 성공 여부")
    message: str = Field(..., description="사용자에게 보여줄 메시지")
    action: Optional[str] = Field(None, description="실행된 액션 (add_process, remove_process 등)")
    data: Optional[dict] = Field(None, description="추가 데이터 (프로세스 정보 등)")
    error: Optional[str] = Field(None, description="에러 메시지 (실패 시)")

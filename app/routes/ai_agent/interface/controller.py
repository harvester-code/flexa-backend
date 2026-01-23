from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status

from app.libs.containers import Container
from packages.supabase.dependencies import verify_token
from app.routes.ai_agent.application.service import AIAgentService
from app.routes.ai_agent.interface.schema import ChatRequest, ChatLocalRequest, ChatResponse


ai_agent_router = APIRouter(
    prefix="/ai-agent",
    dependencies=[Depends(verify_token)],
)


@ai_agent_router.post(
    "/chat",
    status_code=status.HTTP_200_OK,
    response_model=ChatResponse,
    summary="외부 OpenAI API와 채팅",
    description="외부 OpenAI API를 통해 대화를 생성합니다. 사용자의 메시지와 대화 히스토리를 전송하면 AI 응답을 받을 수 있습니다.",
)
@inject
async def chat(
    request: ChatRequest,
    ai_agent_service: AIAgentService = Depends(Provide[Container.ai_agent_service]),
):
    """
    외부 OpenAI API와의 대화 엔드포인트
    
    - **messages**: 대화 히스토리 (최소 1개 이상의 메시지 필요)
    - **model**: 사용할 OpenAI 모델 (기본: gpt-4)
    - **max_tokens**: 생성할 최대 토큰 수 (기본: 1024)
    - **temperature**: 응답의 창의성 (0.0 ~ 2.0, 기본: 1.0)
    """
    result = await ai_agent_service.chat(
        messages=request.messages,
        model=request.model,
        max_tokens=request.max_tokens,
        temperature=request.temperature,
    )
    
    # OpenAI API 응답을 우리의 스키마에 맞게 변환
    return ChatResponse(
        content=result["choices"][0]["message"]["content"],
        model=result["model"],
        usage=result["usage"],
    )


@ai_agent_router.post(
    "/chat/local",
    status_code=status.HTTP_200_OK,
    response_model=ChatResponse,
    summary="로컬 DGX Spark AI 서버와 채팅",
    description="로컬 DGX Spark에 배포된 TRT-LLM 서버(gpt-oss-120b)를 통해 대화를 생성합니다. API 키가 필요 없으며 내부 네트워크로 통신합니다.",
)
@inject
async def chat_local(
    request: ChatLocalRequest,
    ai_agent_service: AIAgentService = Depends(Provide[Container.ai_agent_service]),
):
    """
    로컬 DGX Spark AI 서버와의 대화 엔드포인트
    
    - **messages**: 대화 히스토리 (최소 1개 이상의 메시지 필요)
    - **model**: 사용할 로컬 모델 (기본: openai/gpt-oss-120b)
    - **max_tokens**: 생성할 최대 토큰 수 (기본: 1024, 최대: 32768)
    - **temperature**: 응답의 창의성 (0.0 ~ 2.0, 기본: 1.0)
    """
    result = await ai_agent_service.chat_local(
        messages=request.messages,
        model=request.model,
        max_tokens=request.max_tokens,
        temperature=request.temperature,
    )
    
    # 로컬 AI 서버 응답을 우리의 스키마에 맞게 변환
    return ChatResponse(
        content=result["choices"][0]["message"]["content"],
        model=result["model"],
        usage=result["usage"],
    )

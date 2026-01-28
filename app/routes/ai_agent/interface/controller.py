from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status, HTTPException
from loguru import logger

from app.libs.containers import Container
from packages.supabase.dependencies import verify_token
from app.routes.ai_agent.application.service import AIAgentService
from app.routes.ai_agent.application.core import CommandParser, CommandExecutor
from app.routes.ai_agent.interface.schema import (
    ChatRequest, 
    ChatLocalRequest, 
    ChatResponse,
    CommandRequest,
    CommandResponse
)


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


@ai_agent_router.post(
    "/scenario/{scenario_id}/execute-command",
    status_code=status.HTTP_200_OK,
    response_model=CommandResponse,
    summary="시나리오 명령 실행",
    description="사용자 명령을 파싱하여 시나리오 설정을 변경합니다. AWS S3의 데이터를 확인하고 수정합니다.",
)
@inject
async def execute_command(
    scenario_id: str,
    request: CommandRequest,
    command_parser: CommandParser = Depends(Provide[Container.command_parser]),
):
    """
    시나리오 명령 실행 엔드포인트
    
    사용자가 content만 보내면, AI가 명령을 파싱하고 실행합니다.
    
    예시:
    - "checkin 프로세스 추가해줘"
    - "보안검색 단계 삭제해줘"
    - "프로세스 목록 보여줘"
    
    - **content**: 사용자 명령
    - **model**: 사용할 OpenAI 모델 (기본: gpt-4o-2024-08-06)
    - **temperature**: 응답의 일관성 (기본: 0.1)
    """
    try:
        # 1. 명령 파싱
        parsed = await command_parser.parse_command(
            user_content=request.content,
            scenario_id=scenario_id,
            model=request.model,
            temperature=request.temperature
        )
        
        # 2. 에러 처리
        if parsed.get("action") == "error":
            return CommandResponse(
                success=False,
                message=f"명령을 이해할 수 없습니다: {parsed.get('error', 'Unknown error')}",
                error=parsed.get("error"),
            )
        
        # 3. 일반 대화인 경우
        if parsed.get("action") == "chat":
            return CommandResponse(
                success=True,
                message=parsed.get("content", ""),
                action="chat",
            )
        
        # 4. 명령 실행
        action = parsed.get("action")
        parameters = parsed.get("parameters", {})
        executor = command_parser.command_executor
        
        if action == "add_process":
            result = await executor.add_process(
                scenario_id=scenario_id,
                process_name=parameters.get("process_name"),
            )
            return CommandResponse(
                success=result["success"],
                message=result["message"],
                action="add_process",
                data=result.get("data"),
                error=result.get("error"),
            )
        
        elif action == "remove_process":
            result = await executor.remove_process(
                scenario_id=scenario_id,
                process_name=parameters.get("process_name"),
            )
            return CommandResponse(
                success=result["success"],
                message=result["message"],
                action="remove_process",
                data=result.get("data"),
                error=result.get("error"),
            )
        
        elif action == "list_processes":
            context = await executor.get_scenario_context(scenario_id)
            process_list = context.get("process_names", [])
            
            if not process_list:
                message = "현재 설정된 프로세스가 없습니다."
            else:
                formatted_list = "\n".join([f"- {name}" for name in process_list])
                message = f"현재 프로세스 목록 ({len(process_list)}개):\n{formatted_list}"
            
            return CommandResponse(
                success=True,
                message=message,
                action="list_processes",
                data={"processes": process_list, "count": len(process_list)},
            )
        
        elif action == "list_files":
            result = await executor.list_files(scenario_id)
            return CommandResponse(
                success=result["success"],
                message=result["message"],
                action="list_files",
                data={
                    "files": result.get("files", []),
                    "count": result.get("count", 0),
                    "categories": result.get("categories", {}),
                },
                error=result.get("error"),
            )
        
        elif action == "read_file":
            filename = parameters.get("filename")
            
            # 파일 읽기 (기본적으로 summary 타입으로 AI 분석)
            result = await executor.read_file(
                scenario_id=scenario_id,
                filename=filename,
                summary_type="summary"  # 기본값으로 summary 사용
            )
            
            if not result["success"]:
                return CommandResponse(
                    success=False,
                    message=result["message"],
                    action="read_file",
                    error=result.get("error"),
                )
            
            # AI 분석이 필요한 경우
            if result.get("needs_ai_analysis"):
                # 원본 사용자 질문으로 AI 분석 요청
                analysis_result = await command_parser.analyze_file_content(
                    scenario_id=scenario_id,
                    filename=filename,
                    file_content={
                        "content_preview": result.get("content_preview", ""),
                        "full_content": result.get("full_content")
                    },
                    user_query=request.content,  # 원본 질문
                    model=request.model,
                    temperature=request.temperature
                )
                
                if analysis_result.get("success"):
                    return CommandResponse(
                        success=True,
                        message=analysis_result.get("content", ""),
                        action="read_file",
                        data={
                            "filename": filename,
                            "analysis": True,
                        },
                    )
                else:
                    return CommandResponse(
                        success=False,
                        message=f"파일 분석 중 오류가 발생했습니다: {analysis_result.get('error')}",
                        action="read_file",
                        error=analysis_result.get("error"),
                    )
            
            # 구조나 전체 내용인 경우
            return CommandResponse(
                success=result["success"],
                message=result["message"],
                action="read_file",
                data={
                    "filename": filename,
                    "structure": result.get("structure"),
                    "content": result.get("content"),
                },
                error=result.get("error"),
            )

        else:
            return CommandResponse(
                success=False,
                message=f"지원하지 않는 명령입니다: {action}",
                error=f"Unsupported action: {action}",
            )
    
    except Exception as e:
        logger.error(f"Command execution failed: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"명령 실행 실패: {str(e)}"
        )

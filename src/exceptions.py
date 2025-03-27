# app/exception.py
from fastapi import FastAPI, Request, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError
from starlette.responses import JSONResponse
from src.response import ErrorResponse


# ==================================================
# NOTE: DB 오류 처리
async def db_exception_handler(request: Request, exc: SQLAlchemyError):
    response = ErrorResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # NOTE: 개발 환경에서만 사용할 것
    response.data = str(exc)

    return JSONResponse(
        status_code=response.status_code,
        content=response.model_dump(),
    )


# ==================================================
# NOTE: 400 Bad Request 예외
class BadRequestException(HTTPException):
    def __init__(
        self,
        status_code: int = status.HTTP_400_BAD_REQUEST,
        detail: str = "Invalid request data",
        headers: dict = None,
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)


async def bad_request_exception_handler(request: Request, exc: BadRequestException):
    response = ErrorResponse(status_code=exc.status_code)

    # NOTE: 개발 환경에서만 사용할 것
    response.data = str(exc)

    return JSONResponse(
        status_code=response.status_code,
        content=response.model_dump(),
    )


# ==================================================
# NOTE: 500 Internal Server Error 전역 핸들러
async def internal_server_error_handler(request: Request, exc: Exception):
    response = ErrorResponse(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # NOTE: 개발 환경에서만 사용할 것
    response.data = str(exc)

    return JSONResponse(status_code=response.status_code, content=response.model_dump())


# ==================================================
def add_exception_handlers(app: FastAPI):
    app.add_exception_handler(SQLAlchemyError, db_exception_handler)
    app.add_exception_handler(BadRequestException, bad_request_exception_handler)
    app.add_exception_handler(Exception, internal_server_error_handler)

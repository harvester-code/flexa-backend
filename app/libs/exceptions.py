from fastapi import FastAPI, HTTPException, Request, status
from sqlalchemy.exc import SQLAlchemyError
from starlette.responses import JSONResponse

DEFAULT_ERROR_MESSAGE = "Response Error!"


def _build_error_content(status_code: int, detail: str | None = None) -> dict:
    content = {
        "status_code": status_code,
        "message": DEFAULT_ERROR_MESSAGE,
    }
    if detail is not None:
        content["data"] = detail
    return content


# ==================================================
# NOTE: DB 오류 처리
async def db_exception_handler(request: Request, exc: SQLAlchemyError):
    response_content = _build_error_content(
        status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=response_content,
    )


# ==================================================
# NOTE: 400 Bad Request 예외
class BadRequestException(HTTPException):
    def __init__(
        self,
        detail: str = "Invalid request data",
        status_code: int = status.HTTP_400_BAD_REQUEST,
        headers: dict = None,
    ):
        super().__init__(status_code=status_code, detail=detail, headers=headers)


async def bad_request_exception_handler(request: Request, exc: BadRequestException):
    response_content = _build_error_content(exc.status_code, detail=str(exc))

    return JSONResponse(
        status_code=exc.status_code,
        content=response_content,
    )


# ==================================================
# NOTE: 500 Internal Server Error 전역 핸들러
async def internal_server_error_handler(request: Request, exc: Exception):
    response_content = _build_error_content(
        status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, content=response_content
    )


# ==================================================
def add_exception_handlers(app: FastAPI):
    app.add_exception_handler(SQLAlchemyError, db_exception_handler)
    app.add_exception_handler(BadRequestException, bad_request_exception_handler)
    app.add_exception_handler(Exception, internal_server_error_handler)

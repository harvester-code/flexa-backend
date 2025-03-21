# app/exception.py
from fastapi import FastAPI, Request, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError
from starlette.responses import JSONResponse


# ==================================================
# NOTE: DB 오류 처리
async def db_exception_handler(request: Request, exc: SQLAlchemyError):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Database error. Please try again later.",
            "error": f"{exc}",
        },
    )


# ==================================================
# NOTE: 400 Bad Request 예외
class BadRequestException(HTTPException):
    def __init__(self, detail: str = "Invalid request data"):
        super().__init__(status_code=status.HTTP_400_BAD_REQUEST, detail=detail)


async def bad_request_exception_handler(request: Request, exc: BadRequestException):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"detail": exc.detail},
    )


# ==================================================
def add_exception_handlers(app: FastAPI):
    app.add_exception_handler(SQLAlchemyError, db_exception_handler)
    app.add_exception_handler(BadRequestException, bad_request_exception_handler)

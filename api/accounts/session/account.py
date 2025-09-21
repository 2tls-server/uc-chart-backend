import datetime
from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status
from fastapi.responses import JSONResponse

from database import external

from helpers.session import get_session, Session

router = APIRouter()


@router.get("/")
async def main(
    request: Request,
    session: Session = get_session(
        enforce_auth=True, enforce_type=False, allow_banned_users=False
    ),
):
    return_keys = [
        "sonolus_id",
        "sonolus_handle",
        "sonolus_username",
        "created_at",
        "mod",
        "admin",
    ]
    return_val = {}
    for key, value in (await session.user()).model_dump().items():
        if key in return_keys:
            return_val[key] = value
    return return_val

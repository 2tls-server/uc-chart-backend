PAGE_ITEM_COUNT = 10
PREVIEW_ITEM_COUNT = 3

MAX_FILE_SIZES = {
    "data": 2 * 1024 * 1024, # 2 mb
    "config": 100 # 100 bytes
}

from fastapi import APIRouter, Request, HTTPException, status, UploadFile, Form
from core import ChartFastAPI

from helpers.session import get_session, Session
from helpers.models import LevelSpeed

from database import leaderboards

router = APIRouter()

@router.post("/")
async def upload_replay(
    request: Request,
    replay_data_file: UploadFile,
    replay_config_file: UploadFile,
    level_speed: LevelSpeed,
    session: Session = get_session(
        enforce_auth=True, enforce_type="game", allow_banned_users=False
    )
):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="the")
    
    if (
        replay_data_file.size > MAX_FILE_SIZES["data"]
        or replay_config_file.size > MAX_FILE_SIZES["config"]
    ):
        raise HTTPException(
            status_code=status.HTTP_406_NOT_ACCEPTABLE,
            detail="Uploaded files exceed file size limit.",
        )

    async with app.db_acquire() as conn:
        await conn.execute(leaderboards.)
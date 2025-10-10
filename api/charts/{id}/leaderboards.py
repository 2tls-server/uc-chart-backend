from fastapi import APIRouter, Request, HTTPException, status, UploadFile, Query
import asyncio
from io import BytesIO
from typing import Optional, Literal

from helpers.models import ReplayUploadData, Leaderboard
from helpers.session import Session, get_session
from helpers.hashing import calculate_sha1
from core import ChartFastAPI

from database import leaderboards, charts

MAX_FILE_SIZES = {
    "data": 2 * 1024 * 1024, # 2 mb
    "config": 200 # 200 bytes
}

router = APIRouter()

@router.post("/")
async def upload_replay(
    id: str,
    request: Request,
    replay_data_file: UploadFile,
    replay_config_file: UploadFile,
    data: ReplayUploadData,
    session: Session = get_session(
        enforce_auth=True, enforce_type="game", allow_banned_users=False
    )
):
    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )

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
        curr_leaderboard = await conn.fetchrow(leaderboards.get_user_leaderboard_for_chart(
            id, session.sonolus_id
        ))

        if curr_leaderboard:
            if curr_leaderboard.arcade_score >= data.arcade_score:
                return {"status": "unchanged"}
            
        level = await conn.fetchrow(charts.get_chart_by_id(id))

        if level.status == "PRIVATE" and level.chart_design != session.sonolus_id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This chart is private."
            )
            
    replay_data = await replay_data_file.read()
    replay_config = await replay_config_file.read()

    replay_data_hash = calculate_sha1(replay_data)
    replay_config_hash = calculate_sha1(replay_config)

    async with app.s3_session_getter() as s3:
        tasks = []
        bucket = await s3.Bucket(app.s3_bucket)

        for (contents, hash) in ((replay_data, replay_data_hash), (replay_config, replay_config_hash)):
            tasks.append(bucket.upload_fileobj(
                Fileobj = BytesIO(contents),
                Key=f"{level.chart_design}/{level.id}/replays/{session.sonolus_id}/{hash}",
                ExtraArgs={"ContentType": "application/gzip"}
            ))

        await asyncio.gather(*tasks)

    async with app.db_acquire() as conn:
        await conn.execute(leaderboards.insert_leaderboard_entry(
            Leaderboard(
                submitter=session.sonolus_id,
                replay_data_hash=replay_data_hash,
                replay_config_hash=replay_config_hash,
                chart_id=id,
                engine=data.engine,
                nperfect=data.nperfect,
                ngreat=data.ngreat,
                ngood=data.ngood,
                nmiss=data.nmiss,
                arcade_score=data.arcade_score,
                accuracy_score=data.accuracy_score,
                speed=data.speed
            )
        ))

    return {"status": "ok"}

@router.get("/")
async def get_scores(
    request: Request,
    id: str,
    page: Optional[int] = Query(0, ge=0),
    limit: Literal[3, 10] = 3,
    sort_by: Literal['arcade', 'accuracy'] = 'arcade'
):
    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )
    
    app: ChartFastAPI = request.app

    leaderboards.get_leaderboard_for_chart(id, limit, page) # ...
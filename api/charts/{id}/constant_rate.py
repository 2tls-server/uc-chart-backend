import io, asyncio, gzip

from fastapi import APIRouter, Request, HTTPException, status

from database import charts

from helpers.models import ChartConstantData

from typing import Optional

from helpers.session import get_session, Session
from helpers.constants import MAX_RATINGS

from core import ChartFastAPI

router = APIRouter()


@router.patch("/")
async def main(
    request: Request,
    id: str,
    data: ChartConstantData,
    session: Session = get_session(
        enforce_auth=True, enforce_type="game", allow_banned_users=False
    ),
):
    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )

    user = await session.user()

    if not user.mod:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="not mod")

    app: ChartFastAPI = request.app
    if (data.constant >= MAX_RATINGS["max"] + 1) or (
        data.constant <= MAX_RATINGS["min"] - 1
    ):
        raise HTTPException(
            status=status.HTTP_400_BAD_REQUEST, detail="Length limits exceeded"
        )
    dec_str = str(data.constant.normalize())
    if "." in dec_str and len(dec_str.split(".")[1]) > MAX_RATINGS["decimal_places"]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"More than {MAX_RATINGS['decimal_places']} decimal places are not allowed",
        )
    query = charts.update_metadata(
        chart_id=id,
        rating=data.constant,
    )
    async with app.db_acquire() as conn:
        await conn.execute(query)
    return {"result": "success"}

from fastapi import APIRouter, Request, HTTPException, Query, status
from core import ChartFastAPI

from database import charts
from helpers.session import get_session, Session

router = APIRouter()


@router.get("/")
async def main(
    request: Request,
    id: str,
    is_preview: bool = Query(False),
    session: Session = get_session(),
):
    app: ChartFastAPI = request.app

    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )

    query = charts.get_chart_by_id(id, sonolus_id=session.sonolus_id)

    async with app.db_acquire() as conn:
        result = await conn.fetchrow(query)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Chart not found."
            )

        user = None
        if session.auth:
            user = await session.user()

        if user and user.mod:
            res = {
                "data": result.model_dump(),
                "asset_base_url": app.s3_asset_base_url,
                "mod": True,
                "owner": result.author == session.sonolus_id,
            }
            if user.admin:
                res["admin"] = True
            return res

        is_owner = result.author == session.sonolus_id

        if result.status == "PRIVATE":
            if is_preview and is_owner:
                pass
            elif not is_owner:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND, detail="Chart not found."
                )

    return {
        "data": result.model_dump(),
        "asset_base_url": app.s3_asset_base_url,
        "owner": result.author == session.sonolus_id,
    }

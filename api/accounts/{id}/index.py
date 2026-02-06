from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status

from helpers.delete import delete_from_s3

from database import accounts, charts
from helpers.models import UserProfile

router = APIRouter()


@router.delete("/")
async def main_delete(request: Request, id: str):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    await delete_from_s3(app, id)

    query = accounts.delete_account(id, confirm_change=True)

    async with app.db_acquire() as conn:
        await conn.execute(query)

    return {"result": "success"}


@router.get("/")
async def get(request: Request, id: str):
    app: ChartFastAPI = request.app

    async with app.db_acquire() as conn:
        account = await conn.fetchrow(accounts.get_public_account(id))

        if not account:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

        _, chart_list_query = charts.get_chart_list(
            page=0, items_per_page=5, sort_by="likes", owned_by=account.sonolus_id
        )

        chart_list = await conn.fetch(chart_list_query)

    return UserProfile(
        account=account,
        charts=chart_list if chart_list else [],
        asset_base_url=app.s3_asset_base_url,
    )


@router.get("/stats/")
async def get(request: Request, id: str):
    app: ChartFastAPI = request.app

    async with app.db_acquire() as conn:
        account_stats = await conn.fetchrow(accounts.get_account_stats(id))

        if not account_stats:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    return account_stats.model_dump()

from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status

from helpers.delete import delete_from_s3

from database import accounts

router = APIRouter()


@router.patch("/ban/")
async def ban_user(request: Request, id: str, delete: bool = False):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_banned(id, True)

    async with app.db_acquire() as conn:
        await conn.execute(query)

    if delete:
        await delete_from_s3(app, id)

    return {"result": "success"}


@router.patch("/unban/")
async def unban_user(request: Request, id: str):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_banned(id, False)

    async with app.db_acquire() as conn:
        await conn.execute(query)

    return {"result": "success"}

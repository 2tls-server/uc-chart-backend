from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status

from helpers.delete import delete_from_s3

from database import accounts

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

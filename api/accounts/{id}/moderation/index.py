from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status

from helpers.delete import delete_from_s3

from database import accounts, staff_actions

router = APIRouter()


@router.patch("/ban/")
async def ban_user(request: Request, id: str, delete: bool = False):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_banned(id, True)

    async with app.db_acquire() as conn:
        await conn.execute(query)
        await conn.execute(
            staff_actions.log_action(
                actor_id="SYSTEM",
                action="ban",
                target_type="account",
                target_id=id,
                previous_value="false",
                new_value="true",
            )
        )
        if delete:
            await conn.conn.execute("DELETE FROM charts WHERE author = $1", id)

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
        await conn.execute(
            staff_actions.log_action(
                actor_id="SYSTEM",
                action="unban",
                target_type="account",
                target_id=id,
                previous_value="true",
                new_value="false",
            )
        )

    return {"result": "success"}

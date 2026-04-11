from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status

from database import accounts, staff_actions

router = APIRouter()


@router.patch("/mod/")
async def mod_user(request: Request, id: str):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_mod(id, True)

    async with app.db_acquire() as conn:
        await conn.execute(query)
        await conn.execute(
            staff_actions.log_action(
                actor_id="SYSTEM",
                action="mod",
                target_type="account",
                target_id=id,
                previous_value="false",
                new_value="true",
            )
        )

    return {"result": "success"}


@router.patch("/unmod/")
async def unmod_user(request: Request, id: str):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_mod(id, False)

    async with app.db_acquire() as conn:
        await conn.execute(query)
        await conn.execute(
            staff_actions.log_action(
                actor_id="SYSTEM",
                action="unmod",
                target_type="account",
                target_id=id,
                previous_value="true",
                new_value="false",
            )
        )

    return {"result": "success"}


@router.patch("/admin/")
async def admin_user(request: Request, id: str):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_admin(id, True)

    async with app.db_acquire() as conn:
        await conn.execute(query)
        await conn.execute(
            staff_actions.log_action(
                actor_id="SYSTEM",
                action="admin",
                target_type="account",
                target_id=id,
                previous_value="false",
                new_value="true",
            )
        )

    return {"result": "success"}


@router.patch("/unadmin/")
async def unadmin_user(request: Request, id: str):
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    query = accounts.set_admin(id, False)

    async with app.db_acquire() as conn:
        await conn.execute(query)
        await conn.execute(
            staff_actions.log_action(
                actor_id="SYSTEM",
                action="unadmin",
                target_type="account",
                target_id=id,
                previous_value="true",
                new_value="false",
            )
        )

    return {"result": "success"}

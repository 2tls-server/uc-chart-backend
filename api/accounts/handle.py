from core import ChartFastAPI

from typing import Optional

from fastapi import APIRouter, Request, HTTPException, status

from database import accounts
from helpers.session import get_session, Session
from helpers.models import PublicAccount

router = APIRouter()


@router.get("/{handle}/")
async def main(
    handle: int,
    request: Request,
    session: Session = get_session(enforce_auth=False),
):
    app: ChartFastAPI = request.app

    async with app.db_acquire() as conn:
        account = await conn.fetchrow(accounts.get_account_from_handle(handle))

        if not account:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "account not found")

    return account.model_dump(
        include=[
            "sonolus_id",
            "sonolus_handle",
            "sonolus_username",
            "mod",
            "admin",
            "banned",
        ]
    )

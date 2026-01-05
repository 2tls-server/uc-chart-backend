import asyncio, json, base64, hashlib
import hmac
from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status

from database import accounts, external

from helpers.models import ExternalServiceUserProfileWithType

router = APIRouter()


@router.post("/")
async def main(request: Request, data: ExternalServiceUserProfileWithType):
    """
    We support a maximum of 6 sessions:
    - 3 external (eg. website)
    - 3 in-game

    This route will replace any expired or nonexistent session.

    If all sessions are not expired, we replace the OLDEST session.
    """
    app: ChartFastAPI = request.app

    if request.headers.get(app.auth_header) != app.auth:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="why?")

    id_data = app.decode_key(data.id_key)

    session_key_data = {
        "id": id_data.id,
        "user_id": data.id,
        "type": data.type,
    }
    encoded_key = base64.urlsafe_b64encode(
        json.dumps(session_key_data).encode()
    ).decode()
    signature = hmac.new(
        app.token_secret_key.encode(), encoded_key.encode(), hashlib.sha256
    ).hexdigest()
    session_key = f"{encoded_key}.{signature}"
    account_query, query = accounts.create_account_if_not_exists_and_new_session(
        session_key,
        data.id,
        int(data.handle),
        data.name,
        data.type,
        expiry_ms=7 * 24 * 60 * 60 * 1000,
    )
    query2 = external.update_session_key(id_key=data.id_key, session_key=session_key)

    async with app.db_acquire() as conn:
        await conn.execute(account_query)
        await conn.execute(query2)
        result = await conn.fetchrow(query)
        if result:
            return {"session": result.session_key, "expiry": int(result.expires)}
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error while processing session result.",
        )

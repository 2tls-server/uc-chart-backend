from fastapi import Header, HTTPException, status, Request
from core import ChartFastAPI
from typing import Literal, Optional
from database import accounts
from fastapi import Depends
from helpers.models import Account


def get_session(
    enforce_auth: bool = False,
    enforce_type: Literal["game", "external", False] = False,
    allow_banned_users: bool = True,
):
    async def dependency(request: Request, authorization: str = Header(None)):
        session = Session(
            enforce_auth=enforce_auth,
            enforce_type=enforce_type,
            allow_banned_users=allow_banned_users,
        )
        await session(request, authorization)
        return session

    return Depends(dependency)


class Session:
    def __init__(
        self,
        enforce_auth: bool = False,
        enforce_type: Literal["game", "external", False] = False,
        allow_banned_users: bool = True,
    ):
        self.enforce_auth = enforce_auth
        self.enforce_type = enforce_type
        self.allow_banned_users = allow_banned_users
        self._user_fetched = False
        self._user = None

    async def user(self) -> Account:
        if not self._user_fetched:
            query = accounts.get_account_from_session(
                self.session_data.user_id, self.auth, self.session_data.type
            )

            async with self.app.db_acquire() as conn:
                result = await conn.fetchrow(query)

                if not result and self.enforce_auth:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Not logged in.",
                    )

                self._user = result
                self._user_fetched = True

        return self._user

    async def __call__(
        self, request: Request, authorization: Optional[str] = Header(None)
    ):
        self.app: ChartFastAPI = request.app
        self.auth = authorization

        if not authorization and self.enforce_auth:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Not logged in."
            )

        if authorization:
            self.session_data = self.app.decode_key(authorization)
            self.sonolus_id = self.session_data.user_id

            if self.enforce_type and self.session_data.type != self.enforce_type:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="Invalid token type."
                )
            user = await self.user()
            if not user:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED, detail="Not logged in."
                )
            if not self.allow_banned_users and user.banned:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN, detail="User banned."
                )
        else:
            self.session_data = None
            self.sonolus_id = None
        return self

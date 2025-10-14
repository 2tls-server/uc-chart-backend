from fastapi import APIRouter, Request
from helpers.session import Session, get_session
from helpers import upload_token

router = APIRouter()

@router.get("/")
async def generate(
    request: Request,
    session: Session = get_session(
        enforce_auth=True, enforce_type="game", allow_banned_users=False
    )
):
    # can't use session token here: it might expire before the upload happens
    return upload_token.generate(session.sonolus_id, request.app)
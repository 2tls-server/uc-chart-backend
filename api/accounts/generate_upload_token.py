from fastapi import APIRouter, Request
from helpers.session import Session, get_session
from helpers import upload_token
import json

router = APIRouter()

@router.get("/")
async def generate(
    request: Request,
    hashes_json: str,
    session: Session = get_session(
        enforce_auth=True, enforce_type="game", allow_banned_users=False
    )
):
    # can't use session token here: it might expire before the upload happens
    return upload_token.generate(session.sonolus_id, json.loads(hashes_json), request.app)
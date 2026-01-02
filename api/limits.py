from fastapi import APIRouter, Request, HTTPException, status
from core import ChartFastAPI

from helpers.session import get_session, Session
from helpers.constants import MAX_FILE_SIZES, MAX_TEXT_SIZES

router = APIRouter()


@router.get("/")
async def main(request: Request, session: Session = get_session()):
    # exposed to public
    # no authentication needed

    return {
        "files": MAX_FILE_SIZES,
        "text": MAX_TEXT_SIZES,
    }

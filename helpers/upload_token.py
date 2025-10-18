from core import ChartFastAPI
from fastapi import HTTPException, status
import hmac
import base64
import time
import hashlib
import json

"""
It really shouldn't be like this, but it works for now so uhh

TODO: move to redis
"""

UPLOAD_TOKEN_EXPIRE_TIME = 180

def generate(sonolus_id: str, hashes: dict[str, str], app: ChartFastAPI) -> str:
    upload_token_data = {
        "user_id": sonolus_id,
        "hashes": hashes,
        "expires_at": int(time.time() + UPLOAD_TOKEN_EXPIRE_TIME)
    }

    encoded_key = base64.urlsafe_b64encode(
        json.dumps(upload_token_data).encode()
    ).decode()

    signature = hmac.new(
        app.token_secret_key.encode(), encoded_key.encode(), hashlib.sha256
    ).hexdigest()

    return f"{encoded_key}.{signature}"

def verify(upload_token: str, app: ChartFastAPI) -> tuple[str, dict[str, str]]:
    encoded_data, signature = upload_token.rsplit(".", 1)

    decoded_data = base64.urlsafe_b64decode(encoded_data).decode()
    if decoded_data["expires_at"] > time.time():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Expired upload token.")

    sonolus_id = decoded_data["user_id"]
    hashes = decoded_data["hashes"]

    recalculated_signature = hmac.new(
        app.token_secret_key.encode(), encoded_data.encode(), hashlib.sha256
    ).hexdigest()

    if recalculated_signature != signature:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid upload token.")
    
    return sonolus_id, hashes
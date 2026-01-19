from typing import Literal
from fastapi import HTTPException


async def get_and_check_file(file, expected_type: Literal["image/png", "audio/mpeg"]):
    # Read the first few bytes of the file (enough for magic number check)
    if expected_type == "image/png":
        file_bytes = await file.read(10)
        # PNG magic number (89 50 4E 47 0D 0A 1A 0A)
        if file_bytes[:8] != b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a":
            raise HTTPException(
                status_code=400,
                detail="Invalid file format. Please upload a valid PNG image.",
            )
    elif expected_type == "image":
        file_bytes = await file.read(20)
        # PNG
        if file_bytes[:8] == b"\x89PNG\r\n\x1a\n":
            result = True
        # JPEG/JFIF
        elif file_bytes[:3] == b"\xff\xd8\xff":
            result = True
        # JPEG 2000 (JP2 signature box)
        elif file_bytes[:8] == b"\x00\x00\x00\x0c\x6a\x50\x20\x20":
            result = True
        # JPEG 2000 codestream
        elif file_bytes[:2] == b"\xff\x4f":
            result = True
        # AVIF
        elif file_bytes[4:12] == b"ftypavif" or file_bytes[4:12] == b"ftypavis":
            result = True
        # ICO
        elif file_bytes[:4] == b"\x00\x00\x01\x00":
            result = True
        # ICNS
        elif file_bytes[:4] == b"icns":
            result = True
        else:
            result = False
        if not result:
            raise HTTPException(
                status_code=400,
                detail="Invalid image format. Supported: PNG, JPEG, JPEG 2000, AVIF, ICO, ICNS.",
            )
    elif expected_type == "audio/mpeg":
        file_bytes = await file.read(10)
        if not (file_bytes.startswith(b"ID3") or file_bytes[:2] == b"\xff\xfb"):
            raise HTTPException(
                status_code=400,
                detail="Invalid file format. Please upload a valid MP3 audio file.",
            )
    await file.seek(0)
    return await file.read()

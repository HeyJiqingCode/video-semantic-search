from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path

from fastapi import APIRouter, HTTPException, UploadFile

from src.pipeline import process_uploaded_file

router = APIRouter()
logger = logging.getLogger("uvicorn.error")


# Save upload to a temp file, run analysis pipeline, then clean temp file.
@router.post("/pipeline/upload-and-process")
async def upload_and_process(file: UploadFile) -> dict:
    suffix = Path(file.filename or "upload.mp4").suffix or ".mp4"
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            # Stream chunks to avoid loading large videos fully into memory.
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                tmp.write(chunk)
            temp_path = tmp.name
        result = process_uploaded_file(temp_path, original_filename=file.filename)
        return result
    except Exception as exc:
        logger.exception("[Pipeline] upload-and-process failed: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        await file.close()
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

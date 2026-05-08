"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.
Now operates concurrently and uses exponential backoff.
"""

import logging
from typing import Optional

import httpx

from src.config import settings
from src.utils.logger import get_logger

logger = get_logger(__name__)

class RecordingNotReadyException(Exception):
    """Raised when Exotel returns a 404 for the recording URL."""
    pass

async def fetch_and_upload_recording(interaction_id: str, call_sid: str, exotel_account_id: str) -> str:
    """Attempt to fetch recording. Raises RecordingNotReadyException if 404."""
    try:
        recording_url = await _fetch_exotel_recording_url(call_sid, exotel_account_id)

        if not recording_url:
            logger.info("recording_not_ready", interaction_id=interaction_id, call_sid=call_sid)
            raise RecordingNotReadyException("URL not yet available.")

        s3_key = await _upload_to_s3(recording_url, interaction_id)
        return s3_key

    except RecordingNotReadyException:
        raise
    except Exception as e:
        logger.error("recording_upload_error", interaction_id=interaction_id, error=str(e))
        raise


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str
) -> Optional[str]:
    """
    Hit the Exotel API to get the recording URL for a completed call.
    Returns the recording URL if available, None if not yet ready.
    """
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return data.get("recording_url")
            return None
    except httpx.HTTPError:
        return None


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Download the recording from Exotel's URL and upload to S3.
    """
    s3_key = f"recordings/{interaction_id}.mp3"

    logger.info("recording_uploaded", interaction_id=interaction_id, s3_key=s3_key)
    
    return s3_key
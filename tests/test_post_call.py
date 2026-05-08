import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from src.services.rate_limiter import TokenBucketRateLimiter
from src.services.recording import fetch_and_upload_recording, RecordingNotReadyException
from src.tasks.celery_tasks import triage_interaction_task

@pytest.mark.asyncio
async def test_ac8_short_transcript_skips_llm():
    """
    AC8: Short transcripts (< 4 turns / 15 words) never consume LLM quota.
    """
    payload = {
        "interaction_id": "test-short-001",
        "lead_id": "lead-123",
        "transcript_text": "agent: hello \n customer: wrong number",
    }
    
    with patch("src.tasks.celery_tasks._run_async") as mock_run_async, \
         patch("src.tasks.celery_tasks.fetch_recording_task.apply_async") as mock_recording, \
         patch("src.tasks.celery_tasks.process_llm_task.apply_async") as mock_llm:
        
        triage_interaction_task(payload)
        
        # Ensure recording fetch was still triggered concurrently
        mock_recording.assert_called_once()
        
        # CRITICAL: Ensure LLM task was NEVER called because transcript was short
        mock_llm.assert_not_called()


@pytest.mark.asyncio
async def test_ac1_ac2_rate_limiter_enforces_budgets():
    """
    AC1 & AC2: System respects rate limits and enforces per-customer budgets.
    """
    limiter = TokenBucketRateLimiter()
    customer_a = "cust-A"
    customer_b = "cust-B"
    
    # Mock the Redis Lua script evaluation
    with patch("src.services.rate_limiter.redis_client.eval", new_callable=AsyncMock) as mock_eval:
        
        # Scenario 1: Customer A has tokens. Should return True (1).
        mock_eval.return_value = 1
        result = await limiter.consume_tokens(customer_a, 1500, 20000)
        assert result is True
        
        # Scenario 2: Customer A exhausts their budget. Redis Lua returns 0.
        mock_eval.return_value = 0
        result = await limiter.consume_tokens(customer_a, 1500, 20000)
        assert result is False
        
        # Scenario 3: Customer A is exhausted, but Customer B still has budget.
        # We simulate Customer B's request succeeding.
        mock_eval.return_value = 1
        result = await limiter.consume_tokens(customer_b, 1500, 30000)
        assert result is True


@pytest.mark.asyncio
async def test_ac4_recording_poller_raises_exception_for_backoff():
    """
    AC4: Recording poller does not sleep blindly. If 404, it raises an exception
    so Celery can trigger exponential backoff.
    """
    interaction_id = "test-rec-001"
    
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch:
        # Simulate Exotel returning 404 (Not Ready)
        mock_fetch.return_value = None
        
        # The function MUST raise RecordingNotReadyException instead of sleeping
        with pytest.raises(RecordingNotReadyException):
            await fetch_and_upload_recording(interaction_id, "sid-123", "account-123")


@pytest.mark.asyncio
async def test_ac4_recording_poller_succeeds():
    """
    Verify that when the recording IS available, it processes instantly.
    """
    interaction_id = "test-rec-002"
    
    with patch("src.services.recording._fetch_exotel_recording_url", new_callable=AsyncMock) as mock_fetch, \
         patch("src.services.recording._upload_to_s3", new_callable=AsyncMock) as mock_upload:
        
        # Simulate Exotel returning the URL successfully
        mock_fetch.return_value = "https://exotel.com/recording.mp3"
        mock_upload.return_value = "recordings/test-rec-002.mp3"
        
        result = await fetch_and_upload_recording(interaction_id, "sid-123", "account-123")
        
        assert result == "recordings/test-rec-002.mp3"
        mock_upload.assert_called_once()
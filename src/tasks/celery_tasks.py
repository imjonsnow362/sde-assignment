import asyncio
from typing import Dict, Any
from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.recording import fetch_and_upload_recording, RecordingNotReadyException
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.rate_limiter import rate_limiter
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def _run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()

@celery_app.task(name="triage_interaction_task", queue="postcall_processing")
def triage_interaction_task(payload: Dict[str, Any]):
    """Fast heuristics. Routes calls to Hot/Cold lanes and fires recording fetch."""
    interaction_id = payload["interaction_id"]
    transcript = payload.get("transcript_text", "").lower()
    
    # 1. Spawn Recording Fetcher concurrently
    fetch_recording_task.apply_async(args=[interaction_id, payload.get("call_sid"), payload.get("exotel_account_id")])

    # 2. Triage: Short Transcript Check (AC8)
    word_count = len(transcript.split())
    if word_count < 15:  
        logger.info("triage_skip_llm", extra={"interaction_id": interaction_id})
        _run_async(update_lead_stage(payload["lead_id"], interaction_id, "short_call"))
        return

    # 3. Triage: Differentiated Processing
    cold_keywords = ["wrong number", "not interested", "already bought"]
    priority_lane = "cold" if any(k in transcript for k in cold_keywords) else "hot"

    logger.info("triage_complete", extra={"interaction_id": interaction_id, "lane": priority_lane})
    process_llm_task.apply_async(args=[payload, priority_lane], queue=f"llm_{priority_lane}")


@celery_app.task(bind=True, max_retries=5, queue="postcall_processing")
def fetch_recording_task(self, interaction_id: str, call_sid: str, account_id: str):
    """Exponential Backoff Poller (AC4)"""
    try:
        s3_key = _run_async(fetch_and_upload_recording(interaction_id, call_sid, account_id))
        logger.info("recording_success", extra={"interaction_id": interaction_id, "s3_key": s3_key})
    except RecordingNotReadyException as e:
        delay = 10 * (2 ** self.request.retries) # 10s, 20s, 40s...
        raise self.retry(exc=e, countdown=delay)
    except Exception as e:
        logger.error("recording_failed_permanently", extra={"interaction_id": interaction_id, "error": str(e)})


@celery_app.task(bind=True, max_retries=10, acks_late=True, queue="llm_hot")
def process_llm_task(self, payload: Dict[str, Any], priority_lane: str):
    """Rate-Limit Aware LLM worker (AC1 & AC2)"""
    interaction_id = payload["interaction_id"]
    customer_id = payload["customer_id"]
    
    # Dynamic Token Estimation
    estimated_tokens = int((len(payload.get("transcript_text", "").split()) * 1.5) + 200)
    CUSTOMER_LIMIT = 20000 # Mocked for assignment. In prod, fetch from DB.
    
    # Gatekeeper: Check Budgets
    is_allowed = _run_async(rate_limiter.consume_tokens(customer_id, estimated_tokens, CUSTOMER_LIMIT))
    if not is_allowed:
        logger.warning("llm_rate_limited_backing_off", extra={"interaction_id": interaction_id})
        raise self.retry(countdown=60) # Graceful backoff, no 429 errors

    # Execute
    ctx = PostCallContext(
        interaction_id=interaction_id, session_id=payload["session_id"], lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"], customer_id=customer_id, agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""), transcript_text=payload.get("transcript_text", ""),
        conversation_data={}, additional_data={}, ended_at=datetime.utcnow(), exotel_account_id=None
    )

    try:
        processor = PostCallProcessor()
        result = _run_async(processor.process_post_call(ctx, single_prompt=True))
        
        # Safely trigger downstream
        _run_async(trigger_signal_jobs(interaction_id, ctx.session_id, ctx.campaign_id, result.raw_response))
        _run_async(update_lead_stage(ctx.lead_id, interaction_id, result.call_stage))
        
    except Exception as e:
        logger.error("llm_processing_error", extra={"interaction_id": interaction_id, "error": str(e)})
        raise self.retry(exc=e, countdown=60)
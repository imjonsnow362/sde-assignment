import logging
from src.utils.redis_client import redis_client
from src.config import settings

logger = logging.getLogger(__name__)

class TokenBucketRateLimiter:
    # Lua script ensures atomic check-and-decrement to prevent race conditions
    LUA_SCRIPT = """
    local global_key = KEYS[1]
    local cust_key = KEYS[2]
    local requested = tonumber(ARGV[1])
    local global_limit = tonumber(ARGV[2])
    local cust_limit = tonumber(ARGV[3])

    local global_val = tonumber(redis.call('GET', global_key) or global_limit)
    local cust_val = tonumber(redis.call('GET', cust_key) or cust_limit)

    if global_val >= requested and cust_val >= requested then
        redis.call('SET', global_key, global_val - requested, 'EX', 60)
        redis.call('SET', cust_key, cust_val - requested, 'EX', 60)
        return 1 
    else
        return 0 
    end
    """

    async def consume_tokens(self, customer_id: str, estimated_tokens: int, customer_limit: int) -> bool:
        """Returns True if tokens are available, False if rate limited."""
        global_key = "llm:bucket:global"
        customer_key = f"llm:bucket:customer:{customer_id}"
        
        result = await redis_client.eval(
            self.LUA_SCRIPT, 2, 
            global_key, customer_key, 
            estimated_tokens, settings.LLM_TOKENS_PER_MINUTE, customer_limit
        )
        return result == 1

rate_limiter = TokenBucketRateLimiter()
import asyncio
from functools import wraps
from typing import Callable, Any, Coroutine

def retry_async(retries: int = 3, delay: float = 2.0, backoff: float = 2.0):
    """
    retries: number of total attempts
    delay: initial delay between attempts (seconds)
    backoff: multiplier applied to delay after each failure
    """
    def decorator(func: Callable[..., Coroutine[Any, Any, Any]]):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(1, retries + 1):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == retries:
                        raise
                    from ipeds_crawler.logging import logger
                    logger.warning(f"{func.__name__} failed (attempt {attempt}/{retries}): {e}")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
        return wrapper
    return decorator

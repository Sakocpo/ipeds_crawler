from tenacity import retry, stop_after_attempt, wait_exponential_jitter

retry_net = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=8),
    reraise=True,
)

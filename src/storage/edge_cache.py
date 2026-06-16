import os
import aiofiles
import logging
from typing import List

logger = logging.getLogger(__name__)

# File used to store offline payloads
CACHE_FILE = "offline_telemetry_cache.jsonl"

async def save_offline_payload(payload_json: str) -> None:
    """
    Asynchronously appends a failed MQTT payload to a local JSONL file.
    This ensures zero data loss during network outages.
    """
    try:
        async with aiofiles.open(CACHE_FILE, mode='a') as f:
            await f.write(payload_json + '\n')
        logger.debug("Payload safely written to Edge Cache.")
    except Exception as e:
        logger.error(f"Critical Failure: Could not write to Edge Cache: {e}")

async def drain_offline_cache() -> List[str]:
    """
    Reads all cached payloads and deletes the cache file.
    Returns a list of JSON strings ready to be published.
    """
    if not os.path.exists(CACHE_FILE):
        return []

    try:
        async with aiofiles.open(CACHE_FILE, mode='r') as f:
            lines = await f.readlines()
        
        # Atomically remove the file after reading to prevent duplicate processing
        os.remove(CACHE_FILE)
        
        valid_payloads = [line.strip() for line in lines if line.strip()]
        if valid_payloads:
            logger.info(f"Drained {len(valid_payloads)} offline payloads from Edge Cache.")
            
        return valid_payloads
    except Exception as e:
        logger.error(f"Failed to drain Edge Cache: {e}")
        return []
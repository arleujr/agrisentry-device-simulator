import asyncio
import logging
import aiomqtt
from storage.edge_cache import save_offline_payload, drain_offline_cache

logger = logging.getLogger(__name__)

class AsyncMqttPublisher:
    """
    Production-ready asynchronous MQTT publisher.
    Handles automatic reconnections, offline caching, and queue draining.
    """
    def __init__(self, broker_host: str, client_id: str):
        self.broker_host = broker_host
        self.client_id = client_id
        self.queue = asyncio.Queue(maxsize=1000) 

    async def add_to_queue(self, topic: str, payload_json: str):
        try:
            self.queue.put_nowait((topic, payload_json))
        except asyncio.QueueFull:
            logger.warning("In-memory MQTT queue is full! Forcing to Edge Cache.")
            await save_offline_payload(payload_json)

    async def worker_loop(self):
        reconnect_interval = 3
        while True:
            try:
                logger.info(f"Connecting to MQTT Broker at {self.broker_host}...")
                async with aiomqtt.Client(hostname=self.broker_host, identifier=self.client_id) as client:
                    logger.info("Successfully connected to MQTT Broker!")
                    
                    cached_payloads = await drain_offline_cache()
                    for payload in cached_payloads:
                        topic = f"agrisentry/gateway/{self.client_id}/telemetry"
                        await client.publish(topic, payload, qos=1)
                        await asyncio.sleep(0.05)
                    
                    while True:
                        topic, payload_json = await self.queue.get()
                        try:
                            await client.publish(topic, payload_json, qos=1)
                            self.queue.task_done()
                        except aiomqtt.MqttError as e:
                            logger.error(f"Failed to publish message: {e}. Routing to Edge Cache.")
                            await save_offline_payload(payload_json)
                            raise
                            
            except aiomqtt.MqttError:
                logger.error(f"MQTT connection lost. Retrying in {reconnect_interval}s...")
                await asyncio.sleep(reconnect_interval)
            except asyncio.CancelledError:
                logger.info("MQTT Publisher worker terminating cleanly.")
                break

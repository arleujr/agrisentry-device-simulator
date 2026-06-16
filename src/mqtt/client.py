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
        # In-memory queue to decouple sensor generation from network latency
        self.queue = asyncio.Queue(maxsize=1000) 

    async def add_to_queue(self, topic: str, payload_json: str):
        """ Adds a payload to the publishing queue. """
        try:
            self.queue.put_nowait((topic, payload_json))
        except asyncio.QueueFull:
            logger.warning("In-memory MQTT queue is full! Forcing to Edge Cache.")
            await save_offline_payload(payload_json)

    async def worker_loop(self):
        """
        Background worker that maintains the MQTT connection and processes the queue.
        """
        reconnect_interval = 3 # seconds
        
        while True:
            try:
                logger.info(f"Connecting to MQTT Broker at {self.broker_host}...")
                
                async with aiomqtt.Client(hostname=self.broker_host, identifier=self.client_id) as client:
                    logger.info("Successfully connected to MQTT Broker!")
                    
                    # Connection restored: First priority is to drain the offline cache
                    cached_payloads = await drain_offline_cache()
                    for payload in cached_payloads:
                        # Assuming a unified telemetry topic for the gateway
                        topic = f"agrisentry/gateway/{self.client_id}/telemetry"
                        await client.publish(topic, payload, qos=1)
                        await asyncio.sleep(0.05) # Prevent broker flooding
                    
                    # Process live data from the queue
                    while True:
                        topic, payload_json = await self.queue.get()
                        
                        try:
                            await client.publish(topic, payload_json, qos=1)
                            self.queue.task_done()
                        except aiomqtt.MqttError as e:
                            logger.error(f"Failed to publish message: {e}. Routing to Edge Cache.")
                            await save_offline_payload(payload_json)
                            raise # Trigger reconnection
                            
            except aiomqtt.MqttError:
                logger.error(f"MQTT connection lost. Retrying in {reconnect_interval}s...")
                await asyncio.sleep(reconnect_interval)
            except asyncio.CancelledError:
                logger.info("MQTT Publisher worker terminating cleanly.")
                break
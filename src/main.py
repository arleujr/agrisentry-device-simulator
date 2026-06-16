import asyncio
import sys
import logging
import uuid
from models.payload import SensorPayload
from sensors.generators import SensorSimulator
from mqtt.client import AsyncMqttPublisher

# Professional logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AgrisentryDevice")

# Simulated fixed Edge Device ID
EDGE_DEVICE_ID = f"esp32-gateway-{uuid.uuid4().hex[:8]}"
ACTIVE_SENSORS = ["TEMPERATURE", "HUMIDITY", "SOIL_MOISTURE", "LUMINOSITY"]
MQTT_BROKER_HOST = "localhost" # Apontando para o Mosquitto rodando no seu Docker local

async def sensor_worker(sensor_type: str, simulator: SensorSimulator, publisher: AsyncMqttPublisher, interval_seconds: int = 5):
    """
    Asynchronous worker that continuously generates payloads and pushes them to the MQTT queue.
    """
    try:
        while True:
            # Generate raw float reading
            raw_value = simulator.generate_reading(sensor_type)
            
            # Encapsulate in strict Pydantic contract
            payload = SensorPayload(
                device_id=EDGE_DEVICE_ID,
                sensor_type=sensor_type,
                reading_value=raw_value
            )
            
            payload_json = payload.model_dump_json()
            topic = f"agrisentry/gateway/{EDGE_DEVICE_ID}/telemetry"
            
            # Send to the publisher's internal queue (non-blocking)
            await publisher.add_to_queue(topic, payload_json)
            logger.info(f"[{sensor_type}] Queued -> {raw_value}")
            
            # Non-blocking sleep
            await asyncio.sleep(interval_seconds)
            
    except asyncio.CancelledError:
        logger.info(f"Sensor worker {sensor_type} received shutdown signal.")
        raise

async def main():
    logger.info(f"Starting AgriSentry Edge Simulator (MQTT Enabled): {EDGE_DEVICE_ID}")
    
    # Initialize Core Components
    simulator = SensorSimulator(anomaly_probability=0.05)
    publisher = AsyncMqttPublisher(broker_host=MQTT_BROKER_HOST, client_id=EDGE_DEVICE_ID)
    
    tasks = []
    
    # 1. Start MQTT Background Worker
    mqtt_task = asyncio.create_task(publisher.worker_loop(), name="MQTT_Worker")
    tasks.append(mqtt_task)
    
    # 2. Start Sensor Workers
    for sensor in ACTIVE_SENSORS:
        await asyncio.sleep(0.5) 
        task = asyncio.create_task(
            sensor_worker(sensor, simulator, publisher, interval_seconds=10),
            name=f"Sensor_{sensor}"
        )
        tasks.append(task)
        
    try:
        # Run all tasks concurrently
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Main orchestrator shutting down...")
        # Cancel all tasks cleanly to trigger Graceful Shutdown
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    # Workaround corporativo para compatibilidade do aiomqtt no Windows
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        # Standard execution
        asyncio.run(main())
    except KeyboardInterrupt:
        # Graceful shutdown triggered via Ctrl+C (SIGINT)
        logger.info("Application stopped gracefully by the user/system.")
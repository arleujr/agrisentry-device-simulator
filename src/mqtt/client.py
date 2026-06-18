import asyncio
import sys
import logging
import uuid
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
from models.payload import SensorPayload
from sensors.generators import SensorSimulator


# Professional logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AgrisentryDevice")

# Simulated fixed Edge Device ID - Força o ID a mudar a cada deploy para evitar choque de Client ID
EDGE_DEVICE_ID = f"esp32-gate-{uuid.uuid4().hex[:6]}"
ACTIVE_SENSORS = ["TEMPERATURE", "HUMIDITY", "SOIL_MOISTURE", "LUMINOSITY"]

# SE ESTIVER NO RENDER, USA O EMQX, SE ESTIVER LOCAL, USA O LOCALHOST
MQTT_BROKER_HOST = os.getenv("MQTT_BROKER_HOST", "broker.emqx.io")

# --- Servidor HTTP Fake para enganar o Render (De Graça como Web Service) ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        return # Silencia os logs de HTTP para não poluir o terminal

def start_fake_web_server():
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info(f"🌐 Fake Health Check Server started on port {port}")
    server.serve_forever()
# ----------------------------------------------------------------------------

async def sensor_worker(sensor_type: str, simulator: SensorSimulator, publisher: AsyncMqttPublisher, interval_seconds: int = 5):
    try:
        while True:
            raw_value = simulator.generate_reading(sensor_type)
            
            payload = SensorPayload(
                device_id=EDGE_DEVICE_ID,
                sensor_type=sensor_type,
                reading_value=raw_value
            )
            
            payload_json = payload.model_dump_json()
            topic = f"agrisentry/gateway/{EDGE_DEVICE_ID}/telemetry"
            
            await publisher.add_to_queue(topic, payload_json)
            logger.info(f"[{sensor_type}] Queued -> {raw_value}")
            
            await asyncio.sleep(interval_seconds)
            
    except asyncio.CancelledError:
        logger.info(f"Sensor worker {sensor_type} received shutdown signal.")
        raise

async def main():
    logger.info(f"Starting AgriSentry Edge Simulator (MQTT Enabled): {EDGE_DEVICE_ID}")
    
    # Inicia o servidor fake em uma thread separada para não travar o asyncio
    threading.Thread(target=start_fake_web_server, daemon=True).start()
    
    simulator = SensorSimulator(anomaly_probability=0.05)
    publisher = AsyncMqttPublisher(broker_host=MQTT_BROKER_HOST, client_id=EDGE_DEVICE_ID)
    
    tasks = []
    
    mqtt_task = asyncio.create_task(publisher.worker_loop(), name="MQTT_Worker")
    tasks.append(mqtt_task)
    
    for sensor in ACTIVE_SENSORS:
        await asyncio.sleep(0.5) 
        task = asyncio.create_task(
            sensor_worker(sensor, simulator, publisher, interval_seconds=10),
            name=f"Sensor_{sensor}"
        )
        tasks.append(task)
        
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Main orchestrator shutting down...")
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped gracefully by the user/system.")

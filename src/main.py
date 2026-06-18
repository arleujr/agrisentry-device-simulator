import os
import sys
import argparse


current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

import asyncio
import logging
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading

# Importações internas do seu projeto (agora resolvidas pelo sys.path)
from models.payload import SensorPayload
from sensors.generators import SensorSimulator
from mqtt.client import AsyncMqttPublisher

# Configuração Profissional de Logs
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("AgrisentryDevice")

# --- Servidor HTTP Fake para enganar o Render (De Graça como Web Service) ---
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        return  # Silencia os logs de HTTP para não poluir o terminal do Render

def start_fake_web_server():
    port = int(os.getenv("PORT", 8080))
    server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
    logger.info(f"🌐 Fake Health Check Server started on port {port}")
    server.serve_forever()
# ----------------------------------------------------------------------------

async def sensor_worker(device_id: str, sensor_type: str, simulator: SensorSimulator, publisher: AsyncMqttPublisher, interval_seconds: float):
    """
    Worker assíncrono que gera leituras e coloca na fila de envio do MQTT.
    """
    try:
        while True:
            # Gera os dados simulados
            raw_value = simulator.generate_reading(sensor_type)
            
            # Encapsula no contrato do Pydantic
            payload = SensorPayload(
                device_id=device_id,
                sensor_type=sensor_type,
                reading_value=raw_value
            )
            
            payload_json = payload.model_dump_json()
            topic = f"agrisentry/gateway/{device_id}/telemetry"
            
            # Adiciona na fila interna sem travar a thread
            await publisher.add_to_queue(topic, payload_json)
            logger.info(f"[{device_id}][{sensor_type}] Queued -> {raw_value}")
            
            # Delay não-bloqueante ajustado via CLI
            await asyncio.sleep(interval_seconds)
            
    except asyncio.CancelledError:
        raise

async def main():
    # 📑 Parseador dos argumentos vindos do "Start Command" do Render
    parser = argparse.ArgumentParser()
    parser.add_argument("--devices", type=int, default=1)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--anomaly-rate", type=float, default=0.05)
    parser.add_argument("--broker-host", type=str, default="broker.emqx.io")
    parser.add_argument("--broker-port", type=int, default=1883)
    args = parser.parse_known_args()[0]

    logger.info(f"Starting AgriSentry Edge Simulator Hub. Target Broker: {args.broker_host}")
    
    # Inicia o servidor fake em segundo plano (Daemon) para o Render não dar timeout nas portas
    threading.Thread(target=start_fake_web_server, daemon=True).start()
    
    simulator = SensorSimulator(anomaly_probability=args.anomaly_rate)
    active_sensors = ["TEMPERATURE", "HUMIDITY", "SOIL_MOISTURE", "LUMINOSITY"]
    tasks = []
    
    # 🎛️ Orquestra múltiplos dispositivos virtuais conforme configurado (--devices 5)
    for i in range(args.devices):
        # Cada instância simulada recebe um UUID próprio para evitar colisões no broker MQTT
        device_id = f"esp32-gate-{uuid.uuid4().hex[:6]}"
        
        # Instancia o publicador com o broker passado no argumento do Render
        publisher = AsyncMqttPublisher(broker_host=args.broker_host, client_id=f"{device_id}_pub")
        
        # Cria a tarefa do loop do MQTT para este dispositivo específico
        mqtt_task = asyncio.create_task(publisher.worker_loop(), name=f"MQTT_Worker_{device_id}")
        tasks.append(mqtt_task)
        
        # Cria workers independentes para cada tipo de sensor do dispositivo
        for sensor in active_sensors:
            await asyncio.sleep(0.1)  # Pequeno escalonamento para não sobrecarregar na partida
            task = asyncio.create_task(
                sensor_worker(device_id, sensor, simulator, publisher, interval_seconds=args.interval),
                name=f"Sensor_{device_id}_{sensor}"
            )
            tasks.append(task)
        
    try:
        # Executa todas as tarefas de forma concorrente
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("Main orchestrator shutting down...")
        # Executa o desligamento limpo de todas as tarefas remanescentes (Graceful Shutdown)
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    # Ajuste de compatibilidade para execução do loop assíncrono no Windows (Localdev)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application stopped gracefully by the user/system.")

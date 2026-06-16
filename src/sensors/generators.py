import random
import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)

class SensorSimulator:
    """
    Stochastic sensor simulator that generates realistic agricultural telemetry
    using Gaussian distributions, with intentional anomaly injection for ML training.
    """

    # Baseline configuration: (Mean, Standard Deviation)
    BASES: Dict[str, Tuple[float, float]] = {
        "TEMPERATURE": (25.0, 3.5),    # Celsius
        "HUMIDITY": (60.0, 10.0),      # Percentage
        "SOIL_MOISTURE": (45.0, 5.0),  # Percentage
        "LUMINOSITY": (50000.0, 5000.0) # Lux
    }

    def __init__(self, anomaly_probability: float = 0.05):
        """
        :param anomaly_probability: Chance (0.0 to 1.0) of generating a statistical anomaly.
        """
        self.anomaly_probability = anomaly_probability

    def generate_reading(self, sensor_type: str) -> float:
        """
        Generates a reading for the specified sensor type.
        Applies a massive multiplier if an anomaly is triggered to simulate hardware failure.
        """
        if sensor_type not in self.BASES:
            raise ValueError(f"Unknown sensor type: {sensor_type}")

        mean, std_dev = self.BASES[sensor_type]
        
        # Generate base realistic value using Gaussian distribution
        reading = random.gauss(mean, std_dev)

        # Inject Anomaly for Data Quality Pipeline testing (AgriPlanum Core)
        if random.random() < self.anomaly_probability:
            # Multiply by a random factor between 3x and 5x to create a clear outlier
            anomaly_factor = random.uniform(3.0, 5.0)
            reading *= anomaly_factor
            logger.warning(f"Intentionally generated anomaly for {sensor_type}: {reading:.2f}")

        # Ensure we don't return negative values for real-world physical constraints
        return max(0.0, round(reading, 2))
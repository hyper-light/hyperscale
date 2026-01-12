from dataclasses import dataclass, field

import numpy as np


@dataclass(slots=True)
class AdaptiveKalmanFilter:
    """Kalman filter with adaptive noise estimation."""

    initial_process_noise: float = 10.0
    initial_measurement_noise: float = 25.0
    adaptation_rate: float = 0.1
    innovation_window: int = 20

    _estimate: float = field(default=0.0, init=False)
    _error_covariance: float = field(default=1000.0, init=False)
    _process_noise: float = field(default=10.0, init=False)
    _measurement_noise: float = field(default=25.0, init=False)
    _innovations: list[float] = field(default_factory=list, init=False)
    _initialized: bool = field(default=False, init=False)
    _sample_count: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self._process_noise = self.initial_process_noise
        self._measurement_noise = self.initial_measurement_noise

    def update(self, measurement: float) -> tuple[float, float]:
        """Update filter with adaptive noise estimation."""
        if not self._initialized:
            self._estimate = measurement
            self._error_covariance = self._measurement_noise
            self._initialized = True
            self._sample_count = 1
            return self._estimate, float(np.sqrt(self._error_covariance))

        predicted_estimate = self._estimate
        predicted_covariance = self._error_covariance + self._process_noise

        innovation = measurement - predicted_estimate
        innovation_covariance = predicted_covariance + self._measurement_noise

        self._innovations.append(innovation)
        if len(self._innovations) > self.innovation_window:
            self._innovations.pop(0)

        kalman_gain = predicted_covariance / innovation_covariance
        self._estimate = predicted_estimate + kalman_gain * innovation
        self._error_covariance = (1.0 - kalman_gain) * predicted_covariance

        if len(self._innovations) >= max(2, self.innovation_window // 2):
            self._adapt_noise()

        self._sample_count += 1
        return self._estimate, float(np.sqrt(self._error_covariance))

    def get_estimate(self) -> float:
        return self._estimate

    def get_uncertainty(self) -> float:
        return float(np.sqrt(self._error_covariance))

    def get_sample_count(self) -> int:
        return self._sample_count

    def _adapt_noise(self) -> None:
        if len(self._innovations) < 2:
            return

        innovations_array = np.array(self._innovations)
        empirical_variance = float(np.var(innovations_array))
        expected_variance = (
            self._error_covariance + self._process_noise + self._measurement_noise
        )
        ratio = empirical_variance / max(expected_variance, 1e-6)

        if ratio > 1.2:
            self._measurement_noise *= 1.0 + self.adaptation_rate
        elif ratio < 0.8:
            self._measurement_noise *= 1.0 - self.adaptation_rate

        self._measurement_noise = float(
            np.clip(
                self._measurement_noise,
                self.initial_measurement_noise * 0.1,
                self.initial_measurement_noise * 10.0,
            )
        )

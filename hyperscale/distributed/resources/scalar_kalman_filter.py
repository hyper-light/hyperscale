from dataclasses import dataclass, field

import numpy as np


@dataclass(slots=True)
class ScalarKalmanFilter:
    """
    1D Kalman filter for resource metric smoothing.

    State model: x(k) = x(k-1) + w, where w ~ N(0, Q)
    Measurement model: z(k) = x(k) + v, where v ~ N(0, R)
    """

    process_noise: float = 10.0
    measurement_noise: float = 25.0

    _estimate: float = field(default=0.0, init=False)
    _error_covariance: float = field(default=1000.0, init=False)
    _initialized: bool = field(default=False, init=False)
    _sample_count: int = field(default=0, init=False)

    def update(self, measurement: float) -> tuple[float, float]:
        """
        Update filter with a new measurement.

        Returns (estimate, uncertainty_stddev).
        """
        if not self._initialized:
            self._estimate = measurement
            self._error_covariance = self.measurement_noise
            self._initialized = True
            self._sample_count = 1
            return self._estimate, float(np.sqrt(self._error_covariance))

        predicted_estimate = self._estimate
        predicted_covariance = self._error_covariance + self.process_noise

        kalman_gain = predicted_covariance / (
            predicted_covariance + self.measurement_noise
        )
        innovation = measurement - predicted_estimate

        self._estimate = predicted_estimate + kalman_gain * innovation
        self._error_covariance = (1.0 - kalman_gain) * predicted_covariance
        self._sample_count += 1

        return self._estimate, float(np.sqrt(self._error_covariance))

    def get_estimate(self) -> float:
        return self._estimate

    def get_uncertainty(self) -> float:
        return float(np.sqrt(self._error_covariance))

    def get_sample_count(self) -> int:
        return self._sample_count

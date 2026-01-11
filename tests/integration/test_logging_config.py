"""
Tests for LoggingConfig disable/enable functionality.

Covers:
- Global logging disable
- Per-logger disable
- Re-enabling logging
- Disabled state check
"""

import pytest

from hyperscale.logging.config.logging_config import (
    LoggingConfig,
    _global_logging_disabled,
    _global_disabled_loggers,
)
from hyperscale.logging.models import LogLevel


class TestLoggingConfigDisable:
    """Tests for LoggingConfig.disable() functionality."""

    def setup_method(self):
        """Reset logging state before each test."""
        _global_logging_disabled.set(False)
        _global_disabled_loggers.set([])

    def teardown_method(self):
        """Reset logging state after each test."""
        _global_logging_disabled.set(False)
        _global_disabled_loggers.set([])

    def test_disable_globally(self) -> None:
        """Calling disable() without arguments disables all logging."""
        config = LoggingConfig()

        assert config.disabled is False

        config.disable()

        assert config.disabled is True

    def test_disable_specific_logger(self) -> None:
        """Calling disable(name) disables only that logger."""
        config = LoggingConfig()

        assert config.disabled is False
        assert config.enabled("my_logger", LogLevel.ERROR) is True

        config.disable("my_logger")

        # Global logging still enabled
        assert config.disabled is False
        # But specific logger is disabled
        assert config.enabled("my_logger", LogLevel.ERROR) is False
        # Other loggers still work
        assert config.enabled("other_logger", LogLevel.ERROR) is True

    def test_enable_after_disable(self) -> None:
        """Calling enable() re-enables global logging."""
        config = LoggingConfig()

        config.disable()
        assert config.disabled is True

        config.enable()
        assert config.disabled is False

    def test_disabled_property_reflects_global_state(self) -> None:
        """The disabled property reflects the global context var."""
        config1 = LoggingConfig()
        config2 = LoggingConfig()

        config1.disable()

        # Both instances see the same global state
        assert config1.disabled is True
        assert config2.disabled is True

        config2.enable()

        assert config1.disabled is False
        assert config2.disabled is False

    def test_disable_multiple_loggers(self) -> None:
        """Can disable multiple specific loggers."""
        config = LoggingConfig()

        config.disable("logger_a")
        config.disable("logger_b")
        config.disable("logger_c")

        assert config.enabled("logger_a", LogLevel.ERROR) is False
        assert config.enabled("logger_b", LogLevel.ERROR) is False
        assert config.enabled("logger_c", LogLevel.ERROR) is False
        assert config.enabled("logger_d", LogLevel.ERROR) is True

    def test_disable_same_logger_twice_no_duplicates(self) -> None:
        """Disabling the same logger twice doesn't create duplicates."""
        config = LoggingConfig()

        config.disable("my_logger")
        config.disable("my_logger")

        disabled_loggers = _global_disabled_loggers.get()
        assert disabled_loggers.count("my_logger") == 1


class TestLoggingConfigEnabled:
    """Tests for LoggingConfig.enabled() method."""

    def setup_method(self):
        """Reset logging state before each test."""
        _global_logging_disabled.set(False)
        _global_disabled_loggers.set([])

    def teardown_method(self):
        """Reset logging state after each test."""
        _global_logging_disabled.set(False)
        _global_disabled_loggers.set([])

    def test_enabled_respects_log_level(self) -> None:
        """enabled() respects the configured log level."""
        config = LoggingConfig()

        # Default level is ERROR, so INFO should be disabled
        assert config.enabled("test", LogLevel.INFO) is False
        assert config.enabled("test", LogLevel.ERROR) is True

    def test_enabled_respects_disabled_loggers(self) -> None:
        """enabled() returns False for disabled loggers."""
        config = LoggingConfig()

        config.disable("disabled_logger")

        # Even ERROR level is disabled for this logger
        assert config.enabled("disabled_logger", LogLevel.ERROR) is False
        assert config.enabled("enabled_logger", LogLevel.ERROR) is True


class TestLoggerStreamDisabled:
    """Tests for LoggerStream respecting disabled state."""

    def setup_method(self):
        """Reset logging state before each test."""
        _global_logging_disabled.set(False)
        _global_disabled_loggers.set([])

    def teardown_method(self):
        """Reset logging state after each test."""
        _global_logging_disabled.set(False)
        _global_disabled_loggers.set([])

    @pytest.mark.asyncio
    async def test_initialize_skips_pipe_transport_when_disabled(self) -> None:
        """LoggerStream.initialize() skips pipe transport setup when disabled."""
        from hyperscale.logging.streams.logger_stream import LoggerStream

        config = LoggingConfig()
        config.disable()

        stream = LoggerStream(name="test")
        await stream.initialize()

        # Should be marked as initialized
        assert stream._initialized is True
        # But no stream writers should be created
        assert len(stream._stream_writers) == 0

    @pytest.mark.asyncio
    async def test_log_returns_early_when_disabled(self) -> None:
        """LoggerStream._log() returns early when disabled."""
        from hyperscale.logging.streams.logger_stream import LoggerStream
        from hyperscale.logging.models import Entry, LogLevel as LogLevelModel

        config = LoggingConfig()
        config.disable()

        stream = LoggerStream(name="test")
        await stream.initialize()

        entry = Entry(message="test message", level=LogLevelModel.ERROR)

        # Should not raise even though stream writers aren't set up
        await stream._log(entry)

    @pytest.mark.asyncio
    async def test_log_to_file_returns_early_when_disabled(self) -> None:
        """LoggerStream._log_to_file() returns early when disabled."""
        from hyperscale.logging.streams.logger_stream import LoggerStream
        from hyperscale.logging.models import Entry, LogLevel as LogLevelModel

        config = LoggingConfig()
        config.disable()

        stream = LoggerStream(name="test")
        await stream.initialize()

        entry = Entry(message="test message", level=LogLevelModel.ERROR)

        # Should not raise even though nothing is set up
        await stream._log_to_file(entry)

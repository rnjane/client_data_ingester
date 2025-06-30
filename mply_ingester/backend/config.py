import importlib.util
from typing import Any, Optional
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


class ConfigError(Exception):
    """Custom exception for ConfigBroker when attempting to modify read-only config."""
    pass

class ConfigBroker:
    """A configuration class that loads settings from Python files and is read-only after initialization."""

    def __init__(self, filepaths: list[str]):
        self._config = {}
        self._input_files = filepaths
        for filepath in filepaths:
            self._load_from_file(filepath)
        self._locked = True  # Mark as read-only after initialization
        self._db_engine = None  # For engine reuse

    def _load_from_file(self, filepath: str) -> None:
        """
        Load configuration from a Python file.

        Args:
            filepath: Path to the Python configuration file

        Note:
            Later files in the list override earlier ones for the same key.
        """
        if self._locked:
            raise ConfigError("ConfigBroker is read-only after initialization.")
        spec = importlib.util.spec_from_file_location("config", filepath)
        if spec is None:
            raise ImportError(f"Could not load config from {filepath}")

        config_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config_module)

        # Extract all attributes that don't start with underscore
        for key in dir(config_module):
            if not key.startswith('_'):
                self._config[key] = getattr(config_module, key)

    def __getitem__(self, key: str) -> Any:
        """Get a configuration value using dictionary-style access."""
        return self._config[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """Prevent setting configuration values after initialization."""
        raise ConfigError("ConfigBroker is read-only after initialization.")

    def __contains__(self, key: str) -> bool:
        """Check if a configuration key exists."""
        return key in self._config

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value with an optional default.

        Args:
            key: The configuration key
            default: Default value if key doesn't exist

        Returns:
            The configuration value or default if key doesn't exist
        """
        return self._config.get(key, default)

    def keys(self):
        """Return all configuration keys."""
        return self._config.keys()

    def values(self):
        """Return all configuration values."""
        return self._config.values()

    def items(self):
        """Return all configuration key-value pairs."""
        return self._config.items()

    def update(self, other_dict: dict) -> None:
        """Prevent updating configuration after initialization."""
        raise ConfigError("ConfigBroker is read-only after initialization.")

    def clear(self) -> None:
        """Prevent clearing configuration after initialization."""
        raise ConfigError("ConfigBroker is read-only after initialization.")

    def __repr__(self) -> str:
        return f"ConfigBroker({self._input_files})"

    def __str__(self) -> str:
        return str(self._config)

    def get_session(self) -> Session:
        """
        Create and return a new SQLAlchemy session using the DATABASE_URI from the config.
        Reuses the same engine for all sessions from this ConfigBroker instance.
        Returns:
            Session: A new SQLAlchemy session.
        Raises:
            KeyError: If 'DATABASE_URI' is not present in the config.
        """
        if 'DATABASE_URI' not in self._config:
            raise KeyError("DATABASE_URI not found in config.")
        if self._db_engine is None:
            self._db_engine = create_engine(self['DATABASE_URI'])
        SessionLocal = sessionmaker(bind=self._db_engine)
        return SessionLocal()

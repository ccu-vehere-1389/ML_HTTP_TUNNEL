import os
import json
import logging
from logging import handlers
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler

class Http_tunnelConfigLoader:
    def __init__(self, config_path='/usr/local/etc/dictionaries/ml_http_tunnel_config/http_tunnel_config.json'):
        self.config_path = config_path
        self.config = self.load_config()  # load early so we can use values for logging
        # configure logging using the config value (or fallback)
        self.logger = self._configure_logging()

    def load_config(self):
        """Load JSON config and return dict or None on error (errors are not fatal)."""
        try:
            if not os.path.exists(self.config_path):
                # No log yet — use a temporary logger to report the issue
                print(f"Configuration file not found at {self.config_path}")
                return None

            with open(self.config_path, 'r') as config_file:
                cfg = json.load(config_file)
                return cfg
        except FileNotFoundError:
            print("Configuration file not found")
        except json.JSONDecodeError:
            print("Invalid JSON format in configuration file")
        except Exception as e:
            print(f"Unexpected error loading configuration: {e}")
        return None

    def _configure_logging(self):
        """Configure and return a logger using path from config (or fallback to stdout)."""
        logger = logging.getLogger("mlhttp_tunnel")
        # if already configured, just return existing logger
        if logger.handlers:
            return logger

        log_file_path = None
        if self.config:
            log_file_path = self.config.get("log_file_path")

        if log_file_path:
            os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
            handler = TimedRotatingFileHandler(
                filename=log_file_path,
                when="D",            # rotate every 'interval' days
                interval=30,          # 30 days ≈ 1 month
                backupCount=12        # keep last 12 logs ≈ 1 year
            )
            formatter = logging.Formatter(
                "%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.setLevel(logging.DEBUG)
        else:
            # fallback to console if no path in config
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s - %(levelname)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            )

        return logger

    # --- config getters ---
    def get_log_file_path(self):
        if not self.config:
            self.logger.error("Cannot extract log file path from a None configuration")
            return None
        return self.config.get('log_file_path')

    def get_raw_input_dir_path(self):
        if not self.config:
            self.logger.error("Cannot extract raw_input_dir_path from a None configuration")
            return None
        return self.config.get('http_tunnel_raw_input_dir')

    def get_preprocessor_dir_path(self):
        if not self.config:
            self.logger.error("Cannot extract preprocessor_dir_path from a None configuration")
            return None
        return self.config.get('http_tunnel_preprocessor_input_dir')

    def get_model_path(self):
        if not self.config:
            self.logger.error("Cannot extract model path from a None configuration")
            return None
        return self.config.get('http_tunnel_model_path')

    def get_threshold(self):
        if not self.config:
            self.logger.error("Cannot extract threshold from None Configuration")
            return None
        return self.config.get("threshold")

    def get_alerts_dir(self):
        if not self.config:
            self.logger.error("Cannot extract alerts dir from None Configuration")
            return None
        return self.config.get("alerts_dir")

    def main(self):
        """Convenience: return the tuple you used before (uses getters)."""
        return (self.get_log_file_path(),
                self.get_raw_input_dir_path(),
                self.get_preprocessor_dir_path(),
                self.get_model_path(),
                self.get_threshold(),
                self.get_alerts_dir())




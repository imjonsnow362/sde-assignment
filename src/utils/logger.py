import logging
import json
import time

class StructuredLogger:
    def __init__(self, name):
        self.logger = logging.getLogger(name)
        # Ensure we don't duplicate handlers if instantiated multiple times
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def _log(self, level, event_name, interaction_id=None, **kwargs):
        log_data = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": level,
            "event_name": event_name,
            "interaction_id": str(interaction_id) if interaction_id else "system",
            **kwargs
        }
        
        # Output as a JSON string for CloudWatch / Datadog parsing
        json_output = json.dumps(log_data)
        
        if level == "INFO":
            self.logger.info(json_output)
        elif level == "ERROR":
            self.logger.error(json_output)
        elif level == "WARN":
            self.logger.warning(json_output)

    def info(self, event_name, interaction_id=None, **kwargs):
        self._log("INFO", event_name, interaction_id, **kwargs)

    def error(self, event_name, interaction_id=None, **kwargs):
        self._log("ERROR", event_name, interaction_id, **kwargs)
        
    def warn(self, event_name, interaction_id=None, **kwargs):
        self._log("WARN", event_name, interaction_id, **kwargs)

def get_logger(name):
    return StructuredLogger(name)
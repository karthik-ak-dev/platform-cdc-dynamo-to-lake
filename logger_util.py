import uuid
import logging


class Logger(object):
    trace_event_id = None
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # Enable  for console logging - Dev
    # console_handler = logging.StreamHandler()
    # logger.addHandler(console_handler)

    @classmethod
    def set_trace_event_id(cls):
        cls.trace_event_id = f"logger-{uuid.uuid4().__str__()}"

    @staticmethod
    def info_log(msg):
        Logger.logger.info(f"\nEvent id: {Logger.trace_event_id} -- {str(msg)}")

    @staticmethod
    def error_log(msg):
        Logger.logger.error(f"\nEvent id: {Logger.trace_event_id} -- {str(msg)}")

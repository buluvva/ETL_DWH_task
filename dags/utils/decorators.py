import datetime as dt
import time

from utils.logs import configure_logger
from utils.classes import EventHandler

logger = configure_logger()
format_data = "%d/%m/%y %H:%M:%S.%f"


def logging(func):
    def wrapper(**args):
        event = EventHandler()
        start = time.time()
        try:
            source = args['source']
            destination = args['destination']

            event.level = 'INFO'
            start_date = dt.datetime.now()
            event.start_date = dt.datetime.strftime(start_date, format_data)
            event.dag_id = 'test_task'
            event.task_name = source + '_to_' + destination

            res = func(source, destination)

            event.error_text = None
            event.execution_time = time.time() - start
            event.handle_event()
        except Exception as ex:
            event.level = "ERROR"
            event.error_text = str(ex)
            event.execution_time = time.time() - start
            event.handle_event()

            raise ex
        return res
    return wrapper

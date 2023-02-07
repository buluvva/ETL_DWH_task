from utils.db import connect_to_postgres
from utils.logs import configure_logger

logger = configure_logger()


class EventHandler:
    def __int__(self):
        self.dag_id = ''
        self.task_name = ''
        self.level = ''
        self.start_date = ''
        self.error_text = ''
        self.execution_time = ''

    def handle_event(self):
        conn = connect_to_postgres('dwh')
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO dwh.public.logs_table("
                        "dag_id,"
                        "task_name,"
                        "log_level,"
                        "start_date,"
                        "execution_time,"
                        "error_text) "
                        "VALUES("
                        f"'{self.dag_id}',"
                        f"'{self.task_name}',"
                        f"'{self.level}',"
                        f"'{self.start_date}',"
                        f"{self.execution_time},"
                        f"'{self.error_text}')")
            logger.info('ADDING EVENT...')
            conn.commit()
            conn.close()
        except Exception as ex:
            logger.warning(f'RAISING EVENT ADDING ERROR...\n{ex}')
            conn.rollback()
            conn.close()
            raise ex

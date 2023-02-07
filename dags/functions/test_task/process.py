from typing import List

from utils.db import connect_to_postgres
from utils.logs import configure_logger
from utils.decorators import logging
from utils.classes import EventHandler


logger = configure_logger()

INSERT_FACT = "INSERT INTO {db}.public.{db}_fact_{table} VALUES{values}"
INSERT_DIM = "INSERT INTO {db}.public.{db}_dim_{table} VALUES{values}"
GET_DELTA = "SELECT {query} FROM {db}.public.{db}_{dim}_{table} {where}"

event = EventHandler()


def grab_source_delta(source: str, table: str, watermark: int) -> [List[tuple], int]:
    source_db = connect_to_postgres(source)
    cur = source_db.cursor()

    cur.execute(GET_DELTA.format(query='MAX(id)', db=source, dim='fact', table=table, where=';')) \
        if table == 'sales' \
        else cur.execute(GET_DELTA.format(query='MAX(id)', db=source, dim='dim', table=table, where=';'))

    new_mark = cur.fetchone()[0]

    cur.execute(GET_DELTA.format(query='*', db=source, dim='fact', table=table, where=f'WHERE id > {watermark}')) \
        if table == 'sales' \
        else cur.execute(GET_DELTA.format(query='*', db=source, dim='dim', table=table,
                                          where=f'WHERE id > {watermark}'))

    delta = cur.fetchall()
    source_db.close()
    return delta, new_mark


@logging
def push_delta(source: str, destination: str):
    logger.info(f'Starting ETL: {source} -> {destination}')

    destination_db = connect_to_postgres(destination)
    cur = destination_db.cursor()

    cur.execute(f"""SELECT table_name, table_cols, last_update 
                    FROM {destination}.public.{destination}_high_water_mark """)
    data = cur.fetchall()

    logger.info('Got high_water_mark, started inserting...')
    for row in data:

        table_name, cols, watermark = row

        try:
            tuples, new_mark = grab_source_delta(source, table_name, watermark)
            if len(tuples) == 0:
                logger.info(f'No data for {table_name}, passing...')
                continue

            insert_query = INSERT_FACT if table_name == 'sales' else INSERT_DIM
            for values in tuples:
                cur.execute(insert_query.format(db=destination, table=table_name, values=values))

            cur.execute(f"""UPDATE {destination}.public.{destination}_high_water_mark  
                            SET last_update = {new_mark} 
                            WHERE table_name = '{table_name}'""")
            logger.info(f"Inserted {table_name} data")
            destination_db.commit()
        except Exception as s:
            destination_db.rollback()
            logger.warning(f'Inserting failed:  {s}')

    destination_db.close()
    logger.info('Done!')

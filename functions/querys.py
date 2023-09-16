from config.config import SERVER_AON, STEP_03, STEP_04, STEP_05, STEP_06
from functions.connection_db import connect_database
from config.log import LOGGER

initial_path = 'tools/scripts/'
scripts = [
    '01_Create_Table.sql',
    '02_Insertar_Datos.sql',
    '03_Insert_Datos.sql',
    '04_Insert_Datos_ISAPRE.sql',
    '05_Insert_Datos_Planillas.sql',
    '06_Insert_Datos_Final.sql'
]


def resolve_rs(rs):
    if type(rs) == str and rs.startswith('Exception: '):
        return 0
    else:
        return 1


def drop_table(database, table, log_step=None, type=''):
    table_db = f'{database}..{table}'
    LOGGER.info(f'[DROP] Iniciando eliminación de tabla {table} en la base de datos {database}...')
    if type == 'log':
        instance_id = log_step(execute_sp=execute_sp, id=STEP_03[0], type='start')

    query = f"""
    IF(OBJECT_ID('{table_db}')IS NOT NULL)
         DROP TABLE {table_db}
    """
    resp = connect_database(database, query, 0)

    if type == 'log':
        log_step(
            execute_sp=execute_sp, id=STEP_03[0],
            instance_id=instance_id, statusProc=resolve_rs(resp),
            msj=resp, nameStep=STEP_03[1]
        )
    LOGGER.info(f'[DROP] Eliminación de tabla {table} finalizada')


def select(data_base, table, type=1, server=SERVER_AON, scheme='dbo', where='', order_by=''):
    LOGGER.info(
        f'[SELECT] Iniciando obtención de valores en la tabla {table} de la base de datos {data_base}...')
    table_db = f'{data_base}.{scheme}.{table}'
    query = f"""
    SELECT * FROM {table_db} 
    {where} 
    {order_by}
    """
    result = connect_database(data_base, query, type, None, None, server)
    LOGGER.info(f'[SELECT] Obtención de valores en la tabla {table} finalizada.')

    return result


def create_table(database, table, log_step):
    table_db = f'{database}..{table}'
    LOGGER.info(f'[CREATE] Iniciando creacióm de tabla {table} en la base de datos {database}...')
    instance_id = log_step(execute_sp=execute_sp, id=STEP_04[0], type='start')

    query = f"""
    CREATE TABLE {table_db} (
        rut_pagador INT,
        dv CHAR(1)
    )

    CREATE CLUSTERED INDEX idx_info_usu ON {table_db} (rut_pagador)
    """
    resp = connect_database(database, query, 0)

    log_step(
        execute_sp=execute_sp, id=STEP_04[0],
        instance_id=instance_id, statusProc=resolve_rs(resp),
        msj=resp, nameStep=STEP_04[1].format(name=table_db)
    )
    LOGGER.info(f'[CREATE] Creación de tabla {table} finalizada')


def insert_into(database, table, data, log_step):
    table_db = f'{database}..{table}'
    LOGGER.info(f'[INSERT] Iniciando inserción de data en la tabla {table} en la base de datos {database}...')
    instance_id = log_step(execute_sp=execute_sp, id=STEP_05[0], type='start')

    resp = connect_database(database, None, 2, data, table_db)

    log_step(
        execute_sp=execute_sp, id=STEP_05[0],
        instance_id=instance_id, statusProc=resolve_rs(resp),
        msj=resp, nameStep=STEP_05[1].format(name=table_db)
    )
    LOGGER.info(f'[INSERT] Inserción de datos finalizada en la tabla {table}.')


def execute_script(database, log_step):
    LOGGER.info(f'[EJECUCIÓN SCRIPT SQL] Empezando ejecución de script base de datos...')
    log = 1
    for script in scripts:
        if log == 1:
            instance_id = log_step(execute_sp=execute_sp, id=STEP_06[0], type='start')

        LOGGER.info(f'EJECUTANDO SCRIPT Nro 0{log} con nombre {script} ...')
        resp = connect_database(database, open(file=initial_path+script).read(), 0)
        if log == 1:
            log_step(
                execute_sp=execute_sp, id=STEP_06[0], instance_id=instance_id,
                statusProc=resolve_rs(resp), msj=resp, nameStep=STEP_06[1]
            )
        if resolve_rs(resp) == 0:
            quit()
        log += 1
    LOGGER.info(f'[EJECUCIÓN SCRIPT SQL] Fin de script base de datos')


def execute_sp(database, sp, server):
    response = connect_database(database, sp, None, None, None, server)
    return response

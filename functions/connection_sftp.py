import pysftp

from paramiko import AuthenticationException, SSHException
from config.log import LOGGER
from config.config import STEP_01, STEP_07, host, username, password, port


cnopts = pysftp.CnOpts()
cnopts.hostkeys = None 

def resolve_pos_connection(type):
    if type == 'firstConnection':
        return 'primera'
    elif type == 'secondConnection':
        return 'segunda'
    else:
        return ''

def resolve_step_id_sftp(type):
    if type == 'firstConnection':
        return STEP_01
    elif type == 'secondConnection':
        return STEP_07

def connect_sftp(log_step, execute_sp, expected_function, t_sftp, input_file_name="") :
    LOGGER.info(f'Iniciando {resolve_pos_connection(t_sftp)} conexión al servidor sftp "{host}"')

    id_sftp= resolve_step_id_sftp(t_sftp)
    
    instance_id_sftp= log_step(execute_sp=execute_sp, id=id_sftp[0], type='start') 
    
    try:
        with pysftp.Connection(host=host, username=username, password=password, port=port, cnopts=cnopts) as conn:
            log_step(execute_sp=execute_sp, id=id_sftp[0], instance_id=instance_id_sftp, nameStep=id_sftp[1].format(host=host))
            if t_sftp == 'secondConnection':
                response= expected_function(conn, input_file_name)
            else:
                response= expected_function(conn)
            conn.close()
        
        return response

    except (AuthenticationException, SSHException, PermissionError, TypeError, NameError, ImportError, IndexError) as e:
        LOGGER.exception(e)
        log_step(
            execute_sp=execute_sp, id=id_sftp[0], instance_id=instance_id_sftp,
            nameStep=id_sftp[1].format(host=host), statusProc=0, msj= "Exception: " + str(e)
            )
    
    finally:
        LOGGER.info(f'Terminando {resolve_pos_connection(t_sftp)} conexión al servidor sftp "{host}"')
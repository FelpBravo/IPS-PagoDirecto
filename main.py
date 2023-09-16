from functions.connection_sftp import connect_sftp
from functions.controlProcesses import main_script, log_step
from functions.send_mail import send_mail
from functions.delivery_records import local_entered, local_outputs, local_processed, remote_outputs
from functions.querys import drop_table, create_table, execute_sp, insert_into, execute_script, select
from config.config import BD_INFO_TEMP, COLUMN_NUM_EMP, COLUMN_NUM_TRAB, ID_PROCESO, LOCAL_ENTRY_PATH, NAME_INPUT_FILE, SP, STEP_00, STEP_09, TBL_FASE01, TBL_PRE_PILOTO
from config.log import LOGGER

from paramiko import AuthenticationException, SSHException
import os
import pysftp
import pandas


LOGGER.info(f'[INICIO] SCRIPT PAGO DIRECTO ASIGNACIÓN FAMILIAR IPS')

instanceMain_id = log_step(execute_sp=execute_sp, type='start')
run_py_script = main_script(log_step, execute_sp, local_entered)
cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
id = int(ID_PROCESO)
df_bitacora = None
count = 0
input_file_name = run_py_script
reg_in = 0
reg_out = 0


def validate_input_name(name):
    if name is not None:
        if len(name) == len(NAME_INPUT_FILE):
            return True
        else:
            return
    else:
        return


try:

    if validate_input_name(input_file_name):

        for table in [TBL_PRE_PILOTO, TBL_FASE01]:
            drop_table(BD_INFO_TEMP, table, log_step, type='log' if count == 0 else '')
            count += 1

        create_table(BD_INFO_TEMP, TBL_PRE_PILOTO, log_step)

        file = open(os.getcwd() + LOCAL_ENTRY_PATH + input_file_name, 'r')
        content_file = pandas.read_csv(file, sep=';', header=None)
        n_rows_input, _ = content_file.shape
        insert_into(BD_INFO_TEMP, TBL_PRE_PILOTO, content_file, log_step)
        file.close()

        execute_script(BD_INFO_TEMP, log_step)

        df_response = select(BD_INFO_TEMP, TBL_FASE01)
        n_rows_output, _ = df_response.shape
        num_emp = str(len(df_response[COLUMN_NUM_EMP].unique()))
        num_trab = str(len(df_response[COLUMN_NUM_TRAB].unique()))
        local_outputs(df_response, input_file_name)

        connect_sftp(log_step, execute_sp, remote_outputs,
                     t_sftp='secondConnection', input_file_name=input_file_name)

        local_processed(input_file_name)

        # Armado de mails.
        instance_id_mails = log_step(execute_sp=execute_sp, id=STEP_09[0], type='start')
        for t in ['bitacora', 'detalle']:
            resp = send_mail(
                fileName=input_file_name, reg_in=str(n_rows_input), reg_out=str(n_rows_output),
                num_emp=num_emp, num_trab=num_trab, type=t
            )
        log_step(
            execute_sp=execute_sp, id=STEP_09[0], instance_id=instance_id_mails,
            statusProc=1 if resp is None else 0,
            nameStep=STEP_09[1],
            msj='' if resp is None else resp
        )

except (AuthenticationException, FileNotFoundError, SSHException, PermissionError, TypeError, NameError, ImportError, IndexError) as e:
    log_step(execute_sp=execute_sp, instance_id=instanceMain_id,
             statusProc=0, msj="Exception: " + str(e), nameStep=STEP_00[1])
    LOGGER.exception(e)
else:
    if validate_input_name(input_file_name) is None:
        LOGGER.error(input_file_name)
        log_step(
            execute_sp=execute_sp, instance_id=instanceMain_id, statusProc=0, nameFileInput=' ',
            reg_out='0', reg_in='0', num_emp='0', num_trab='0', nameStep=STEP_00[1], msj=input_file_name
        )
    else:
        log_step(
            execute_sp=execute_sp, instance_id=instanceMain_id, statusProc=1, nameFileInput=input_file_name,
            reg_out=str(n_rows_output), reg_in=str(n_rows_input), num_emp=str(num_emp), num_trab=str(num_trab),
            nameStep=STEP_00[1], msj=''
        )
finally:
    [drop_table(BD_INFO_TEMP, table) for table in [TBL_PRE_PILOTO, TBL_FASE01]]

LOGGER.info(f'[FIN] SCRIPT PAGO DIRECTO ASIGNACIÓN FAMILIAR IPS')

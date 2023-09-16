
from functions.connection_sftp import connect_sftp
from functions.querys import select
from config.config import BD_INTEGRA, ID_PROCESO, LEN, LOCAL_PROCESSED_PATH, MODE, NAME_INPUT_FILE, SERVER_INTEGRA, SP_INS, SP_UPD, YYYYMMDD, to_xml
from config.log import LOGGER
import os


def log_step(execute_sp, id=ID_PROCESO, type='', statusProc=1, instance_id=0,
            nameStep='', msj='', reg_out='0', nameFileInput='', reg_in='0',
            num_emp='0', num_trab='0'):
    
    if statusProc == 1:
        statusXml=True
    else:
        statusXml=False

    if type == 'start':
        instance_id= execute_sp(BD_INTEGRA, SP_INS.format(id, 0), SERVER_INTEGRA)
        return instance_id
    else:
        xml= to_xml(
            reg_out= reg_out,
            status= statusXml, 
            message= str(msj).replace("'",""), 
            nameProc= nameStep, 
            idProc= id, 
            idProcM= ID_PROCESO,
            nameFileInput= nameFileInput,
            reg_in= reg_in,
            num_emp=num_emp,
            num_trab=num_trab
            )
        sp_end= f'''{SP_UPD} {instance_id}, {str(statusProc)}, '{xml}',0, 0, 0, NULL'''
        execute_sp(BD_INTEGRA, sp_end, SERVER_INTEGRA) 
        

def main_script(fx_log_step, fx_execute_sp, fx_local_entered):
    LOGGER.info('Iniciando validación de dia habil. (EJECUCIÓN DE SCRIPT DEPENDE DE ESTO)..........')
    start_day= '15'
    now_date= f'{YYYYMMDD[:-2]}16' if MODE=='QA' else YYYYMMDD #  YYYYMMDD ### ORIGINAL ## PRUEBA ##  f'{YYYYMMDD[:-2]}31'
    processed_files= os.getcwd() + LOCAL_PROCESSED_PATH
    
    calendar= select(BD_INTEGRA, 'Calendario' ,server=SERVER_INTEGRA)
    
    today= calendar[
        (calendar["dia_char"] >= f'{YYYYMMDD[:-2]}{start_day}') & (calendar["dia_habil"] == True)
        ].sort_values(
            by=["dia_char"], ascending=True
            ).reset_index(drop=True)
    
    if now_date in list(today["dia_char"]): 
        if os.path.exists(processed_files): 
            if NAME_INPUT_FILE[:LEN].lower() not in [f[:LEN].lower() for f in list(os.listdir(processed_files))]: 
                if len(today[today["dia_char"] <= now_date]) >=2:  
                    
                    input= connect_sftp(fx_log_step, fx_execute_sp, fx_local_entered, t_sftp='firstConnection') 
                    
                    if len(input) == len(NAME_INPUT_FILE):
                        return input
                    else: 
                        return input 
                else:
                    return f'Segun la lógica de programación. Hoy con fecha {now_date} no corresponde procesar el archivo de entrada.'
            else:
                return f'En el directorio local "{LOCAL_PROCESSED_PATH}" ya existe un archivo de entrada procesado para el periodo {now_date[:-2]}' 
        else: 
            return f'No existe el directorio local "{LOCAL_PROCESSED_PATH}". Resolver problema lo antes posible.' 
    else:
        return f'({now_date}). El script solo se ejecuta los dias habiles despues del día {start_day}. Hoy es ({now_date})'
    

  

from functions.send_mail import send_mail
from functions.querys import execute_sp
from functions.controlProcesses import log_step
from config.config import START_INPUT, STEP_02, STEP_08, host, LEN, LOCAL_ENTRY_DIR, LOCAL_ENTRY_PATH, LOCAL_OUPUT_DIR, LOCAL_OUPUT_PATH, LOCAL_PROCESSED_DIR, LOCAL_PROCESSED_PATH, LOCAL_REPORTS_DIR, NAME_INPUT_FILE, NAME_OUTPUT_FILE, REMOTE_HOME_PATH, REMOTE_INPUT_PATH, REMOTE_OUTPUT_DIR, REMOTE_OUTPUT_PATH, TYPE
from config.log import LOGGER

import os
import shutil
import pathlib


#Función encargada de validar que el archivo de entrada exista dentro del servidor remoto.
def local_entered(ftp):
    LOGGER.info(f'Buscando archivo de entrada en el servidor SFTP para copiarlo a la ruta local...') 
    instance_id= log_step(execute_sp=execute_sp, id=STEP_02[0], type='start')

    remote_files_txt = []
    count = 0
    response = False
    remotePath_input = REMOTE_HOME_PATH + REMOTE_INPUT_PATH 
    localPath_input = os.getcwd() + LOCAL_ENTRY_PATH 
    
    try:
        l_validate_existence_dir(LOCAL_ENTRY_DIR, LOCAL_ENTRY_PATH)
        
        LOGGER.info(f'Validando nombre y existencia del archivo de entrada...') 
        list_files = ftp.listdir(REMOTE_HOME_PATH + REMOTE_INPUT_PATH)
        if len(list_files) > 0:
            for file in list(list_files):
                if pathlib.Path(file).suffix == TYPE:
                    remote_files_txt.append(file)
            if len(remote_files_txt) > 0:
                for file in list(remote_files_txt):
                    if file[:LEN].lower() == NAME_INPUT_FILE[:LEN].lower():
                        ftp.get(remotePath_input + file, localPath_input + file) #Agregar validación
                        response=file
                        count+=1
                if response is False:
                    response=f'El archivo de entrada no se encuentra en la ruta remota {remotePath_input}'
                if count > 1:
                    response=f'Existe mas de un archivo de entrada con el mismo periodo(mes) para la ruta remota {remotePath_input}' 
            else:
                response=f'No existe un archivo de entrada con la extensión esperada "{TYPE}" en el directorio remoto {remotePath_input}'
        else:
            response=f'No existe un archivos dentro del directorio remoto {remotePath_input}'


        if response is not None and len(response) == len(NAME_INPUT_FILE):
            log_step(execute_sp=execute_sp, id=STEP_02[0], instance_id=instance_id, nameStep=STEP_02[1].format(host=host))
            return response

        elif response is not None:
            send_mail(msj=response, type='error')
            LOGGER.error(f'Error {response}')
            log_step(
                execute_sp=execute_sp, id=STEP_02[0], instance_id=instance_id,
                nameStep=STEP_02[1].format(host=host), statusProc=0, msj=str(response)
                )
            return response

    except FileNotFoundError as e:
        LOGGER.exception(e)
        log_step(
                execute_sp=execute_sp, id=STEP_02[0], instance_id=instance_id,
                nameStep=STEP_02[1].format(host=host), statusProc=0, 
                msj= "Exception: " + str(e) + " Nombre función: local_entered()"
            )
    finally:
        LOGGER.info(f'Validación y recate de archivo de entrada finalizada.')


#Función encargada de guardar el archivo de entrada en el directorio local /Procesados
def local_processed(input_file_name):
    LOGGER.info(f'Moviendo archivo de entrada local a la carpeta {LOCAL_PROCESSED_PATH}') 
    try:
        localPath_processed_file = os.getcwd() + LOCAL_PROCESSED_PATH + input_file_name
        localPath_input_file = os.getcwd() + LOCAL_ENTRY_PATH + input_file_name

        l_validate_existence_dir(LOCAL_PROCESSED_DIR, LOCAL_PROCESSED_PATH)

        shutil.move(localPath_input_file, localPath_processed_file)

        LOGGER.info(f'Función de procesados locales finalizada.') 
    except FileNotFoundError as e:
        LOGGER.error(str(e) + " ERROR al copiar el archivo de salida al directorio " +
                                    str(localPath_processed_file) + " en función local_processed()")
        quit()


#Función encargada de eliminar salidas antiguas dentro del directorio local /Procesados, para añadir las salidas actuales
def local_outputs(df, input_file_name):
    LOGGER.info('Preparando salidas locales...')
    localPath_ouputs = os.getcwd() + LOCAL_OUPUT_PATH + START_INPUT + input_file_name[-13:]
    try:
        l_validate_existence_dir(LOCAL_OUPUT_DIR, LOCAL_OUPUT_PATH)
        df.to_csv(localPath_ouputs, sep=";", index=False, index_label=False)

    except ValueError as e:    
        LOGGER.error(e)  
    finally:
        LOGGER.info('Preparación de salidas locales finalizada.')


#Función encargada de registrar salidas en el directorio remoto, dentro de /Informes
def remote_outputs(ftp, input_file_name):
    LOGGER.info('Preparando salidas remotas...')
    instance_id= log_step(execute_sp=execute_sp, id=STEP_08[0], type='start')
    localPath_outputs = os.getcwd() + LOCAL_OUPUT_PATH + START_INPUT + input_file_name[-13:]
    remotePath_output = REMOTE_HOME_PATH + REMOTE_OUTPUT_PATH + START_INPUT + input_file_name[-13:]
    try:
        
        r_validate_existence_dir(ftp, REMOTE_HOME_PATH, REMOTE_OUTPUT_PATH, REMOTE_OUTPUT_DIR)
        ftp.put(localPath_outputs, remotePath_output)
   
    except FileNotFoundError as e:
        LOGGER.exception(e)
        log_step(
                execute_sp=execute_sp, id=STEP_08[0], instance_id=instance_id,
                nameStep=STEP_08[1].format(host=host), statusProc=0, 
                msj= "Exception: " + str(e) + " Nombre función: remote_outputs()"
             )
        quit()
    else:
        log_step(
            execute_sp=execute_sp, id=STEP_08[0], instance_id=instance_id, 
            nameStep=STEP_08[1].format(host=host)
            )
    finally:
        LOGGER.info('Preparación de salidas remotas finalizada.')


#Funciónes encargada del validar existencia de directorios.
#REMOTO
def r_validate_existence_dir(ftp, home_path, dir_path, dir_name):
    LOGGER.info(f'Validando existencia de directorios de entrada remotos...') 
    if not [dir for dir in ftp.listdir(home_path) if dir == dir_name]:
        LOGGER.info(f'Creando directorio remoto {dir_name}...')
        ftp.mkdir(home_path + dir_path.rstrip(dir_path[-1])) 
#LOCAL
def l_validate_existence_dir(dir_name, dir_path):
    LOGGER.info(f'Validando existencia de directorios de entrada locales...') 
    try:
        if not [dir for dir in os.listdir(os.getcwd() + LOCAL_REPORTS_DIR) if dir == dir_name]:
            LOGGER.info(f'Creando directorio local {dir_name}...') 
            os.mkdir(os.getcwd() + dir_path.rstrip(dir_path[-1])) 
    except FileExistsError as e:
        LOGGER.info(e)
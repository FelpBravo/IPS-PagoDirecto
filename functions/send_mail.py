from functions.querys import select
from config.log import LOGGER
from config.config import BD_INTEGRA, ERROR_SUBJECT, FROM_USER, HTML_BIT_PATH, HTML_DET_PATH, HTML_ERROR_PATH, ID_PROCESO, NOMBRE_PROCESO, PASS, PORT, SERVER_INTEGRA, SMTP_SERVER, START_INPUT, SUBJECT_DETAIL, TABLE_PROCESOS_GESTION_CONTACTOS, SUBJECT, TO_USER_DETAIL, USER
import sys
import smtplib
import os
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

EMAIL = 'email'
message = ''
subject = ''
destinations = ''
contacts = select(
    data_base=BD_INTEGRA,
    table=TABLE_PROCESOS_GESTION_CONTACTOS,
    server=SERVER_INTEGRA,
    scheme='cuadre',
    where=f'WHERE id_proceso={ID_PROCESO}'
)
contacts_for_success = contacts[(contacts['tipo_envio'] == 1)]
contacts_for_error = contacts[(contacts['tipo_envio'] == 0)]


def send_mail(fileName='', msj='', reg_in=0, reg_out=0, num_emp=0, num_trab=0, type='bitacora'):
    try:
        if type == 'bitacora':
            LOGGER.info('Iniciando envio de EMAIL con la bitacora...')
            message = open(
                HTML_BIT_PATH, 'r', encoding='utf8'
            ).read().format(
                c01=START_INPUT + "_" + fileName[28:-4], c02=str(reg_in),
                c03=str(reg_out), date=datetime.today().strftime('%m/%d/%Y')
            )
            subject = SUBJECT
            destinations_for_bitacora = contacts_for_success[~contacts_for_success[EMAIL].isin(
                TO_USER_DETAIL[:-1])]
            destinations = ','.join(list(destinations_for_bitacora[EMAIL]))

        elif type == 'detalle':
            LOGGER.info('Iniciando envio de EMAIL con el detalle de facturación...')
            message = open(
                HTML_DET_PATH, 'r', encoding='utf8'
            ).read().format(
                c01=fileName[28:-6], c02=NOMBRE_PROCESO, c03=str(num_emp),
                c04=str(num_trab), c05=str(reg_out), date=datetime.today().strftime('%m/%d/%Y')
            )
            subject = SUBJECT_DETAIL
            destinations_for_billing = contacts_for_success[contacts_for_success[EMAIL].isin(TO_USER_DETAIL)]
            destinations = ','.join(list(destinations_for_billing[EMAIL]))

        elif type == 'error':
            LOGGER.info('Iniciando envio de EMAIL con aviso de ERROR...')
            message = open(
                HTML_ERROR_PATH, 'r', encoding='utf8'
            ).read().format(
                message=msj,
                date=datetime.today().strftime('%m/%d/%Y')
            )
            subject = ERROR_SUBJECT
            destinations = ','.join(list(contacts_for_error[EMAIL]))

        content = MIMEMultipart("alternative")
        content['From'] = FROM_USER
        content['To'] = destinations
        content['Subject'] = subject
        content.attach(MIMEText(message, "html"))

        resp = connect_mail(content, FROM_USER, destinations)

        LOGGER.info(f'Finalizando envio de EMAIL a destinatarios "{destinations}".')
        return resp

    except (FileNotFoundError, AttributeError, KeyError) as e:
        LOGGER.exception(e)


def connect_mail(content, from_u, to_u):
    LOGGER.info('Iniciando conexión de EMAIL con el servidor ' + SMTP_SERVER)

    try:

        with smtplib.SMTP(SMTP_SERVER, PORT) as smtp:
            LOGGER.info('Esperando conexión con el servidor ' + SMTP_SERVER)
            smtp.connect(SMTP_SERVER, PORT)

            LOGGER.info('Conectado a  ' + SMTP_SERVER + '.........')
            if sys.platform.startswith('win32'):
                smtp.ehlo()
                smtp.starttls()

            LOGGER.info('Iniciando sesión en ' + FROM_USER + '.........')
            smtp.login(USER, PASS)
            LOGGER.info('Sesión iniciada con éxito en ' + FROM_USER)

            # En caso de que se requieran agregar con CC#
            # smtp.sendmail(from_addr=from_u, to_addrs=to_u + cc_u, msg=content.as_string())
            #
            smtp.sendmail(from_addr=from_u, to_addrs=to_u.split(","), msg=content.as_string())
            LOGGER.info('Correo enviado con éxito')

            smtp.quit()

        LOGGER.info('Finalizando conexión de sesión EMAIL cerrada en ' + from_u)
        return

    except (smtplib.SMTPAuthenticationError, smtplib.SMTPResponseException) as e:
        LOGGER.exception(e)
        return "Exception: " + str(e)

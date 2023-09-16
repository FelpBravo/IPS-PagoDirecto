# Proyecto IPS - Pago Directo

Este proyecto es un script Python diseñado para el sistema operativo Linux. Se ejecuta automáticamente mediante un cron de Linux que verifica si es un día hábil antes de ejecutar el proyecto. Su principal función es validar archivos de entrada, conectarse a una base de datos, ejecutar procedimientos almacenados, recibir información, depositar archivos en una casilla SFTP y enviar informes por correo electrónico a la institución correspondiente.

## Características Clave

- **Ejecución Automática**: El script se ejecuta automáticamente a través de un cron de Linux. Antes de ejecutarse, verifica que sea un día hábil.

- **Validación de Archivos de Entrada**: El proyecto valida archivos de entrada que cumplen con ciertos criterios.

- **Conexión a Base de Datos**: Se conecta a una base de datos y ejecuta procedimientos almacenados (SP) para procesar los datos.

- **Transferencia SFTP**: Deposita archivos en una casilla SFTP para su posterior uso.

- **Envío de Correo Electrónico**: Envía informes por correo electrónico a la institución correspondiente con los resultados del procesamiento.

## Uso del Proyecto

El script se ejecuta automáticamente según la programación del cron de Linux. Asegúrate de tener todas las dependencias necesarias instaladas y configuradas correctamente para su correcto funcionamiento.

## Configuración

- Asegúrate de configurar las variables necesarias en el script y en la configuración del cron de Linux para que el proyecto se ejecute según tus requerimientos.

- Añade las credenciales de SFTP y configuración de correo electrónico en los archivos correspondientes.

## Dependencias

El proyecto utiliza diversas dependencias de Python, como Pandas, Pysftp, SQLAlchemy y otras. Asegúrate de tener estas dependencias instaladas antes de ejecutar el script.

## 
**Nota Importante**: Los archivos sensibles no han sido subidos al repositorio o han sido ocultados por razones de seguridad.

Estos archivos contienen información confidencial, como contraseñas o claves de acceso, y no deben estar disponibles públicamente en un repositorio de código abierto.


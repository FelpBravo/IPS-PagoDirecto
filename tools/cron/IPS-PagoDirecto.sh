#!/usr/bin/bash

echo "Activando entorno virtual"
source /previred/IPS-PagoDirecto/.env/bin/activate

echo "Dirijiendo a directorio /previred/IPS-PagoDirecto"
cd /previred/IPS-PagoDirecto

echo "Iniciando script IPS-PagoDirecto"
python3 main.py

echo "Desactivando entorno virtual"
deactivate

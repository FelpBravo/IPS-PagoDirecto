import logging as LOGGER

FORMAT = '[%(asctime)s]::%(filename)s.::.%(funcName)s:%(levelname)s:   %(message)s::.[%(lineno)d]'
LOGGER.basicConfig(filename='config/app.log', level='INFO', format=FORMAT)


from config.config import PWD, SERVER_AON, UID
from config.log import LOGGER

import pymssql 
import pandas



def connect_database(data_base, query=None, type=None, data=None, table_db=None, server=SERVER_AON):
    conn = pymssql.connect(server, UID, PWD, data_base)
    cursor = conn.cursor()

    try:
            
        if type is None:
            cursor.execute(query)
            if cursor.description is not None:
                resp = list(cursor.fetchall()[0])[0] #Ejemplo salida fetchall [(3263,)]
                conn.commit()
                return resp
            else: 
                conn.commit()
                return 
        if type == 1:
            return pandas.read_sql_query(query, conn) 
        if type == 2:
            for i,row in data.iterrows():
                query = "INSERT INTO " + table_db + " VALUES (" + "%d,"*(len(row)-1) + "%d)"
                LOGGER.info(f'Insertando registros de la posición "{[tuple(row),i][1]}"... {str([tuple(row),i][0])}')
                cursor.executemany(query, [[tuple(row),i][0]])
            conn.commit()    
        else:
            cursor.execute(query)
            conn.commit()

         
    except (pymssql.ProgrammingError, pymssql.DataError, pymssql._pymssql.OperationalError, pymssql.Error, TypeError) as e:
        LOGGER.exception(e)
        return "Exception: " + str(e)
        
    finally:
        conn.close()
        cursor.close()  
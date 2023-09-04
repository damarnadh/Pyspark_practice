import cx_Oracle
import config

connection = None

try:
    connection = cx_Oracle.connect(
        config.username,
        config.password,
        cx_Oracle.makedsn(host='localhost',port='1521',service_name='XE'),
        #config.dsn,
        encoding=config.encoding)

    connection.cursor().execute('insert into pyspark_table_a values(:seq_num,:person_name)',[1,'damar'])

    connection.commit()




    # show the version of the Oracle Database
    print(connection.version)
except cx_Oracle.Error as error:
    print(error)
finally:
    # release the connection
    if connection:
        connection.close()
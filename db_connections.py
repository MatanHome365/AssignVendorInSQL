import pyodbc
import psycopg2
import os
import pandas as pd

# client = boto3.client('s3')


server = 'projectdb.cvwpst8ibfx9.us-east-1.rds.amazonaws.com'
# database = 'ProjectsProduction'
database = 'ProjectTestProd'
port = 5432
user = 'afik365'
password = '!qwde45ttA'
send_mail_queue_url = 'https://sqs.us-east-1.amazonaws.com/090922436798/emailProd'

HOST = '172.31.66.17'
DBNAME = 'HOME365_REP'
USER = 'postgres'
PASSWORD = 'fKnHDLme37Qm^Mc'



def importDataFromPG(query, host=HOST,
                     database=DBNAME, user=USER, password=PASSWORD):
    print('[INFO]: Connecting to SQL')
    conn = psycopg2.connect(host=host,
                            database=database, user=user, password=password)
    print('[INFO]: Connected to SQL...')
    data = pd.read_sql_query(query, conn)
    return data


def connectoToSQL():
    server = '172.31.25.110'
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + user + ';PWD=' + password)


def ImportPropertiesFromSql(query):
    # print('[INFO]: Connecting to SQL...')
    print(logObject('[INFO]: Conneting to SQL', 'ImportPropertiesFromSql', 'Connecting to SQL'))
    cnxn = connectoToSQL()
    # print('[INFO]: Connected Successfully.')
    print(logObject('[INFO]: Connected Successfully to SQL', 'ImportPropertiesFromSql', 'Connecting to SQL'))
    sql = query
    print(logObject('[INFO]: Loading data as Pandas dataframe', 'ImportPropertiesFromSql', 'Convrting to dataframe'))
    # print('[INFO]: Loading data as Pandas DataFrame')
    data = pd.read_sql(sql, cnxn)
    print(logObject('[INFO]: Closing Connection to SQL...', 'ImportPropertiesFromSql', 'Connecting to SQL'))
    # print('[INFO]: Closing Connection...')
    cnxn.close()
    return data
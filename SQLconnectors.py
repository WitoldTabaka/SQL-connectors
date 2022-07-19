import MySQLdb
import sshtunnel
import pandas as pd

sshtunnel.SSH_TIMEOUT = 5.0
sshtunnel.TUNNEL_TIMEOUT = 5.0

#method to select query in MySQL by SSH
def ssh_MySQL_connector_select(userNameSSH,passSSH,ipAddressSSH,portSSH,userNameDB,passDB,dbName,query):
    # userNameSSH = "string"
    # passSSH = "string"
    # ipAddressSSH = "xxx.xxx.x.xxx"
    # portSSH = int
    # userNameDB = "string"
    # passDB = "string#"
    # dbName = "string"
    # query = '''SELECT * from table_name'''
    data = pd.DataFrame()

    with sshtunnel.SSHTunnelForwarder(
        (ipAddressSSH, portSSH),
        ssh_username= userNameSSH , ssh_password=passSSH,
        remote_bind_address=('127.0.0.1', 3306)
    ) as tunnel:
        try:
            connection = MySQLdb.connect(
                user=userNameDB,
                passwd=passDB,
                host='127.0.0.1', port=tunnel.local_bind_port,
                db= dbName
            )

            data = pd.read_sql_query(query, connection)
        except BaseException as e:
            print('problem: ', e)
            return (data, False)

        connection.close()
        return (data, True)

#method to insert query in MySQL by SSH
def ssh_MySQL_connector_insert(userNameSSH,passSSH,ipAddressSSH,portSSH,userNameDB,passDB,dbName,query):
    # userNameSSH = "string"
    # passSSH = "string"
    # ipAddressSSH = "xxx.xxx.x.xxx"
    # portSSH = int
    # userNameDB = "string"
    # passDB = "string#"
    # dbName = "string"
    # query = '''INSERT INTO table_name (column1, column2)
    # VALUES ('value1', 'value1');'''

    with sshtunnel.SSHTunnelForwarder(
        (ipAddressSSH, portSSH),
        ssh_username= userNameSSH , ssh_password=passSSH,
        remote_bind_address=('127.0.0.1', 3306)
    ) as tunnel:
        try:
            connection = MySQLdb.connect(
                user=userNameDB,
                passwd=passDB,
                host='127.0.0.1', port=tunnel.local_bind_port,
                db= dbName
            )
            curosr = connection.cursor()
            curosr.execute(query)
            connection.commit()
        except BaseException as e:
            print('problem: ', e)
            return (False)

        connection.close()
        return (True)
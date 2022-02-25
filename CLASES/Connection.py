import mysql.connector
from mysql.connector import errorcode

class Connection():
    USER=None
    PASS=None
    HOST=None
    DATABASE=None

    conn=None

    def __init__(self, USER, PASS, HOST, DATABASE):

        self.USER=USER
        self.PASS=PASS
        self.HOST=HOST
        self.DATABASE=DATABASE

        try:
            cnx=cnx=mysql.connector.connect(user=self.USER
                                            ,password=self.PASS
                                            ,host=self.HOST
                                            ,database=self.DATABASE)
            cnx.autocommit=False
            print("Conectado a la base de datos")
            self.conn=cnx
        except mysql.connector.Error as err:
            if err.errno==errorcode.ER_ACCESS_DENIED_ERROR:
                print("ALgo esta mal con su nombre de usuario y password")
            elif err.errno==errorcode.ER_BAD_DB_ERROR:
                print("La base de datos no existe")
            else:
                print(err)
    def execQuery(self, Query_params,params):
        cursor=self.conn.cursor()
        cursor.execute(Query_params, params)

    def execQueryArray(self, Query_params,paramsArray):
        cursor=self.conn.cursor()
        cursor.executemany(Query_params, paramsArray)

    def commit(self):
        """Para confirmar los insert se tiene que terminar con un commit"""
        self.conn.commit()
        
    def execQuerySimple(self,query):
        cursor=self.conn.cursor()
        cursor.execute(query)
        self.conn.commit()
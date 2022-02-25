# -*- coding: utf-8 -*-
from datetime import datetime

class LogTrazabilidad():
    
    IDPROCESS = 0
    conexion = None
    ETL_MAESTRO = None
    conexion = None
    
    def __init__(self, conexion, ETL_MAESTRO):
        self.IDPROCESS = self.getIdProcess()
        self.conexion = conexion
        self.ETL_MAESTRO = ETL_MAESTRO
        print("Log Trazabilidad: " + self.IDPROCESS)
        
    def getIdProcess(self):
        return datetime.now().strftime("%Y%m%d%H%M%S")
        
    def iniStatusActivity( self, FICHERO, DESCRIPCION, FECHA, STATUS, CANT_REGISTROS = None ):
        """ Insertar un nuevo registro en la tabla de control """
        
        sql_insert = """insert into ctl_activity_process ( id_process, etl_master, descri_activity, fichero, status, start_date, cant_row ) VALUES """
        sql_insert += """ ( %s, %s, %s, %s, %s, %s, %s ) """
        params = ( self.IDPROCESS, self.ETL_MAESTRO, DESCRIPCION, FICHERO, STATUS, FECHA, CANT_REGISTROS )
        self.conexion.execQuery( sql_insert, params )
        self.conexion.commit()
    
    def updateStatusActivity( self, FICHERO, DESCRIPCION, FECHA, STATUS ):
        """ Modificar el estado y la fecha de finalización del registro insertado """
        
        sql_update = """ update ctl_activity_process set 
                            	status = %s
                                , end_date = %s
                        where id_process = %s
                        	and descri_activity = %s
                        	and fichero = %s
                        	and start_date = %s
                    """
        params = ( STATUS, datetime.now(), self.IDPROCESS, DESCRIPCION, FICHERO, FECHA )
        self.conexion.execQuery( sql_update, params )
        self.conexion.commit()
        
    def getIdprocess(self):
        return self.IDPROCESS
    
    
    
    
    
    
"""
logTrazabilidad = LogTrazabilidad( conexion = conexion
                                  , ETL_MAESTRO = "PARSEAR XML PRUEBAS")

fecha_process = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Esta fecha es un campo clave para la trazabilidad
logTrazabilidad.iniStatusActivity( FICHERO = "N/A"
                                  , DESCRIPCION = "PROCESO"
                                  , FECHA = fecha_process
                                  ,  STATUS = 0 )

fecha_process = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Esta fecha es un campo clave para la trazabilidad
logTrazabilidad.iniStatusActivity( FICHERO = "FRANCISCO RODRIGUEZ"
                                  , DESCRIPCION = "PARSEAR XML"
                                  , FECHA = fecha_process
                                  ,  STATUS = 0 )

fecha_process = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Esta fecha es un campo clave para la trazabilidad
logTrazabilidad.updateStatusActivity( FICHERO = "FRANCISCO RODRIGUEZ"
                                      , DESCRIPCION = "PARSEAR XML"
                                      , FECHA = fecha_process
                                      ,  STATUS = 1 )    


fecha_process = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Esta fecha es un campo clave para la trazabilidad
logTrazabilidad.updateStatusActivity( FICHERO = "N/A"
                                      , DESCRIPCION = "PROCESO"
                                      , FECHA = fecha_process
                                      ,  STATUS = 1 )      
"""

"""
CREATE TABLE ctl_activity_process (
  etl_master varchar(50),
  id_process bigint(20),
  fichero varchar(100),
  descri_activity varchar(100),
  status int(11),
  start_date datetime,
  end_date datetime,
  cant_row int(11),
  fecha_desde date,
  fecha_hasta date
);
        
"""









   
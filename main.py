from CLASES.ParserXML import ParserXML
from CLASES.LogTrazabilidad import LogTrazabilidad
from CLASES.Connection import Connection
from CLASES.gestionArchivos import GestionArchivos
from datetime import datetime
import json
"""
- Iniciar el proceso de parsear la lista de ficheros XML que se encuentra en la lista: ficheros_xml
- Se conecta a la BBDD y va insertando en la tabla de logs todo lo que va sucendiendoy calculando tiempos etc...
"""
#DEFINIR EL ENTORNO PARA QUE COJA LOS VALORES QUE SE ENCUENTRAN EN EL config.json
ENTORNO = "PRODUCCION"      #PRODUCCION | DESARROLLO | TOR | LOCAL


# Obtener las variables de acceso
with open('./config.json', 'r') as file:
    config = json.load(file)

conexion = Connection( USER = config[ENTORNO]['USER']
                        , PASS = config[ENTORNO]['PASS']
                        , HOST = config[ENTORNO]['HOST']
                        , DATABASE = config[ENTORNO]['DATABASE'] )


#Crear la trazabilidad para ir registrando en la tabla de control todo lo que va sucediendo
logTrazabilidad = LogTrazabilidad( conexion = conexion
                                  , ETL_MAESTRO = "PARSEAR XML PRUEBAS")

fecha_process = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Esta fecha es un campo clave para la trazabilidad
logTrazabilidad.iniStatusActivity( FICHERO = "N/A"
                                  , DESCRIPCION = "PROCESO"
                                  , FECHA = fecha_process
                                  ,  STATUS = 0 )

fecha_activity = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Esta fecha es un campo clave para la trazabilidad
gestionArchivos = GestionArchivos()


fecha_process = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Esta fecha es un campo clave para la trazabilidad
logTrazabilidad.iniStatusActivity( FICHERO = "N/A"
                                  , DESCRIPCION = "DOWNLOAD & UNRAR"
                                  , FECHA = fecha_process
                                  ,  STATUS = 0 )
gestionArchivos.downloadPedidosRarURL( URL_PEDIDOS_DOWNLOAD = config[ENTORNO]['URL_PEDIDOS_DOWNLOAD']
                                        , RUTA_DOWNLOAD = config[ENTORNO]['RUTA_DOWNLOAD'] )

gestionArchivos.unRarFileDownload( RUTA_DOWNLOAD = config[ENTORNO]['RUTA_DOWNLOAD']
                                    , RUTA_DESCARGA = config[ENTORNO]['RUTA_XML'] )
logTrazabilidad.updateStatusActivity( FICHERO = "N/A"
                                             , DESCRIPCION = "DOWNLOAD & UNRAR"
                                             ,  FECHA = fecha_process
                                             ,  STATUS = 1 )


#obtener una lista de todos los ficheros XML que hay dentro de la RUTA que se le envía por parámetro
ficheros_xml = gestionArchivos.getFilesXMLFromOrigin( RUTA = config[ENTORNO]['RUTA_XML'] )
parserXML = ParserXML()
for ficheroEntrada in ficheros_xml:
    
    RUTA_ENTRADA = ficheroEntrada["RUTA_ENTRADA"]
    FICHERO = ficheroEntrada["FICHERO"]
    
    fecha_activity = datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Esta fecha es un campo clave para la trazabilidad
    
    try:
        
        logTrazabilidad.iniStatusActivity( FICHERO = FICHERO
                                          , DESCRIPCION = "ParsearXML"
                                          , FECHA = fecha_activity
                                          ,  STATUS = 0 )
        print("Parseando el fichero: " + FICHERO)
                
        parserXML.parsearXML( FICHERO, RUTA_ENTRADA )
        print("Parser OK")
        parserXML.insertRowsToBBDD( conexion )
        
    except:
        
        logTrazabilidad.updateStatusActivity( FICHERO = FICHERO
                                             , DESCRIPCION = "ParsearXML"
                                             ,  FECHA = fecha_activity
                                             ,  STATUS = -1 )
        print("hubo un error Parseando el fichero: " +RUTA_ENTRADA+ FICHERO+" Proceso: "+logTrazabilidad.IDPROCESS)
        
    else:
        
        logTrazabilidad.updateStatusActivity( FICHERO = FICHERO
                                             , DESCRIPCION = "ParsearXML"
                                             ,  FECHA = fecha_activity
                                             ,  STATUS = 1 )
        
    
### for ficheroEntrada in ficheros_xml:  
print("Hasta Luego")    
logTrazabilidad.updateStatusActivity( FICHERO = "N/A"
                                     , DESCRIPCION = "PROCESO"
                                     ,  FECHA = fecha_process
                                     ,  STATUS = 1 )

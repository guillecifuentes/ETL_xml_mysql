#from xml.etree import ElementTree as ET
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

class ParserXML():
    """ Inicializar las listas que contendrán los registros de las tablas/CSV  """
    list_header = list()
    list_pedidos = list()
    list_pedidos_detalles = list()
    
    pd_list_header = None
    pd_list_pedidos = None
    pd_list_pedidos_detalles = None
    
    FICHERO = None
    RUTA_ENTRADA = None

    def __init__(self):
        print("Parsear creado")

    def parsearXML( self, FICHERO, RUTA_ENTRADA ):
        """ Cargar el fichero XML de entrada en la variable xroot """
        
        self.FICHERO = FICHERO
        self.RUTA_ENTRADA = RUTA_ENTRADA
        
        FICHERO_PARSER = RUTA_ENTRADA + FICHERO
        print("Iniciando Parcer XML: ________"+FICHERO_PARSER)
        xtree = ET.parse(FICHERO_PARSER)
        xroot = xtree.getroot()
        xroot.tag
        #print("Iniciando Parcer XML: " +xroot)
        
        
        """ Definición de las funciones  """
        def getValue(valor):
            """ Esta función es para recibir el valor de los objetos del XML 
            en caso de que no sea None """
            if (valor is not None):
                return valor.text
            else:
                None

        def funcion_list_header(self, xroot):
            """ Esta función extrae del XML los datos de la cabecera del fichero """
            print("ujuuu")
            self.list_header.clear()
            fecha_desde = xroot.find('header/fecha_desde').text
            fecha_hasta = xroot.find('header/fecha_hasta').text
            pagina = xroot.find('header/pagina').text
            DateInsert = str(datetime.now())

            # Crear un dicionario con los valores
            diccionario = {"fecha_desde": fecha_desde
                            , "fecha_hasta": fecha_hasta
                            , "pagina": pagina
                            , "DateInsert": DateInsert
                            }
            # Añadir este diccionario a la lista. Solo tiene un registro
            self.list_header.append(diccionario)
            

        def funcion_list_pedidos( self, xroot ):
            """ guarda en una lista todos los registros que están en la etiqueta 'pedido'"""
            print("Estoy iniciando en proceso lista pedidos")
            self.list_pedidos.clear()
            for node in xroot.findall('pedido'):
                pedido_id = node.attrib.get('id')
                cliente_id = node.find('cliente_id')
                cliente = node.find('cliente')
                fecha = node.find('fecha')
                descuento_euro = node.find('descuento_euro')
                DateInsert = str(datetime.now())

                self.list_pedidos.append({"pedido_id": pedido_id
                                              , "cliente_id" : getValue(cliente_id)
                                              , "cliente" : getValue(cliente)
                                              , "fecha" : getValue(fecha)
                                              , "descuento_euro" : getValue(descuento_euro)
                                              , "DateInsert": DateInsert
                                        })
                print(pedido_id+" Es el numero de pedido insertado")
            print("Finalizando proceso lista pedidos")
        def funcion_list_pedidos_detalles( self, xroot ):
            
            self.list_pedidos_detalles.clear()
            for node in xroot.findall('pedido'):
                pedido_id = node.attrib.get('id')
                cliente_id = node.find('cliente_id')
                cliente = node.find('cliente')
                fecha = node.find('fecha')
                descuento_euro = node.find('descuento_euro')
                DateInsert = str(datetime.now())
                
                for node_2 in node.findall('detalles/linea'):
                    producto_id = node_2.attrib.get('producto_id')
                    color = node_2.find('color')
                    precio_euro = node_2.find('precio_euro')
                    unidades = node_2.find('unidades')
    
                    self.list_pedidos_detalles.append({"pedido_id": pedido_id
                                                  , "cliente_id" : getValue(cliente_id)
                                                  , "cliente" : getValue(cliente)
                                                  , "fecha" : getValue(fecha)
                                                  , "descuento_euro" : getValue(descuento_euro)
                                                  , "DateInsert": DateInsert
                                                  , "producto_id" : producto_id
                                                  , "color" : getValue(color)
                                                  , "precio_euro" : getValue(precio_euro)
                                                  , "unidades" : getValue(unidades)
                                            })
        

        """ Parsear el fichero XML para obtener los valores de las listas """
        funcion_list_header(self, xroot )
        funcion_list_pedidos(self, xroot )
        funcion_list_pedidos_detalles(self, xroot )

        """ Crear los dataframes a partir de las listas de diccionarios, se trabaja mejor con Pandas """
        self.pd_list_header = pd.DataFrame( self.list_header )
        self.pd_list_pedidos = pd.DataFrame( self.list_pedidos )
        self.pd_list_pedidos_detalles = pd.DataFrame( self.list_pedidos_detalles )
        print(self.pd_list_pedidos)

    def insertRowsToBBDD(self, conexion):
        print("Iniciando proceso de insercion")
        self.conexion = conexion
        def insert_list_header(self):
            sql_insert = """ insert into header ( pagina, fecha_desde, fecha_hasta, DateInsert ) 
                            values ( %s, %s, %s, %s ) """
                            
            params_array = ( self.list_header[0]["pagina"]
                            , self.list_header[0]["fecha_desde"]
                            , self.list_header[0]["fecha_hasta"]
                            , self.list_header[0]["DateInsert"]
                            )
            
            print(params_array)
            
            self.conexion.execQuery( Query_params = sql_insert, params = params_array )
        
        def insert_list_pedidos(self):
            print("Insertando parametros" )
            sql_insert = """ insert into pedidos ( id, cliente_id, cliente, fecha, descuento_euro, DateInsert )
                            values ( %s, %s, %s, %s, %s, %s ) """
            print(self.list_pedidos)
            params_array = []
            for pedido in self.list_pedidos:
                params_array.append( (
                                        pedido["pedido_id"]
                                        , pedido["cliente_id"]
                                        , pedido["cliente"]
                                        , pedido["fecha"]
                                        , pedido["descuento_euro"]
                                        , pedido["DateInsert"]
                                        ) 
                                    )
            print("el arreglo guardado")   
            print(params_array) 
            self.conexion.execQueryArray( Query_params = sql_insert, paramsArray = params_array )
            print("La consulta de insert product paso")    
        def insert_list_pedidos_detalles(self):
            sql_insert = """ insert into pedidos_detalles ( id, cliente_id, cliente, fecha, descuento_euro, DateInsert
                                                    , producto_id, color, precio_euro, unidades )
                            values ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s ) """
            params_array = []
            for pedido in self.list_pedidos_detalles:
                params_array.append( (
                                        pedido["pedido_id"]
                                        , pedido["cliente_id"]
                                        , pedido["cliente"]
                                        , pedido["fecha"]
                                        , pedido["descuento_euro"]
                                        , pedido["DateInsert"]
                                        , pedido["producto_id"]
                                        , pedido["color"]
                                        , pedido["precio_euro"]
                                        , pedido["unidades"]
                                        )
                                    )
            self.conexion.execQueryArray( Query_params = sql_insert, paramsArray = params_array )
            
        insert_list_header(self)
        insert_list_pedidos(self)
        insert_list_pedidos_detalles(self)
        
        self.conexion.commit()
        

 
"""
create table header ( 
	pagina varchar(100)
	, fecha_desde date 
	, fecha_hasta date 
	, DateInsert datetime
);

create table pedidos ( 
	id integer
	, cliente_id integer 
	, cliente varchar(100)
	, fecha datetime 
	, descuento_euro float 
	, DateInsert datetime
);

create table pedidos_detalles ( 
	id integer
	, cliente_id integer 
	, cliente varchar(100)
	, fecha datetime 
	, descuento_euro float 
	, DateInsert datetime
	, producto_id integer 
	, color varchar(50)
	, precio_euro float 
	, unidades integer  
);
 
"""



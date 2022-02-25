# ETL_xml_mysql
Tomado de: https://datamanagement.es/2019/10/13/proceso-etl-con-python-desde-cero-y-paso-a-paso/

ETL desarrollado en Python que extrae la información de pedidos de varios archivos xml en un repositorio en la web.
Los pasos que realiza son : 
  1. Descarga un archivo comprimido en formato .rar desde la web a un repositorio local 
  2. Una vez este se encuentra en el repositorio local, lo toma y lo descomprime en otra carpeta local
  3. Lee las cabeceras y la data (Parsear los ficheros XML) para insertarla en la base de datos

Todo el desarrollo se hizo en visual studio code y la conexión al motor de bases de datos MySQL

![20220225_](https://user-images.githubusercontent.com/17502722/155815004-e1348b8f-aae4-4cfa-a7b2-7c102b662080.png)


El Script de las tablas en MySQL es:

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

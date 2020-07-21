from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.unificadas.unificadas_segmentos_beam as unificadas_segmentos_beam
import dataflow_pipeline.unificadas.unificadas_campanas_beam as unificadas_campanas_beam
import dataflow_pipeline.unificadas.unificadas_codigos_gestion_beam as unificadas_codigos_gestion_beam
import dataflow_pipeline.unificadas.unificadas_codigos_causal_beam as unificadas_codigos_causal_beam
import dataflow_pipeline.unificadas.unificadas_tipificaciones_beam as unificadas_tipificaciones_beam
import dataflow_pipeline.unificadas.unificadas_gestiones_beam as unificadas_gestiones_beam
import dataflow_pipeline.unificadas.unificadas_usuarios_beam as unificadas_usuarios_beam
import dataflow_pipeline.unificadas.unificadas_clientes_beam as unificadas_clientes_beam
import dataflow_pipeline.unificadas.unificadas_seg_campanas_beam as unificadas_seg_campanas_beam
import dataflow_pipeline.unificadas.unificadas_data_beam as unificadas_data_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import sys

#coding: utf-8 

unificadas_api = Blueprint('unificadas_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@unificadas_api.route("/segmento")
def segmento():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_segmentos"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Segmento,Nombre_Segmento,Fecha_Creacion,Usuario_Creacion,Estado FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Segmento']).encode('utf-8') + "|"
        text_row += str(row['Nombre_Segmento']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Usuario_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "Segmento/Unificadas_Segmento" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.segmentos` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = unificadas_segmentos_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("Segmento/Unificadas_Segmento" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "Segmento cargado" + "flowAnswer" 
   
   ############################################################



@unificadas_api.route("/campana")
def campana():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_Campanas"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Campana,Nombre_Campana,Codigo_Campana,Id_UEN,Fecha_Creacion,Estado FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Campana']).encode('utf-8') + "|" 
        text_row += str(row['Nombre_Campana']).encode('utf-8') + "|"
        text_row += str(row['Codigo_Campana']).encode('utf-8') + "|"
        text_row += str(row['Id_UEN']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        # text_row += str(row['Logo']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
        
    conn.close()
    
    filename = "campanas/Unificadas_campanas" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Campanas` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)
    
    flowAnswer = unificadas_campanas_beam.run()
  
    # time.sleep(180)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("campanas/Unificadas_campanas" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "Campana cargada" + "flowAnswer" 
   



####################################################

@unificadas_api.route("/codigo_ges")
def codigo_ges():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_Cod_Gestion"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Cod_Gestion,Nombre_Codigo,Descripcion,Fecha_Creacion,Usuario_gestion,Estado FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Cod_Gestion']).encode('utf-8') + "|" 
        text_row += str(row['Nombre_Codigo']).encode('utf-8') + "|"
        text_row += str(row['Descripcion']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Usuario_gestion']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
        
    conn.close()
    
    filename = "codigos_gestion/Unificadas_cod_ges" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Codigos_Gestion` WHERE 5=5"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)
    
    flowAnswer = unificadas_codigos_gestion_beam.run()
  
    # time.sleep(180)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("codigos_gestion/Unificadas_cod_ges" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "codigo_ges cargada" + "flowAnswer" 


    #####################################################################

@unificadas_api.route("/codigos")
def codigos():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_Cod_Causal"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Cod_Causal,Nombre_Causal,Descripcion,Fecha_Creacion,Usuario_Creacion,Estado FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Cod_Causal']).encode('utf-8') + "|" 
        text_row += str(row['Nombre_Causal']).encode('utf-8') + "|"
        text_row += str(row['Descripcion']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Usuario_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
        
    conn.close()
    
    filename = "codigos_causal/Unificadas_cod_causal" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Codigos_Causal` WHERE 1=1"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)
    
    flowAnswer = unificadas_codigos_causal_beam.run()
  
    # time.sleep(180)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("codigos_causal/Unificadas_cod_causal" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "codigos cargados" + "flowAnswer" 

    ####################################################


@unificadas_api.route("/tipificaciones")
def tipificaciones():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Rel_Tipificaciones"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Tipif,Id_Campana,Id_Cod_Gestion,Id_Cod_Causal,Id_Cod_SubCausal,Cod_Homologado,Cod_Homologado_Causal,AdicionalOne,AdicionalTwo,AdicionalTree,Fecha_Creacion,Plantilla,Plantilla1,Estado,Usuario_Gestor,HIT FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Tipif']).encode('utf-8') + "|" 
        text_row += str(row['Id_Campana']).encode('utf-8') + "|"
        text_row += str(row['Id_Cod_Gestion']).encode('utf-8') + "|"
        text_row += str(row['Id_Cod_Causal']).encode('utf-8') + "|"
        text_row += str(row['Id_Cod_SubCausal']).encode('utf-8') + "|"
        text_row += str(row['Cod_Homologado']).encode('utf-8') + "|"
        text_row += str(row['Cod_Homologado_Causal']).encode('utf-8') + "|"
        text_row += str(row['AdicionalOne']).encode('utf-8') + "|"
        text_row += str(row['AdicionalTwo']).encode('utf-8') + "|"
        text_row += str(row['AdicionalTree']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Plantilla']).encode('utf-8') + "|"
        text_row += str(row['Plantilla1']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        text_row += str(row['Usuario_Gestor']).encode('utf-8') + "|"
        text_row += str(row['HIT']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
        
    conn.close()
    
    filename = "tipificaciones/Unificadas_tipificaciones" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Tipificaciones` WHERE 2=2"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)
    
    flowAnswer = unificadas_tipificaciones_beam.run()
  
    # time.sleep(180)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("tipificaciones/Unificadas_tipificaciones" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "tipificaciones cargadas" + "flowAnswer" 

    ####################################################

@unificadas_api.route("/gestiones")
def gestiones():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_Gestion"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    # conn.execute_query('SELECT Id_Gestion,Documento,Num_Obligacion,Id_Data,Id_Cod_Gestion,Id_Cod_Causal,Id_Cod_SubCausal,Id_Bot,Vlr_Promesa,Fecha_Promesa,Usuario_Gestor,Fecha_Gestion FROM ' + TABLE_DB + ' where CAST(Fecha_Gestion AS date) = CAST(GETDATE() as DATE) ')

    conn.execute_query("SELECT Id_Gestion,Documento,Num_Obligacion,Id_Campana,Id_Segmento,Id_Cod_Gestion,Id_Cod_Causal,Id_Cod_SubCausal,Vlr_Promesa,Fecha_Promesa,Num_Cuotas,Telefono,Fecha_Gestion,Usuario_Gestor,Opt_1,Opt_2,Opt_3,Opt_4,Opt_5,Cuadrante,Modalidad_Pago FROM " + TABLE_DB + " WHERE CONVERT(DATE, Fecha_Gestion) = CONVERT(DATE,GETDATE())")
    
    # conn.execute_query("SELECT Id_Gestion,Documento,Num_Obligacion,Id_Campana,Id_Segmento,Id_Cod_Gestion,Id_Cod_Causal,Id_Cod_SubCausal,Vlr_Promesa,Fecha_Promesa,Num_Cuotas,Telefono,Fecha_Gestion,Usuario_Gestor,Opt_1,Opt_2,Opt_3,Opt_4,Opt_5,Cuadrante,Modalidad_Pago FROM " + TABLE_DB + " WHERE CONVERT(DATE, Fecha_Gestion) = CAST('2020-07-04' AS DATE)")

    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Gestion']).encode('utf-8') + "|"
        text_row += str(row['Documento']).encode('utf-8') + "|"
        text_row += str(row['Num_Obligacion']).encode('utf-8') + "|"
        text_row += str(row['Id_Campana']).encode('utf-8') + "|"
        text_row += str(row['Id_Segmento']).encode('utf-8') + "|"
        text_row += str(row['Id_Cod_Gestion']).encode('utf-8') + "|"
        text_row += str(row['Id_Cod_Causal']).encode('utf-8') + "|"
        text_row += str(row['Id_Cod_SubCausal']).encode('utf-8') + "|"
        # text_row += str(row['Observacion']).encode('utf-8') + "|"
        text_row += str(row['Vlr_Promesa']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Promesa']).encode('utf-8') + "|"
        text_row += str(row['Num_Cuotas']).encode('utf-8') + "|"
        text_row += str(row['Telefono']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Gestion']).encode('utf-8') + "|"
        text_row += str(row['Usuario_Gestor']).encode('utf-8') + "|"
        text_row += str(row['Opt_1']).encode('utf-8') + "|"
        text_row += str(row['Opt_2']).encode('utf-8') + "|"
        text_row += str(row['Opt_3']).encode('utf-8') + "|"
        text_row += str(row['Opt_4']).encode('utf-8') + "|"
        text_row += str(row['Opt_5']).encode('utf-8') + "|"
        text_row += str(row['Cuadrante']).encode('utf-8') + "|"
        text_row += str(row['Modalidad_Pago']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "gestiones/Unificadas_gestiones" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Gestiones` WHERE CAST(SUBSTR(Fecha_Gestion,0,10) AS DATE) = CURRENT_DATE()"
        # deleteQuery = "DELETE FROM `contento-bi.unificadas.Gestiones` WHERE CAST(SUBSTR(Fecha_Gestion,0,10) AS DATE) = '2020-07-04'"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = unificadas_gestiones_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("gestiones/Unificadas_gestiones" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "gestiones cargadas" + "flowAnswer" 

    ##################################################

@unificadas_api.route("/usuarios")
def usuarios():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_usuarios"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Usuario,Documento_Usuario,Nombre_usuario,Estado,Fecha_Cargue,Usuario_Creacion,Extension,Id_Perfil FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Usuario']).encode('utf-8') + "|"
        text_row += str(row['Documento_Usuario']).encode('utf-8') + "|"
        text_row += str(row['Nombre_usuario']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Cargue']).encode('utf-8') + "|"
        text_row += str(row['Usuario_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Extension']).encode('utf-8') + "|"
        text_row += str(row['Id_Perfil']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "usuarios/Unificadas_usuarios" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Usuarios` WHERE 3=3"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = unificadas_usuarios_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("usuarios/Unificadas_usuarios" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "usuarios cargados" + "flowAnswer" 


#########################################

@unificadas_api.route("/clientes")
def clientes():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_Clientes"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Cliente,Documento,Nombre_Cliente,Apellidos_Cliente,Fecha_Creacion FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Cliente']).encode('utf-8') + "|"
        text_row += str(row['Documento']).encode('utf-8') + "|"
        text_row += str(row['Nombre_Cliente']).encode('utf-8') + "|"
        text_row += str(row['Apellidos_Cliente']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Creacion']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
    conn.close()

    filename = "clientes/Unificadas_clientes" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Clientes` WHERE CAST(SUBSTR(Fecha_Creacion,0,10) AS DATE) = CURRENT_DATE()"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)

    flowAnswer = unificadas_clientes_beam.run()

    # time.sleep(60)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("clientes/Unificadas_clientes" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "clientes cargados " + "flowAnswer" 

    ################################################

@unificadas_api.route("/segmento_camp")
def segmento_camp():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Rel_Seg_Campanas"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Rel_Seg_Campana,Id_Campana,Id_Segmento,Id_Bot,Fecha_Creacion,Usuario_Creacion,Estado FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")

    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Rel_Seg_Campana']).encode('utf-8') + "|" 
        text_row += str(row['Id_Campana']).encode('utf-8') + "|"
        text_row += str(row['Id_Segmento']).encode('utf-8') + "|"
        text_row += str(row['Id_Bot']).encode('utf-8') + "|"
        text_row += str(row['Fecha_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Usuario_Creacion']).encode('utf-8') + "|"
        text_row += str(row['Estado']).encode('utf-8') + "|"
        # text_row += str(row['Logo']).encode('utf-8') + "|"
        text_row += "\n"
        cloud_storage_rows += text_row
        
    conn.close()
    
    filename = "seg_campanas/Unificadas_seg_campanas" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Seg_campanas` WHERE 4=4"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)
    
    flowAnswer = unificadas_seg_campanas_beam.run()
  
    # time.sleep(180)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("seg_campanas/Unificadas_seg_campanas" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "Seg_Campana cargada" + "flowAnswer" 

    #####################################################################

@unificadas_api.route("/data_uni")
def data_uni():
    reload(sys)
    sys.setdefaultencoding('utf8')
    SERVER="192.168.20.63\MV"
    USER="DP_USER"
    PASSWORD="DPUSER12062020*"
    DATABASE="Mirror_UN1002XZCVBN"
    TABLE_DB = "dbo.Tb_Data"
    HOY = datetime.datetime.today().strftime('%Y-%m-%d')

    #Nos conectamos a la BD y obtenemos los registros
    conn = _mssql.connect(server=SERVER, user=USER, password=PASSWORD, database=DATABASE)
    conn.execute_query('SELECT Id_Data,Id_Campana,Zona,Documento,Cod_Interno,Tipo_Comprador,Customer_Class,Cupo,Num_Obligacion,Vlr_Factura,Fecha_Factura,Fecha_Vencimiento,Vlr_Saldo_Cartera,Dias_vencimiento,Campana_Orig,Ult_Campana,Codigo,Abogado,Division,Pais,Fecha_Prox_Conferencia,Cod_Gestion,Fecha_Gestion,Fecha_Promesa_Pago,Actividad_Economica,Saldo_Capital,Num_Cuotas,Num_Cuotas_Pagadas,Num_Cuotas_Faltantes,Num_Cuotas_Mora,Cant_Veces_Mora,Fecha_Ult_Pago,Saldo_Total_Vencido,Cod_Consecionario,Concesionario,Cod_Gestion_Ant,Grabador,Estado,Fecha_Cargue,Usuario_Cargue,Interes_Mora,Vlr_Cuotas_Vencidas FROM ' + TABLE_DB )
    # conn.execute_query('SELECT Id_Gestion,Id_Causal,Fecha_Seguimiento,Id_Usuario,Valor_Obligacion,Id_Docdeu, Nota FROM ' + TABLE_DB + ' where CAST(Fecha_Seguimiento AS date) >= CAST(' + "'2019-02-01' as DATE) ")
 
    cloud_storage_rows = ""

    # Debido a que los registros en esta tabla pueden tener saltos de linea y punto y comas inmersos
    for row in conn:
        text_row =  ""
        text_row += str(row['Id_Data']).encode('utf-8') + "|"
        text_row += str(row['Id_Campana']).encode('utf-8') + "|" 
        text_row += str(row['Documento']).encode('utf-8') + "|" 
        text_row += str(row['Cod_Interno']).encode('utf-8') + "|" 
        text_row += str(row['Tipo_Comprador']).encode('utf-8') + "|" 
        text_row += str(row['Customer_Class']).encode('utf-8') + "|" 
        text_row += str(row['Cupo']).encode('utf-8') + "|" 
        text_row += str(row['Num_Obligacion']).encode('utf-8') + "|" 
        text_row += str(row['Vlr_Factura']).encode('utf-8') + "|" 
        text_row += str(row['Fecha_Factura']).encode('utf-8') + "|" 
        text_row += str(row['Fecha_Vencimiento']).encode('utf-8') + "|" 
        text_row += str(row['Vlr_Saldo_Cartera']).encode('utf-8') + "|" 
        text_row += str(row['Dias_vencimiento']).encode('utf-8') + "|" 
        text_row += str(row['Campana_Orig']).encode('utf-8') + "|" 
        text_row += str(row['Ult_Campana']).encode('utf-8') + "|" 
        text_row += str(row['Codigo']).encode('utf-8') + "|" 
        text_row += str(row['Abogado']).encode('utf-8') + "|" 
        text_row += str(row['Division']).encode('utf-8') + "|" 
        text_row += str(row['Pais']).encode('utf-8') + "|" 
        text_row += str(row['Fecha_Prox_Conferencia']).encode('utf-8') + "|" 
        text_row += str(row['Cod_Gestion']).encode('utf-8') + "|" 
        text_row += str(row['Fecha_Gestion']).encode('utf-8') + "|" 
        text_row += str(row['Fecha_Promesa_Pago']).encode('utf-8') + "|" 
        text_row += str(row['Actividad_Economica']).encode('utf-8') + "|" 
        text_row += str(row['Saldo_Capital']).encode('utf-8') + "|" 
        text_row += str(row['Num_Cuotas']).encode('utf-8') + "|" 
        text_row += str(row['Num_Cuotas_Pagadas']).encode('utf-8') + "|" 
        text_row += str(row['Num_Cuotas_Faltantes']).encode('utf-8') + "|" 
        text_row += str(row['Num_Cuotas_Mora']).encode('utf-8') + "|" 
        text_row += str(row['Cant_Veces_Mora']).encode('utf-8') + "|" 
        text_row += str(row['Fecha_Ult_Pago']).encode('utf-8') + "|" 
        text_row += str(row['Saldo_Total_Vencido']).encode('utf-8') + "|" 
        text_row += str(row['Cod_Consecionario']).encode('utf-8') + "|" 
        text_row += str(row['Concesionario']).encode('utf-8') + "|" 
        text_row += str(row['Cod_Gestion_Ant']).encode('utf-8') + "|" 
        text_row += str(row['Grabador']).encode('utf-8') + "|" 
        text_row += str(row['Estado']).encode('utf-8') + "|" 
        text_row += str(row['Fecha_Cargue']).encode('utf-8') + "|" 
        text_row += str(row['Usuario_Cargue']).encode('utf-8') + "|" 
        text_row += str(row['Interes_Mora']).encode('utf-8') + "|" 
        text_row += str(row['Vlr_Cuotas_Vencidas']).encode('utf-8') + "|" 
        text_row += "\n"
        cloud_storage_rows += text_row
        
    conn.close()
    
    filename = "data_unificadas/Unificadas_data" + ".csv"
    #Finalizada la carga en local creamos un Bucket con los datos
    gcscontroller.create_file(filename, cloud_storage_rows, "ct-unificadas")

    try:
        deleteQuery = "DELETE FROM `contento-bi.unificadas.Data` WHERE CAST(SUBSTR(Fecha_Creacion,0,10) AS DATE) = CURRENT_DATE()"
        client = bigquery.Client()
        query_job = client.query(deleteQuery)
        query_job.result()
    except:
        print("no se pudo eliminar")

    #Primero eliminamos todos los registros que contengan esa fecha
    
    # time.sleep(60)
    
    flowAnswer = unificadas_data_beam.run()
  
    # time.sleep(180)
    # Poner la ruta en storage cloud en una variable importada para posteriormente eliminarla 
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('ct-unificadas')
    blob = bucket.blob("data_unificadas/Unificadas_data" + ".csv")
    # Eliminar el archivo en la variable
    blob.delete()
    
    # return jsonify(flowAnswer), 200
    return "data cargada " + "flowAnswer" 
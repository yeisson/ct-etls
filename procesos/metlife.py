from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.metlife.Metlife_BM_beam as Metlife_BM_beam
import dataflow_pipeline.metlife.metlife_seguimiento_beam as metlife_seguimiento_beam
import os
import socket
import procesos.descargas as descargas

Metlife_BM_api = Blueprint('Metlife_BM_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@Metlife_BM_api.route("/Metlife_BM_Base")
def Metlife_BM_Base():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Metlife/Base/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[19:27]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-metlife')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.MetLife.Base` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = Metlife_BM_beam.run('gs://ct-metlife/Base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Metlife/Base/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Campos calculados agregados exitosamente"
                response["status"] = True
                
    return jsonify(response), response["code"]


Metlife_BM_descarga_api = Blueprint('Metlife_BM_descarga_api', __name__)
@Metlife_BM_descarga_api.route("/Metlife_BM_descarga_Base")

def Metlife_BM_descarga_Base():

    # Query de ejecucion de los campos calculados:
    # Defino la ruta de descarga.
    myRoute = '/BI_Archivos/GOOGLE/Metlife/Base/Descargas_Base_Calculada/Base_Calculada.csv'
    # Defino la consulta SQL a ejecutar en BigQuery.
    myQuery = 'SELECT * FROM `contento-bi.MetLife.Metlife_Base_Calculada`'
    # Defino los titulos de los campos resultantes de la ejecucion del query.
    myHeader = ["FECHA","REFERENCIA","TIPO_IDENTIFICACION","CEDULA_NIT","NOMBRE_DEL_CLIENTE","SEXO","FECHA_DE_NACIMIENTO","EDAD","CODIGO_PRODUCTO","CICLO","DIRECCION_CORRESPONDENCIA","CODIGO_CIUDAD_DE_CORRESPON","TEL_NRO_CORRESPONDENCIA","CELULAR","NOMBRE_EMPRESA","DIRNRO_EMPRESA","TELNRO_OFICINA","CODIGO_CIUDAD_OFICINA","CARGO_EMPRESA","FECHA_DE_INGRESO","CODIGO_ESTADO_CIVIL","CEDULA_CONYUGE","PERSONAS_A_CARGO","CODIGO_PROFESION","SALARIO_MENSUAL","TOTAL_EGRESOS","OCUPACION","CUPO","SALDO_DISPONIBLE","MERCADO","FECHA_DE_APROBACION_TCO","FECHA_DESBLOQUEO_TCO","MAIL","TIPO_EXTRACTO","SEGMENTO","CATEGORIA","ANO_MES_ENVIO","COD_GESTION","TIPIFICACION","TEL_MARCADO","RANGO_CUPO_VENTA","PLAN_MAX_AP","RANGO_EDAD","RANGO_CUPOS_DESC","RANGO_CUPOS_COP","LLAMADA_ESPECIAL_CUMPLEANOS","PYG_CLIENTE_DESC","PYG_CLIENTE_COP","GESTION_POR_MEDIOS_DIGITALES","No9_y_NoCELULAR","MEJOR_DIA","MEJOR_HORA"]
                
    return descargas.descargar_csv(myRoute, myQuery, myHeader) 

# Metlife_BM_descarga_api = Blueprint('Metlife_BM_descarga_api', __name__) No es necesaria porq ya esta declarada arriba
@Metlife_BM_descarga_api.route("/Metlife_BM_descarga_Base_Novedades")

def Metlife_BM_descarga_Base_Novedades():

    # Query de ejecucion de los campos calculados:
    # Defino la ruta de descarga.
    myRoute = '/BI_Archivos/GOOGLE/Metlife/Base/Descargas_Novedades/Base_Novedades.csv'
    # Defino la consulta SQL a ejecutar en BigQuery.
    myQuery = 'SELECT * FROM `contento-bi.MetLife.base_con_novedades`'
    # Defino los titulos de los campos resultantes de la ejecucion del query.
    myHeader = ["FECHA","REFERENCIA","TIPO_IDENTIFICACION","CEDULA_NIT","NOMBRE_DEL_CLIENTE","SEXO","FECHA_DE_NACIMIENTO","EDAD","CODIGO_PRODUCTO","CICLO","DIRECCION_CORRESPONDENCIA","CODIGO_CIUDAD_DE_CORRESPON","TEL_NRO_CORRESPONDENCIA","CELULAR","NOMBRE_EMPRESA","DIRNRO_EMPRESA","TELNRO_OFICINA","CODIGO_CIUDAD_OFICINA","CARGO_EMPRESA","FECHA_DE_INGRESO","CODIGO_ESTADO_CIVIL","CEDULA_CONYUGE","PERSONAS_A_CARGO","CODIGO_PROFESION","SALARIO_MENSUAL","TOTAL_EGRESOS","OCUPACION","CUPO","SALDO_DISPONIBLE","MERCADO","FECHA_DE_APROBACION_TCO","FECHA_DESBLOQUEO_TCO","MAIL","TIPO_EXTRACTO","SEGMENTO","CATEGORIA","ANO_MES_ENVIO","COD_GESTION","TIPIFICACION","TEL_MARCADO","RANGO_CUPO_VENTA","PLAN_MAX_AP","DIF_NOM_CLIENTE","DIF_SEXO","DIF_F_NACIMIENTO","VALOR_LARGO_CC","VALOR_INEXISTENTE_CC","VALOR_ESPECIAL_NOM_CLIENTE","VALOR_LARGO_TELEFONOS","VALOR_INEXISTENTE_CELULAR","VALOR_INEXISTENTE_TEL_NRO_CORRESPONDENCIA","VALOR_INVENTARIO_TELEFONOS","VALOR_EDAD","VALOR_SEXO","VALOR_E_CIVIL"]
                
    return descargas.descargar_csv(myRoute, myQuery, myHeader) 


##################################################################### archivos seguimiento ##########################################################################################################################################################

@Metlife_BM_api.route("/archivos_seguimiento")  
def archivos_seguimiento_metlife():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Metlife/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[19:27]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-metlife')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.MetLife.Seguimiento_Diario` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = metlife_seguimiento_beam.run('gs://ct-metlife/seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Metlife/Seguimiento/Procesados/" + archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje
    # return "Corriendo : " + mensaje

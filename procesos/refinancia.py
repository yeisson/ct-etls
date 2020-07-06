from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.refinancia.refinancia_seguimiento_beam as refinancia_seguimiento_beam
import dataflow_pipeline.refinancia.refinancia_prejuridico_beam as refinancia_prejuridico_beam
import dataflow_pipeline.refinancia.refinancia_BD_Calculada_Base_Inicial_beam as refinancia_BD_Calculada_Base_Inicial_beam
import dataflow_pipeline.refinancia.refinancia_BD_Calculada_Demograficos_beam as refinancia_BD_Calculada_Demograficos_beam
import dataflow_pipeline.refinancia.refinancia_BD_Calculada_Gestion_Diaria_beam as refinancia_BD_Calculada_Gestion_Diaria_beam
import dataflow_pipeline.refinancia.refinancia_BD_Calculada_Base_Pagos_beam as refinancia_BD_Calculada_Base_Pagos_beam
import procesos.descargas as descargas
import os
import socket
import time

refinancia_api = Blueprint('refinancia_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@refinancia_api.route("/archivos_seguimiento")
def archivos_Seguimiento():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-refinancia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.refinancia.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = refinancia_seguimiento_beam.run('gs://ct-refinancia/info-seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/Seguimiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

################################################################################################################
    
@refinancia_api.route("/archivos_Prejuridico")
def archivos_Prejuridico():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/Prejuridico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[23:31]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-refinancia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-prejuridico/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.refinancia.prejuridico` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = refinancia_prejuridico_beam.run('gs://ct-refinancia/info-prejuridico/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/Prejuridico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

'''
-----------------------------------------------------------------------------------------------------------------------
                                            PROYECTO "BASE CALCULADA" -> BASE INICIAL
-----------------------------------------------------------------------------------------------------------------------
'''

@refinancia_api.route("/BD_Calculada_Base_Inicial")
def BD_Calculada_Base_Inicial():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Base_Inicial/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[23:31]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-refinancia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('BD_Calculada_Base_Inicial/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.refinancia.BD_Calculada_Base_Inicial` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = refinancia_BD_Calculada_Base_Inicial_beam.run('gs://ct-refinancia/BD_Calculada_Base_Inicial/' + archivo, mifecha)
            if mensaje == "Base Calculada Actualizada":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Base_Inicial/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion con Exito"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

'''
-----------------------------------------------------------------------------------------------------------------------
                                  PROYECTO "BASE CALCULADA" -> BASE DEMOGRAFICOS
-----------------------------------------------------------------------------------------------------------------------
'''

@refinancia_api.route("/BD_Calculada_Demograficos")
def BD_Calculada_Demograficos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Demograficos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[15:23]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-refinancia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('BD_Calculada_Demograficos/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.refinancia.BD_Calculada_Demograficos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = refinancia_BD_Calculada_Demograficos_beam.run('gs://ct-refinancia/BD_Calculada_Demograficos/' + archivo, mifecha)
            if mensaje == "Base Calculada Actualizada":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Demograficos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion con Exito"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

'''
-----------------------------------------------------------------------------------------------------------------------
                                  PROYECTO "BASE CALCULADA" -> GESTION DIARIA
-----------------------------------------------------------------------------------------------------------------------
'''

@refinancia_api.route("/BD_Calculada_Gestion_Diaria")
def BD_Calculada_Gestion_Diaria():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Gestion_Diaria/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[24:32]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-refinancia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('BD_Calculada_Gestion_Diaria/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.refinancia.BD_Calculada_Gestion_Diaria` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = refinancia_BD_Calculada_Gestion_Diaria_beam.run('gs://ct-refinancia/BD_Calculada_Gestion_Diaria/' + archivo, mifecha)
            if mensaje == "Base Calculada Actualizada":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Gestion_Diaria/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion con Exito"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

'''
-----------------------------------------------------------------------------------------------------------------------
                                  PROYECTO "BASE CALCULADA" -> BASE PAGOS
-----------------------------------------------------------------------------------------------------------------------
'''

@refinancia_api.route("/BD_Calculada_Base_Pagos")
def BD_Calculada_Base_Pagos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Base_Pagos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[30:38]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-refinancia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('BD_Calculada_Base_Pagos/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.refinancia.BD_Calculada_Base_Pagos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = refinancia_BD_Calculada_Base_Pagos_beam.run('gs://ct-refinancia/BD_Calculada_Base_Pagos/' + archivo, mifecha)
            if mensaje == "Base Calculada Actualizada":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Base_Pagos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion con Exito"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

Refinancia_descarga_api = Blueprint('Refinancia_descarga_api', __name__)
@Refinancia_descarga_api.route("/Refinancia_descarga_Base")

def x():

    # Query de ejecucion de los campos calculados:
    # Defino la ruta de descarga.
    myRoute = '/BI_Archivos/GOOGLE/Refinancia/BD_Calculada/Descargas/Base_Calculada.csv'
    # Defino la consulta SQL a ejecutar en BigQuery.
    myQuery = 'SELECT * FROM `contento-bi.refinancia.BD_Calculada_QRY_Consolidado`'
    # Defino los titulos de los campos resultantes de la ejecucion del query.
    myHeader = ["FECHA","IDENTIFICACION","CLIENTE","ID_CLIENTE","PORTAFOLIO","CIUDADDEPTO","PERFIL","ESTCLIENTE","ESTCOMERCIAL","CAPITAL","MONTOTOTAL","NO_CRE","SCORE","DIAS_MORA","TOP","CUANTIA_RANGOS","E_ASIGNACION","MEJOR_COD_MES_ACTUAL","MEJOR_COD_MES_ANTERIOR","MEJOR_COD_ULT_TRIMESTRE","PAGOS","TEL1","TEL2","TEL3","TEL4","TEL5","TEL6","TEL7","TEL8","TEL9","TEL10","FECHA_GENERACION_ULT_PROMESA","FECHA_COMPROMISO_ULT_PROMESA","VALOR_COMPROMISO_ULT_PROMESA","VALOR_TOTAL_PAGADO_ULT_PROMESA","ESTADO_ULT_PROMESA"]
                
    return descargas.descargar_csv(myRoute, myQuery, myHeader) 
    
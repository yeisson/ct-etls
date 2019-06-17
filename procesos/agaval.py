#coding: utf-8
from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.agaval.agaval_asignacion_beam as agaval_asignacion_beam
import dataflow_pipeline.agaval.agaval_seguimiento_beam as agaval_seguimiento_beam
import dataflow_pipeline.agaval.agaval_recaudo_beam as agaval_recaudo_beam
#import dataflow_pipeline.bancolombia.bancolombia_seguimiento_beam as bancolombia_seguimiento_beam
#import dataflow_pipeline.bancolombia.bancolombia_bm_beam as bancolombia_bm_beam
import os
import socket
import time

agaval_api = Blueprint('agaval_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@agaval_api.route("/archivos_asignacion")
def archivos_Asignacion():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Agaval/Asignacion/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[18:26]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-agaval')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('asignacion/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.agaval.asignacion` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = agaval_asignacion_beam.run('gs://ct-agaval/asignacion/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Agaval/Asignacion/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]

@agaval_api.route("/archivos_seguimiento")
def archivos_Seguimiento():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Agaval/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[11:19]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-agaval')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.agaval.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = agaval_seguimiento_beam.run('gs://ct-agaval/info-seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Agaval/Seguimiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                # ----------------------------------------------------------------------------------------------------------------
                # Elimina datos de la tabla de Seguimiento Consolidada:
                deleteQuery_2 = "DELETE FROM `contento-bi.Contento.seguimiento_consolidado` WHERE ID_OPERACION = '32' AND FECHA = '" + mifecha + "'"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                time.sleep(600)

                # Inserta la información agrupada según funciones de agregación en la tabla de Seguimiento Consolidada:
                inserteDatos = "INSERT INTO `contento-bi.Contento.seguimiento_consolidado` (ID_OPERACION,FECHA,ANO,MES,NOMBRE_MES,DIA,HORA,GRABADOR,NEGOCIADOR,ID_LIDER,LIDER,EJECUTIVO,GERENTE,TIPO_CONTACTO,RANGO_MORA,TIENDA,MACRO_PRODUCTO,PRODUCTO,META_GESTIONES,TRABAJO,GESTIONES,WPC,RPC,HIT) (SELECT * FROM `contento-bi.agaval.QRY_CONSL_HORA_HORA` WHERE FECHA = '" + mifecha + "')"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(inserteDatos)
                query_job_3.result()
                # ----------------------------------------------------------------------------------------------------------------                

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

@agaval_api.route("/archivos_recaudo")
def archivos_Recaudo():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Agaval/Recaudo/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[15:23]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-agaval')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('recaudo/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.agaval.recaudo` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = agaval_recaudo_beam.run('gs://ct-agaval/recaudo/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Agaval/Recaudo/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje
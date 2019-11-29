from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.ucc.ucc_mensajes_beam as ucc_mensajes_beam
import dataflow_pipeline.ucc.ucc_agentev_beam as ucc_agentev_beam
import dataflow_pipeline.ucc.ucc_campana_beam as ucc_campana_beam
import os
import socket 
import time

universidad_cooperativa_col_api = Blueprint('universidad_cooperativa_col_api', __name__)
# ucc_api = Blueprint('ucc_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@universidad_cooperativa_col_api.route("/archivos_sms")
def archivos_sms():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Mensajes/Resultado/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-sms/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.ucc.sms` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = ucc_mensajes_beam.run('gs://ct-ucc/info-sms/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Mensajes/Resultado/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

################################################################################################################################################

@universidad_cooperativa_col_api.route("/archivos_agentev")
def archivos_agentev():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/agente_virtual/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[20:28]
            lote =  archivo[15:19]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-agente_virtual/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.ucc.agente_virtual` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = ucc_agentev_beam.run('gs://ct-ucc/info-agente_virtual/' + archivo, mifecha,lote)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/agente_virtual/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

############################################################################################################################################

@universidad_cooperativa_col_api.route("/archivos_campanas")
def archivos_campanas():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Campanas/Resultado/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[13:21]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-campanas/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.ucc.base_campanas` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = ucc_campana_beam.run('gs://ct-ucc/info-campanas/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Campanas/Resultado/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje
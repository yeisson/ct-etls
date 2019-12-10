from flask import Blueprint

from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.fanalca.fanalca_agendamiento_asignacion_beam as fanalca_agendamiento_asignacion_beam
import dataflow_pipeline.fanalca.fanalca_agendamiento_agendados_beam as fanalca_agendamiento_agendados_beam

import os
import socket

fanalca_agendamientos_api = Blueprint('fanalca_agendamientos_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@fanalca_agendamientos_api.route("/archivos_asignacion")
def archivos_asignacion():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Agendamientos/Asignacion/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-fanalca')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agendamiento-asignacion/' + archivo)
            blob.upload_from_filename(local_route + archivo)
            try:
                deleteQuery = "DELETE FROM `contento-bi.fanalca_agendamiento.asignacion` WHERE fecha = '" + mifecha + "'"
                client = bigquery.Client()
                query_job = client.query(deleteQuery)
                query_job.result() 
            except: 
                print("Eliminado de bigquery")
            

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = fanalca_agendamiento_asignacion_beam.run('gs://ct-fanalca/agendamiento-asignacion/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Agendamientos/Asignacion/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]


#########################################################################################################################################
#Espiritu santo de Dios, que sean tus manos creando este codigo. En el nombre del padre todo poderoso AMEN.
#########################################################################################################################################

@fanalca_agendamientos_api.route("/archivos_agendamientos")
def archivos_agendamientos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Agendamientos/Agendados/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-fanalca')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agendamiento-agendados/' + archivo)
            blob.upload_from_filename(local_route + archivo)
            try:
                deleteQuery = "DELETE FROM `contento-bi.fanalca_agendamiento.agendados` WHERE fecha = '" + mifecha + "'"
                client = bigquery.Client()
                query_job = client.query(deleteQuery)
                query_job.result() 
            except: 
                print("Eliminado de bigquery")
            

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = fanalca_agendamiento_agendados_beam.run('gs://ct-fanalca/agendamiento-agendados/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Agendamientos/Agendados/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
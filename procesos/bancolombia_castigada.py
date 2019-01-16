from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.bancolombia.bancolombia_castigada_seguimiento_beam as bancolombia_castigada_seguimiento_beam
import os
import socket

bancolombia_castigada_api = Blueprint('bancolombia_castigada_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@bancolombia_castigada_api.route("/archivos_seguimiento_castigada")
def archivos_Seguimiento_castigada():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[29:37]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia_castigada')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_castigada.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_castigada_seguimiento_beam.run('gs://ct-bancolombia_castigada/info-seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Cast/Seguimiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje
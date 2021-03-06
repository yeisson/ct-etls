# encoding=utf8
from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.cafam.cafam_campanas_beam as cafam_campanas_beam
import os
import socket

cafam_api = Blueprint('cafam_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@cafam_api.route("/archivos_campanas")
def archivos_campanas():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Cafam/Campanas/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]
            id_campana = archivo[17:23]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-telefonia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('cafam_campanas/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.telefonia.claro_campanas` WHERE fecha = '" + mifecha + "'"

            # #Primero eliminamos todos los registros que contengan esa fecha
            # client = bigquery.Client()
            # query_job = client.query(deleteQuery)

            #result = query_job.result()
            # query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = cafam_campanas_beam.run('gs://ct-telefonia/cafam_campanas/' + archivo, mifecha, id_campana)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Cafam/Campanas/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

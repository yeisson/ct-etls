from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.crediorbe_sac.base_beam as base_beam
import os
import socket
import time

crediorbe_sac_api = Blueprint('crediorbe_sac_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@crediorbe_sac_api.route("/base")
def base():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron archivos para subir"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Crediorbe_sac/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[5:12]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-crediorbe_sac')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Crediorbe_sac.Base` WHERE ID = '0'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = base_beam.run('gs://ct-crediorbe_sac/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Crediorbe_sac/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]
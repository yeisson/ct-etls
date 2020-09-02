from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.Jerarquias.Jerarquias_beam as Jerarquias_beam
import os
import socket
import time

Jerarquias_api = Blueprint('Jerarquias_api', __name__)

# fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@Jerarquias_api.route("/Jerarquias")
def Jerarquias():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    homepath = os.environ["HOMEPATH"]
    dir_ = "\Documents\gmv/"   
    local_route = homepath + dir_

    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-prueba')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Jerarquias/' + archivo)
            blob.upload_from_filename(local_route + archivo)


            # # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.proteccion.Gestiones_adm` WHERE fecha = '" + mifecha + "'"

            # #Primero eliminamos todos los registros que contengan esa fecha
            # client = bigquery.Client()
            # query_job = client.query(deleteQuery)

            # result = query_job.result()
            # query_job.result() # Corremos el job de eliminacion de datos de BigQuery


            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = Jerarquias_beam.run('gs://ct-prueba/Jerarquias/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                # move(local_route + archivo,+dir_ "/Procesados/" + archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]

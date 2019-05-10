from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.cesde.hora_hora_beam as cesde_beam
import os
import socket
import shutil
import time

# coding=utf-8

cesde_api = Blueprint('cesde_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@cesde_api.route("/hora_hora")
def hora_hora():

    

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Cesde/Procesado/"

    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            Fecha = str(archivo[21:])
            Fecha = Fecha.replace(".csv","")
            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-cesde')
            nombre = 'gs://ct-cesde/' + archivo

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob(archivo)
            blob.upload_from_filename(local_route + archivo)

            try:
                deleteQuery = "DELETE FROM `contento-bi.cesde.hora_hora` WHERE SUBSTR(Fecha_de_inicio,0,10) = '" + Fecha + "'"
                client = bigquery.Client()
                query_job = client.query(deleteQuery)
                query_job.result()
            except:
                print("no se encontraron datos para eliminar.")

            mensaje = cesde_beam.run(nombre, Fecha)
            
            time.sleep(30)
            # shutil.rmtree(local_route + archivo)
            blob.delete()


    return jsonify(response), response["code"]






    
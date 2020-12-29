from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.neurologico.b2chat_beam as b2chat_beam
import dataflow_pipeline.neurologico.asignacion_de_citas_beam as asignacion_de_citas_beam
import os
import socket
import time

neurologico_api = Blueprint('neurologico_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@neurologico_api.route("/b2chat")
def b2chat():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron archivos"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Neurologico/B2chat"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[6:14]
            

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-neurologico')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('b2chat/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V2` WHERE fecha = '" + mifecha + "'"
            deleteQuery = "DELETE FROM `contento-bi.Neurologico.B2chat` WHERE fecha = '" + mifecha + "'"


            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = b2chat_beam.run('gs://ct-neurologico/b2chat/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Neurologico/B2chat/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]


############################ ASIGNACION DE CITAS ##############################


@neurologico_api.route("/asignacion")
def asignacion():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron archivos"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Neurologico/Asignacion de citas/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]
            

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-neurologico')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('asignacion_citas/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V2` WHERE fecha = '" + mifecha + "'"
            deleteQuery = "DELETE FROM `contento-bi.Neurologico.Asignacion_citas` WHERE fecha = '" + mifecha + "'"


            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = asignacion_de_citas_beam.run('gs://ct-neurologico/asignacion_citas/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Neurologico/Asignacion de citas/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]
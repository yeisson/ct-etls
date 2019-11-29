from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.fanalca.honda_digital_asignacion_beam as honda_digital_asignacion_beam
import dataflow_pipeline.fanalca.honda_digital_gestion_cotizados_beam as honda_digital_gestion_cotizados_beam
import dataflow_pipeline.fanalca.honda_digital_gestion_ipdial_beam as honda_digital_gestion_ipdial_beam
import os
import socket

fanalca_api = Blueprint('fanalca_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################

@fanalca_api.route("/digital/asignacion")
def archivos_asignacion():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Honda Digital/Asignacion/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[12:20]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-fanalca')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('digital-asignacion/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            try:
                # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
                deleteQuery = "DELETE FROM `contento-bi.fanalca.asignacion_digital` WHERE fecha_cargue = '" + mifecha + "'"
            
                #Primero eliminamos todos los registros que contengan esa fecha
                client = bigquery.Client()
                query_job = client.query(deleteQuery)

                #result = query_job.result()
                query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            except:
                "No se pudo eliminar "


            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = honda_digital_asignacion_beam.run('gs://ct-fanalca/digital-asignacion/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Honda Digital/Asignacion/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    

#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################


@fanalca_api.route("/digital/gestion_cotizaciones")
def gestion_cotizaciones():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Honda Digital/Gestion_COTIZADOS/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[20:28]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-fanalca')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('digital-gestion/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            try:
                # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
                deleteQuery = "DELETE FROM `contento-bi.fanalca.gestion_cotizados_digital` where 1 = 1"
            
                #Primero eliminamos todos los registros que contengan esa fecha
                client = bigquery.Client()
                query_job = client.query(deleteQuery)

                #result = query_job.result()
                query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            except:
                "No se pudo eliminar "


            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = honda_digital_gestion_cotizados_beam.run('gs://ct-fanalca/digital-gestion/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Honda Digital/Gestion_COTIZADOS/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    

#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################

@fanalca_api.route("/digital/gestion_ipdial")
def gestion_ipdial():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Honda Digital/Gestion_IPDIAL/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[17:25]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-fanalca')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('digital-gestion/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            try:
                # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
                deleteQuery = "DELETE FROM `contento-bi.fanalca.gestion_ipdial_digital` WHERE fecha_cargue = '" + mifecha + "'"
            
                #Primero eliminamos todos los registros que contengan esa fecha
                client = bigquery.Client()
                query_job = client.query(deleteQuery)

                #result = query_job.result()
                query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            except:
                "No se pudo eliminar "


            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = honda_digital_gestion_ipdial_beam.run('gs://ct-fanalca/digital-gestion/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Fanalca/Honda Digital/Gestion_IPDIAL/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"] 
    

#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
#########################################################################################################################
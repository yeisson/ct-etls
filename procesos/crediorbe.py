from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.crediorbe.crediorbe_asignacion_beam as crediorbe_asignacion_beam
import dataflow_pipeline.crediorbe.crediorbe_gestiones_beam as crediorbe_gestiones_beam
import dataflow_pipeline.crediorbe.crediorbe_recaudo_beam as crediorbe_recaudo_beam
#import dataflow_pipeline.bancolombia.bancolombia_seguimiento_beam as bancolombia_seguimiento_beam
#import dataflow_pipeline.bancolombia.bancolombia_bm_beam as bancolombia_bm_beam
import os
import socket
import time

crediorbe_api = Blueprint('crediorbe_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@crediorbe_api.route("/archivos_asignacion")
def archivos_Asignacion():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Crediorbe/Asignacion/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[21:29]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-crediorbe')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('asignacion/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.crediorbe.asignacion` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = crediorbe_asignacion_beam.run('gs://ct-crediorbe/asignacion/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Crediorbe/Asignacion/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]

@crediorbe_api.route("/archivos_gestiones")
def archivos_Gestiones():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Crediorbe/Gestiones/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[14:22]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-crediorbe')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('gestiones/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.crediorbe.gestiones` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = crediorbe_gestiones_beam.run('gs://ct-crediorbe/gestiones/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Crediorbe/Gestiones/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                # ----------------------------------------------------------------------------------------------------------------
                # Elimina datos de la tabla de Seguimiento Consolidada:
                deleteQuery_2 = "DELETE FROM `contento-bi.Contento.seguimiento_consolidado` WHERE ID_OPERACION = '20' AND FECHA = '" + mifecha + "'"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                time.sleep(10)

                # Inserta la informacion agrupada segun funciones de agregacion en la tabla de Seguimiento Consolidada:
                inserteDatos = "INSERT INTO `contento-bi.Contento.seguimiento_consolidado` (ID_OPERACION,FECHA,ANO,MES,NOMBRE_MES,DIA,HORA,GRABADOR,NEGOCIADOR,ID_LIDER,LIDER,EJECUTIVO,GERENTE,TIPO_CONTACTO,RANGO_MORA,TIENDA,MACRO_PRODUCTO,PRODUCTO,META_GESTIONES,TRABAJO,GESTIONES,WPC,RPC,HIT) (SELECT * FROM `contento-bi.crediorbe.QRY_CONSL_HORA_HORA` WHERE FECHA = '"+ mifecha +"')"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(inserteDatos)
                query_job_3.result()
                # ----------------------------------------------------------------------------------------------------------------

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

@crediorbe_api.route("/archivos_recaudo")
def archivos_Recaudo():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Crediorbe/Recaudo/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[18:26]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-crediorbe')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('recaudo/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.crediorbe.recaudo` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = crediorbe_recaudo_beam.run('gs://ct-crediorbe/recaudo/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Crediorbe/Recaudo/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje
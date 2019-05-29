#coding: utf-8
from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.avalcreditos.avalcreditos_seguimiento_beam as avalcreditos_seguimiento_beam
import dataflow_pipeline.avalcreditos.avalcreditos_prejuridico_beam as avalcreditos_prejuridico_beam
import dataflow_pipeline.avalcreditos.avalcreditos_pagos_beam as avalcreditos_pagos_beam
import os
import socket

avalcreditos_api = Blueprint('avalcreditos_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]


@avalcreditos_api.route("/archivos_seguimiento")
def archivos_Seguimiento():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Avalcreditos/Seguimiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[17:25]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-avalcreditos')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-seguimiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.avalcreditos.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)                        

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = avalcreditos_seguimiento_beam.run('gs://ct-avalcreditos/info-seguimiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Avalcreditos/Seguimiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

                # ----------------------------------------------------------------------------------------------------------------
                # Elimina datos de la tabla de Seguimiento Consolidada:
                deleteQuery_2 = "DELETE FROM `contento-bi.Contento.seguimiento_consolidado` WHERE ID_OPERACION = '14' AND FECHA = '" + mifecha + "'"
                client_2 = bigquery.Client()
                query_job_2 = client_2.query(deleteQuery_2)
                query_job_2.result()

                # Inserta la información agrupada según funciones de agregación en la tabla de Seguimiento Consolidada:
                inserteDatos = "INSERT INTO `contento-bi.Contento.seguimiento_consolidado` (ID_OPERACION,FECHA,ANO,MES,NOMBRE_MES,DIA,HORA,GRABADOR,NEGOCIADOR,ID_LIDER,LIDER,EJECUTIVO,GERENTE,TIPO_CONTACTO,RANGO_MORA,TIENDA,MACRO_PRODUCTO,PRODUCTO,META_GESTIONES,TRABAJO,GESTIONES,WPC,RPC,HIT) (SELECT * FROM `contento-bi.avalcreditos.QRY_CONSL_HORA_HORA` WHERE FECHA = '"+ mifecha +"')"
                client_3 = bigquery.Client()
                query_job_3 = client_3.query(inserteDatos)
                query_job_3.result()
                # ----------------------------------------------------------------------------------------------------------------

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


@avalcreditos_api.route("/archivos_prejuridico")
def archivos_Prejuridico():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Avalcreditos/Prejuridico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[17:25]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-avalcreditos')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-prejuridico/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.avalcreditos.prejuridico` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = avalcreditos_prejuridico_beam.run('gs://ct-avalcreditos/info-prejuridico/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Avalcreditos/Prejuridico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


@avalcreditos_api.route("/archivos_pagos")
def archivos_Pagos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Avalcreditos/Pagos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[17:25]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-avalcreditos')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-pago/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.avalcreditos.pagos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = avalcreditos_pagos_beam.run('gs://ct-avalcreditos/info-pago/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Avalcreditos/Pagos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje
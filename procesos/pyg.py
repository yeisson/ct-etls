from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.pyg.pyg_ofima_beam as pyg_ofima_beam
import dataflow_pipeline.pyg.pyg_PUC_beam as pyg_PUC_beam
import dataflow_pipeline.pyg.pyg_CeCo_beam as pyg_CeCo_beam
import dataflow_pipeline.pyg.pyg_ajustes_beam as pyg_ajustes_beam
import dataflow_pipeline.pyg.pyg_puestos_beam as pyg_puestos_beam
import dataflow_pipeline.pyg.pyg_agentes_beam as pyg_agentes_beam
import os
import socket

pyg_api = Blueprint('pyg_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@pyg_api.route("/pyg_ofima")
def pyg_ofima():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/Ofima/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[6:10]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-pyg')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('ofima/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.PyG.ofima` WHERE fecha = '" + mifecha + "' AND FUENTE = 'OFIMA'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = pyg_ofima_beam.run('gs://ct-pyg/ofima/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/Ofima/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True
                
    return jsonify(response), response["code"]

##################################################################################################################################
#                                                      ____     _    _     ___                                                   #
#                                                     /  _ \   | |  | |   / __ \                                                 #
#                                                     | |_| |  | |  | |  / /  \_|                                                #
#                                                     | ___/   | |  | |  | |   __                                                #
#                                                     | |      \ \__/ /  | \__/ /                                                #
#                                                     |_|       \____/    \____/                                                 #
#                                                                                                                                #
##################################################################################################################################

@pyg_api.route("/pyg_PUC")
def pyg_PUC():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/PUC/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[4:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-pyg')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('PUC/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.PyG.PUC` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = pyg_PUC_beam.run('gs://ct-pyg/PUC/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/PUC/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True
                
    return jsonify(response), response["code"]

##################################################################################################################################
#                                      ___       ____       ___         ___                                                      #
#                                    / __ \     |  __|     / __ \      / __ \                                                    #
#                                   / /  \_|    | |__     / /  \_|    / /  \ \                                                   #
#                                   | |   __    |  __|    | |   __    | |  | |                                                   #
#                                   | \__/ /    | |__     | \__/ /    | \__/ /                                                   #
#                                    \____/     |____|     \____/      \____/                                                    #
#                                                                                                                                #
##################################################################################################################################

@pyg_api.route("/pyg_CeCo")
def pyg_CeCo():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/CeCo/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[5:9]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-pyg')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('CeCo/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.PyG.CeCo` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = pyg_CeCo_beam.run('gs://ct-pyg/CeCo/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/CeCo/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True
                
    return jsonify(response), response["code"]


##################################################################################################################################

@pyg_api.route("/ajustes")
def pyg_ajustes():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/Ajustes/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:12]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-pyg')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('ajustes/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.PyG.ofima` WHERE fecha = '" + mifecha + "' AND FUENTE = 'AJUSTES'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = pyg_ajustes_beam.run('gs://ct-pyg/ajustes/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/Ajustes/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True
                
    return jsonify(response), response["code"]

##################################################################################################################################

@pyg_api.route("/puestos")
def pyg_puestos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/Puestos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:12]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-pyg')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('puestos/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.PyG.puestos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = pyg_puestos_beam.run('gs://ct-pyg/puestos/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/Puestos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True
                
    return jsonify(response), response["code"]

##################################################################################################################################

@pyg_api.route("/agentes")
def pyg_agentes():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/Agentes/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:12]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-pyg')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agentes/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.PyG.agentes` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = pyg_agentes_beam.run('gs://ct-pyg/agentes/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/PYG/Agentes/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True
                
    return jsonify(response), response["code"]


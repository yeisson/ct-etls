from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.mobility.mobility_base_beam as mobility_base_beam
import dataflow_pipeline.mobility.mobility_tipificacion_beam as mobility_tipificacion_beam
import dataflow_pipeline.mobility.mobility_agente_beam as mobility_agente_beam
import dataflow_pipeline.mobility.mobility_ivr_beam as mobility_ivr_beam
import dataflow_pipeline.mobility.mobility_inte_beam as mobility_inte_beam
import dataflow_pipeline.mobility.mobility_intervalo_beam as mobility_intervalo_beam
import dataflow_pipeline.mobility.mobility_abandono_beam as mobility_abandono_beam
import dataflow_pipeline.mobility.mobility_correop_beam as mobility_correop_beam
import dataflow_pipeline.mobility.mobility_parametros_beam as mobility_parametros_beam
import dataflow_pipeline.mobility.mobility_autotec_beam as mobility_autotec_beam
import dataflow_pipeline.mobility.mobility_chat_beam as mobility_chat_beam
import dataflow_pipeline.mobility.mobility_AHT_beam as mobility_AHT_beam
import dataflow_pipeline.mobility.mobility_messenger_beam as mobility_messenger_beam
import dataflow_pipeline.mobility.perfil_demografico_beam as perfil_demografico_beam



import os
import socket

mobility_api = Blueprint('mobility_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]
 
######################################################################################################

@mobility_api.route("/base")
def base():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Base/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[4:12]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.base` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_base_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Base/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

########################################################################################################################################################3

@mobility_api.route("/tipificacion")
def tipificacion():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Tipificacion/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[9:17]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.tipificacion` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_tipificacion_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Tipificacion/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

########################################################################################################################################################3
@mobility_api.route("/agente")
def agente():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Data_agente/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[7:15]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.agente` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_agente_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Data_agente/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

########################################################################################################################################################3

@mobility_api.route("/ivr")
def ivr():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Data_IVR/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[4:12]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.ivr` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_ivr_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Data_IVR/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

########################################################################################################################################################3

@mobility_api.route("/inte")
def inte():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Data_int/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[11:19]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.inte` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_inte_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Data_int/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


########################################################################################################################################################3

@mobility_api.route("/intervalo")
def intervalo():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Intervalo/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[9:17]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.intervalo` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_intervalo_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Intervalo/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


    ########################################################################################################################################################3

@mobility_api.route("/abandono")
def abandono():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Abandono/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:16]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.abandono` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_abandono_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Abandono/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

      ########################################################################################################################################################3

@mobility_api.route("/correop")
def correop():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Correo/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:14]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.correop` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_correop_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Correo/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

         ########################################################################################################################################################3

@mobility_api.route("/parametros")
def parametros():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Parametros/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[9:17]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.parametros` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_parametros_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Parametros/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

    ########################################################################################################



@mobility_api.route("/autotec")
def autotec():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Autotec/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[8:15]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.autotec` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_autotec_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Autotec/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


    ########################################################################################################



@mobility_api.route("/chat")
def chat():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Chat/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[4:12]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.chat` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_chat_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Chat/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje



    ########################################################################################################



@mobility_api.route("/AHT")
def AHT():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/AHT/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[3:11]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.AHT` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_AHT_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/AHT/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje


########################################################################################################



@mobility_api.route("/messenger")
def messenger():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Messenger/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[9:17]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-dispersion')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('base/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.messenger` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = mobility_messenger_beam.run('gs://ct-dispersion/base/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Messenger/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

########################## PERFIL DEMOGRAFICO ############################

@mobility_api.route("/perfil")
def perfil():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Perfil_demografico/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-auteco')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('perfil_demografico/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.Auteco_Mobility.Base_perfil_cliente` WHERE BASE = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = perfil_demografico_beam.run('gs://ct-auteco/perfil_demografico/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Mobility/Perfil_demografico/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

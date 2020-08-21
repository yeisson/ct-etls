from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.ucc2.sms_beam as sms_beam
import dataflow_pipeline.ucc2.sms_opc1_beam as sms_opc1_beam
import dataflow_pipeline.ucc2.Apertura_beam as Apertura_beam
import dataflow_pipeline.ucc2.Rebote_beam as Rebote_beam
import dataflow_pipeline.ucc2.Base_correo_beam as Base_correo_beam
import dataflow_pipeline.ucc2.Doble_via_beam as Doble_via_beam
import os
import socket
import time

ucc_api = Blueprint('ucc_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@ucc_api.route("/mensajes")
def mensajes():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Mensajes/Resultado/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]
            

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-sms/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V2` WHERE fecha = '" + mifecha + "'"
            deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V3` WHERE campana = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sms_beam.run('gs://ct-ucc/info-sms/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Mensajes/Resultado/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]




    ############################## ESTRUCTURA SMS NUEVA ####################################



@ucc_api.route("/sms1")
def sms1():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Mensajes/sms_opc1/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]
            

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-sms/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V2` WHERE fecha = '" + mifecha + "'"
            deleteQuery = "DELETE FROM `contento-bi.ucc.sms_opc1` WHERE campana = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sms_opc1_beam.run('gs://ct-ucc/info-sms/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Mensajes/sms_opc1/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]



########################## ESTRUCTURA NUEVA CORREO APERTURA ###################################



@ucc_api.route("/apertura")
def apertura():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Email/Apertura/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]
            

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Mailing/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V2` WHERE fecha = '" + mifecha + "'"
            deleteQuery = "DELETE FROM `contento-bi.ucc.Correo_Apertura` WHERE campana = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = Apertura_beam.run('gs://ct-ucc/Mailing/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Email/Apertura/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]



########################## ESTRUCTURA NUEVA CORREO REBOTE ###################################



@ucc_api.route("/rebote")
def rebote():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Email/Rebote/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]
            

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Mailing/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V2` WHERE fecha = '" + mifecha + "'"
            deleteQuery = "DELETE FROM `contento-bi.ucc.Correo_Rebote` WHERE campana = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = Rebote_beam.run('gs://ct-ucc/Mailing/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Email/Rebote/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]


    

########################## ESTRUCTURA NUEVA  BASE CORREO  ###################################



@ucc_api.route("/correo")
def correo():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Email/Base/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]
            

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Mailing/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V2` WHERE fecha = '" + mifecha + "'"
            deleteQuery = "DELETE FROM `contento-bi.ucc.Base_Correo` WHERE campana = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = Base_correo_beam.run('gs://ct-ucc/Mailing/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Email/Base/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]



############################### ESTRUCTURA NUEVA MENSAJE DOBLE VIA ##################################


@ucc_api.route("/doble_via")
def doble_via():
    
    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Mensajes/Doble_via/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:]
            

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-ucc')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('sms_doble_via/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            # deleteQuery = "DELETE FROM `contento-bi.ucc.SMS_V2` WHERE fecha = '" + mifecha + "'"
            deleteQuery = "DELETE FROM `contento-bi.ucc.sms_doble_via` WHERE campana = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = Doble_via_beam.run('gs://ct-ucc/sms_doble_via/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Ucc/Mensajes/Doble_via/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo el cargue correctamente"
                response["status"] = True

          
    # # return jsonify(response), response["code"]
    # return "Corriendo : " 
    return jsonify(response), response["code"]
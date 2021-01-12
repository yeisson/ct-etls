from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
from pandas import DataFrame
import dataflow_pipeline.sensus.sensus_seguimiento_beam as sensus_seguimiento_beam
import dataflow_pipeline.sensus.sensus_metas_beam as sensus_metas_beam
import dataflow_pipeline.sensus.sensus_adh_beam as sensus_adh_beam
import dataflow_pipeline.sensus.sensus_dmobility_beam as sensus_dmobility_beam
import dataflow_pipeline.sensus.sensus_dchat_beam as sensus_dchat_beam
import dataflow_pipeline.sensus.sensus_dcorreo_beam as sensus_dcorreo_beam
import dataflow_pipeline.sensus.sensus_banco_beam as sensus_banco_beam
import dataflow_pipeline.sensus.sensus_metalal_beam as sensus_metalal_beam
import dataflow_pipeline.sensus.sensus_poc_beam as sensus_poc_beam
import dataflow_pipeline.sensus.sensus_tecnicos_beam as sensus_tecnicos_beam
import dataflow_pipeline.sensus.sensus_agricolaadmin_beam as sensus_agricolaadmin_beam
import dataflow_pipeline.sensus.sensus_agricolacast_beam as sensus_agricolacast_beam
import dataflow_pipeline.sensus.sensus_estrategy_beam as sensus_estrategy_beam
import dataflow_pipeline.sensus.sensus_alter_beam as sensus_alter_beam
import procesos.descargas as descargas
import os
import socket
import requests
from flask import request
import csv

sensus_api = Blueprint('sensus_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]
 
######################################################################################################

@sensus_api.route("/archivos_seguimiento")
def archivos_Seguimiento():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[18:26]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('info-aseguramiento/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.seguimiento` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_seguimiento_beam.run('gs://ct-sensus/info-aseguramiento/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje

#####################################################################################################


@sensus_api.route("/metas")
def metas():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Metas/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[34:]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('metas/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.metas` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_metas_beam.run('gs://ct-sensus/metas/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Metas/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
    # return "Corriendo : " + mensaje



@sensus_api.route("/adh")
def adh():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/ADH/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[9:17]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('adh/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.adh` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_adh_beam.run('gs://ct-sensus/adh/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/ADH/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
   
   

  

#####################################################################################################

@sensus_api.route("/dmobility")
def dmobility():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Detallado/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('adh/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.dmobility` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_dmobility_beam.run('gs://ct-sensus/adh/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Detallado/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]
  #####################################################################################################
 

@sensus_api.route("/dchat")
def dchat():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Chat/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('adh/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.dchat` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_dchat_beam.run('gs://ct-sensus/adh/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Chat/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]

    #####################################################################################################

@sensus_api.route("/dcorreo")
def dcorreo():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Correo/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('adh/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.dcorreo` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_dcorreo_beam.run('gs://ct-sensus/adh/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Correo/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]




     #####################################################################################################

@sensus_api.route("/banco")
def banco():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Banco/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('adh/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.banco` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_banco_beam.run('gs://ct-sensus/adh/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Banco/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]

#####################################################################################################

@sensus_api.route("/metalal")
def metalal():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Metalal/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('adh/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.banco` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_metalal_beam.run('gs://ct-sensus/adh/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Metalal/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se realizo la peticion Full HD"
                response["status"] = True

    return jsonify(response), response["code"]


#####################################################################################################

@sensus_api.route("/poc")
def poc():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Poc/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('poc/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.poc` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_poc_beam.run('gs://ct-sensus/poc/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Poc/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se cargaron los ficheros exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]    

#####################################################################################################

@sensus_api.route("/tecnicos")
def tecnicos():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/tecnicos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('tecnicos/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.tecnicos` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_tecnicos_beam.run('gs://ct-sensus/tecnicos/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/tecnicos/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se cargaron los ficheros exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]    

#####################################################################################################

@sensus_api.route("/agricolaa")
def agricolaa():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Dagricola/Administrativa/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agricolaadmin/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.agricolaadm` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_agricolaadmin_beam.run('gs://ct-sensus/agricolaadmin/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Dagricola/Administrativa/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se cargaron los ficheros exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]    


#####################################################################################################

@sensus_api.route("/agricolac")
def agricolac():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Dagricola/Castigada/"
    archivos = os.listdir(local_route)
    
    for archivo in archivos:
        


        if archivo.endswith(".csv"):
            mifecha = archivo[0:8]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agricolacast/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.agricolacast` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_agricolacast_beam.run('gs://ct-sensus/agricolacast/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Dagricola/Castigada/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se cargaron los ficheros exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]        

######################################################################################################

@sensus_api.route("/descargar", methods=['POST','GET'])

def Descarga_Encuesta():
       
    dateini= request.args.get('desde')
    dateend= request.args.get('hasta')
   
  
    myRoute = '/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Dagricola/Descargas/'+dateini+'_'+dateend+'.csv'
    myQuery ='SELECT * FROM `contento-bi.sensus.Dagrciola_admin`  where fecha between'+'"'+dateini+'"'+'AND'+'"'+dateend+'"'    
    myHeader = ["Unico","Nombre","Fecha","Nombre_gestor","Cartera","Nombre_evaluador","PEC","Telefono_cliente","Cierre_de_la_llamada","Evaluacion_del_Saludo","Evaluacion_negociacion","Aspectos_mejora","Aspectos_positivos","Observaciones"
   ]
    
    

    return descargas.descargar_csv(myRoute, myQuery, myHeader)    


######################################################################################################    

@sensus_api.route("/estrategy")
def estrategy():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Estrategia/Estrategy/"
    archivos = os.listdir(local_route)
    
    for archivo in archivos:
        


        if archivo.endswith(".csv"):
            mifecha = archivo[0:]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agricolacast/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.estrategy` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_estrategy_beam.run('gs://ct-sensus/agricolacast/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Estrategia/Estrategy/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se cargaron los ficheros exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]        

######################################################################################################

@sensus_api.route("/alter")
def alter():

    response = {}
    response["code"] = 400
    response["description"] = "No se encontraron ficheros"
    response["status"] = False

    local_route = fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Estrategia/Sinalternativas/"
    archivos = os.listdir(local_route)
    
    for archivo in archivos:
        


        if archivo.endswith(".csv"):
            mifecha = archivo[0:]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-sensus')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('agricolacast/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.sensus.alter` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = sensus_alter_beam.run('gs://ct-sensus/agricolacast/' + archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Sensus/Aseguramiento/Estrategia/Sinalternativas/Procesados/"+archivo)
                response["code"] = 200
                response["description"] = "Se cargaron los ficheros exitosamente"
                response["status"] = True

    return jsonify(response), response["code"]        

######################################################################################################

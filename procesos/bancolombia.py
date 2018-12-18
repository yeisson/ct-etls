from flask import Blueprint
from flask import jsonify
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.bancolombia.bancolombia_prejuridico_beam as bancolombia_prejuridico_beam
import dataflow_pipeline.bancolombia.bancolombia_seguimiento_beam as bancolombia_seguimiento_beam
import dataflow_pipeline.bancolombia.bancolombia_bm_beam as bancolombia_bm_beam
import os
import socket

bancolombia_api = Blueprint('bancolombia_api', __name__)

fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]

@bancolombia_api.route("/")
def inicio():
    return "Apis de Bancolombia"

@bancolombia_api.route("/hola")
def Hola():
    return "Hola Muchacho"

@bancolombia_api.route("/prejuridico")
def Prejuridico():
    # return "Hola Prejuridico"
    mensaje = bancolombia_prejuridico_beam.run('a', 'b')
    return "Corriendo : " + mensaje

@bancolombia_api.route("/seguimiento")
def Seguimiento():
    mensaje = bancolombia_seguimiento_beam.run('a', 'b')
    return "Corriendo : " + mensaje

#@bancolombia_api.route("/base_marcada")
#def Base_marcada():
#    # return "Hola Base Marcada"
#    mensaje = bancolombia_bm_beam.run()
#    return "Corriendo : " + mensaje

@bancolombia_api.route("/archivos_base_marcada")
def archivos_bm():
    # archivos = os.listdir("gs://ct-bancolombia/bm/")
    local_route = fileserver_baseroute + "/aries/Inteligencia_Negocios/EQUIPO BI/dcaro/Fuente Archivos/"
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[15:23]

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bancolombia')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('bm/' + archivo)
            blob.upload_from_filename(local_route + archivo)

            # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
            deleteQuery = "DELETE FROM `contento-bi.bancolombia_admin.bm` WHERE fecha = '" + mifecha + "'"

            #Primero eliminamos todos los registros que contengan esa fecha
            client = bigquery.Client()
            query_job = client.query(deleteQuery)

            #result = query_job.result()
            query_job.result() # Corremos el job de eliminacion de datos de BigQuery

            # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
            mensaje = bancolombia_bm_beam.run('gs://ct-bancolombia/bm/' + archivo, mifecha)
            
            if mensaje == "Corrio Full HD":
                move(local_route + archivo, fileserver_baseroute + "/aries/Inteligencia_Negocios/EQUIPO BI/dcaro/Procesados/"+archivo)
    return "El cargue de archivos: " + mensaje

@bancolombia_api.route("/archivos_prejuridico")
def archivos_Prejuridico():

    archivos = os.listdir(fileserver_baseroute + "/aries/Inteligencia_Negocios/EQUIPO BI/dcaro/Prejuridicos Fuente Archivos")
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[21:29]
            mensaje = bancolombia_prejuridico_beam.run(archivo, mifecha)
            if mensaje == "Corrio Full HD":
                move(fileserver_baseroute + "/aries/Inteligencia_Negocios/EQUIPO BI/dcaro/Prejuridicos Fuente Archivos/"+archivo, fileserver_baseroute + "/aries/Inteligencia_Negocios/EQUIPO BI/dcaro/Prejuridicos Procesados/"+archivo)

@bancolombia_api.route("/archivos_seguimiento")
def archivos_Seguimiento():

    archivos = os.listdir(fileserver_baseroute + "/aries/Inteligencia_Negocios/EQUIPO BI/nflorez/fuentes_seg")
    for archivo in archivos:
        if archivo.endswith(".csv"):
            mifecha = archivo[20:28]
            mensaje = bancolombia_seguimiento_beam.run(archivo, mifecha)
            if mensaje == "Corrio sin problema":
                move(fileserver_baseroute + "/aries/Inteligencia_Negocios/EQUIPO BI/nflorez/fuentes_seg/"+archivo, fileserver_baseroute + "/aries/Inteligencia_Negocios/EQUIPO BI/nflorez/procesados_seg/"+archivo)
    return "Corriendo : " + mensaje
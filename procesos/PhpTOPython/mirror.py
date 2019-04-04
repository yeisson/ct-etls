from flask import Blueprint
from flask import jsonify
from flask import request
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.bridge.bridge_beam as bridge_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime

# coding=utf-8

mirror_api = Blueprint('mirror_api', __name__)
fileserver_baseroute = ("//192.168.20.87", "/media")[socket.gethostname()=="contentobi"]



#####################################################################################################################################
#####################################################################################################################################
######################################################## DELETE #####################################################################
#####################################################################################################################################
#####################################################################################################################################



@mirror_api.route("/delete", methods=['GET'])
def delete():

#Parametros GET para modificar la consulta segun los parametros entregados
    id_cliente = request.args.get('id_cliente')
    producto = request.args.get('producto')
    sub_producto = request.args.get('sub_producto')
    

    deleteQuery = "DELETE FROM `contento-bi.Contento.Jerarquias_Metas` WHERE id_cliente = '" + id_cliente + "' AND producto = '" + producto + "' AND sub_producto = '"  + sub_producto + "'"
    client = bigquery.Client()
    query_job = client.query(deleteQuery)
    query_job.result()

    print("Proceso de eliminacion Completado")
    return "La siguiente informacion proviente de python: " + id_cliente + "," + producto + "," + sub_producto





####################################################################################################################################
####################################################################################################################################
####################################################### LOADs #######################################################################
####################################################################################################################################
####################################################################################################################################



@mirror_api.route("/load", methods=['GET'])
def load():

#Parametros GET para modificar la consulta segun los parametros entregados
    # ruta = request.args.get('mi_archivo')
    
    cwd = os.getcwd()
    dir_path = os.path.dirname(os.path.realpath(__file__))
    return dir_path

    # "/var/gunicorn/contento-etls/procesos/PhpTOPython"


    # response = {}
    # response["code"] = 400
    # response["description"] = "No se encontraron ficheros"
    # response["status"] = False

    # local_route = fileserver_baseroute + "/" + ruta
    # archivos = os.listdir(local_route)
    # for archivo in archivos:
    #     if archivo.endswith(".csv"):
    #         mifecha = archivo[20:28]

    #         storage_client = storage.Client()
    #         bucket = storage_client.get_bucket('ct-bancolombia')

    #         # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
    #         blob = bucket.blob('info-seguimiento/' + archivo)
    #         blob.upload_from_filename(local_route + archivo)

    #         # Una vez subido el fichero a Cloud Storage procedemos a eliminar los registros de BigQuery
    #         deleteQuery = "DELETE FROM `contento-bi.bancolombia_admin.seguimiento` WHERE fecha = '" + mifecha + "'"

    #         #Primero eliminamos todos los registros que contengan esa fecha
    #         client = bigquery.Client()
    #         query_job = client.query(deleteQuery)

    #         #result = query_job.result()
    #         query_job.result() # Corremos el job de eliminacion de datos de BigQuery

    #         # Terminada la eliminacion de BigQuery y la subida a Cloud Storage corremos el Job
    #         mensaje = bancolombia_seguimiento_beam.run('gs://ct-bancolombia/info-seguimiento/' + archivo, mifecha)
    #         if mensaje == "Corrio Full HD":
    #             move(local_route + archivo, fileserver_baseroute + "/BI_Archivos/GOOGLE/Bancolombia_Admin/Seguimiento/Procesados/"+archivo)
    #             response["code"] = 200
    #             response["description"] = "Se realizo la peticion Full HD"
    #             response["status"] = True

    # return jsonify(response), response["code"]
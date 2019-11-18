from flask import Blueprint
from flask import jsonify
from flask import request
from shutil import copyfile, move
from google.cloud import storage
from google.cloud import bigquery
import dataflow_pipeline.phptopython.phptopython_beam as phptopython_beam
import cloud_storage_controller.cloud_storage_controller as gcscontroller
import os
import time
import socket
import _mssql
import datetime
import glob

# coding=utf-8

mirror_api = Blueprint('mirror_api', __name__)
fecha = time.strftime('%Y-%m-%d')

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
####################################################### JEARQUIAS METAS #######################################################################
####################################################################################################################################
####################################################################################################################################



@mirror_api.route("/load", methods=['GET'])
def load():

#Parametros GET para modificar la consulta segun los parametros entregados
    url = request.args.get('mi_archivo') # Recibe con esto / 
    response = {}
    
    local_route = url
    archivos = os.listdir(local_route)
    for archivo in archivos:
        if archivo.endswith(".csv"):

            storage_client = storage.Client()
            bucket = storage_client.get_bucket('ct-bridge')

            # Subir fichero a Cloud Storage antes de enviarlo a procesar a Dataflow
            blob = bucket.blob('Uploads_php/' + archivo)
            blob.upload_from_filename(local_route + archivo)


            mensaje = phptopython_beam.run('gs://ct-bridge/Uploads_php/' + archivo)
            if mensaje == "El proceso de cargue a bigquery fue ejecutado con exito":
                
                response["code"] = 200
                response["description"] = "El proceso de cargue a BIGQUERY por medio del MIRROR fue ejecutado correctamente"
                response["status"] = True
            
            os.remove(local_route + archivo)

    return jsonify(response), response["code"]
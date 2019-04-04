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
import glob

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
    url = request.args.get('mi_archivo') # Recibe con esto / 
  
    r = os.listdir(url)

    return str(r)
    
